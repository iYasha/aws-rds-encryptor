import abc
from collections.abc import Generator

import psycopg2

from rds_encryptor.rds.instance import RDSInstance


class InvalidCredentialsException(Exception):
    pass


class InvalidPostgresCredentialsException(InvalidCredentialsException):
    pass


class DBManager(abc.ABC):
    invalid_credentials_exception: InvalidCredentialsException

    @staticmethod
    def from_rds(rds_instance: RDSInstance, database: str = "postgres") -> "PostgresDBManager":
        return PostgresDBManager(
            host=rds_instance.endpoint,
            port=rds_instance.port,
            user=rds_instance.master_username,
            password=rds_instance.master_password,
            database=database,
        )

    @abc.abstractmethod
    def check_connection(self) -> bool:
        pass


class PostgresDBManager:
    invalid_credentials_exception = InvalidPostgresCredentialsException

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def __get_connection(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def check_connection(self) -> bool:
        try:
            conn = self.__get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
        except psycopg2.DatabaseError:
            return False
        return True

    def get_parameter(self, parameter: str) -> str:
        conn = self.__get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SHOW {parameter}")
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return result

    def create_extension(self, extension: str):
        conn = self.__get_connection()
        cursor = conn.cursor()
        cursor.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        conn.commit()
        cursor.close()
        conn.close()

    def get_partitioned_tables(self) -> list[dict[str, str]]:
        query = """
        select relnamespace::regnamespace::text schema_name, oid::regclass::text table_name from pg_class
        where relkind = 'p' and oid in (select distinct inhparent from pg_inherits)
        order by schema_name, table_name;
        """
        conn = self.__get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        tables = [
            {
                "schema": row[0],
                "table": row[1].replace(f"{row[0]}.", "") if row[1].startswith(f"{row[0]}.") else row[1],
            }
            for row in cursor.fetchall()
        ]
        cursor.close()
        conn.close()
        return tables

    def get_all_tables(self) -> list[str]:
        conn = self.__get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT schemaname, tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname NOT LIKE 'pg_%'
              AND schemaname != 'information_schema'
              and tablename not like 'awsdms_ddl_audit%'
            ORDER BY schemaname, tablename;
            """
        )
        tables = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return tables

    def truncate_database(self):
        conn = self.__get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE (schema_name NOT LIKE 'pg_%'
              AND schema_name != 'information_schema') or schema_name = 'pglogical';
        """
        )
        schemas = [row[0] for row in cursor.fetchall()]
        for schema in schemas:
            # FIXME: S608 Possible SQL injection vector through string-based query construction
            cursor.execute(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}'")  # noqa: S608
            tables = [row[0] for row in cursor.fetchall()]
            for table in tables:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table} CASCADE")
        conn.commit()
        cursor.close()
        conn.close()

    def get_sequences(self) -> list[dict[str, int | str]]:
        conn = self.__get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT schemaname, sequencename, last_value FROM pg_sequences;")
        sequences = [
            {
                "schema": row[0],
                "sequence": row[1],
                "last_value": row[2] or 1,
            }
            for row in cursor.fetchall()
        ]
        cursor.close()
        conn.close()
        return sequences

    def set_sequences(self, sequences: list[dict[str, int | str]]):
        conn = self.__get_connection()
        cursor = conn.cursor()
        for sequence in sequences:
            if sequence["sequence"].startswith("awsdms_ddl_audit"):
                continue
            cursor.execute(f"SELECT setval('{sequence['schema']}.{sequence['sequence']}', {sequence['last_value']})")
        conn.commit()
        cursor.close()
        conn.close()

    def iter_count(self, tables: list[str]) -> Generator[int, None, None]:
        conn = self.__get_connection()
        cursor = conn.cursor()
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
            yield cursor.fetchone()[0]
        cursor.close()
        conn.close()
