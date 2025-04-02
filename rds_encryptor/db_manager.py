import abc

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
        cursor.close()
        conn.close()

    def truncate_database(self):
        conn = self.__get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT LIKE 'pg_%'
              AND schema_name != 'information_schema'
        """
        )
        schemas = [row[0] for row in cursor.fetchall()]
        for schema in schemas:
            # FIXME: S608 Possible SQL injection vector through string-based query construction
            cursor.execute(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}'")  # noqa: S608
            tables = [row[0] for row in cursor.fetchall()]
            for table in tables:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table} CASCADE")
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
            cursor.execute(f"SELECT setval('{sequence['schema']}.{sequence['sequence']}', {sequence['last_value']})")
        cursor.close()
        conn.close()
