from rds_encryptor.db_manager import DBManager
from rds_encryptor.dms.endpoints import SourceEndpoint, TargetEndpoint
from rds_encryptor.dms.enums import MigrationType
from rds_encryptor.dms.migration_task import MigrationTask, TableMapping
from rds_encryptor.dms.replication_instance import ReplicationInstance
from rds_encryptor.dms.task_manager import MigrationTaskManager
from rds_encryptor.rds.instance import RDSInstance
from rds_encryptor.rds.parameter_group import (
    ParameterGroup,
    build_shared_preload_libraries_param,
    get_migration_parameter_group_name,
    get_original_parameter_group,
)
from rds_encryptor.utils import MIGRATION_SEED, get_logger, normalize_aws_id


class EncryptionPipeline:
    logger = get_logger("EncryptionPipeline")

    def __init__(
        self,
        instance_id: str,
        master_password: str,
        kms_key_arn: str,
        dms_replication_instance_arn: str,
        databases: list[str] = None,  # noqa: RUF013
        new_instance_identifier: str | None = None,
    ):
        self.rds_instance = RDSInstance.from_id(instance_id=instance_id, root_password=master_password)
        if self.rds_instance is None:
            raise ValueError(f"Cannot find source RDS instance by identifier={instance_id}")

        self.kms_key_arn = kms_key_arn
        self.dms_replication_instance_arn = dms_replication_instance_arn
        self.databases = databases or []
        self.new_instance_identifier = new_instance_identifier or normalize_aws_id(
            f"{instance_id}-{MIGRATION_SEED}-encrypted"
        )

    def check_databases_connections(self):
        self.logger.info("Checking database connections...")
        for database in self.databases:
            db_manager = DBManager.from_rds(rds_instance=self.rds_instance, database=database)
            if not db_manager.check_connection():
                raise db_manager.invalid_credentials_exception(f"Cannot connect to source RDS to {database} database.")
            self.logger.info('Successfully connected to "%s" database.', database)

    def create_encrypted_instance(self):
        self.logger.info('Trying to provision encrypted RDS instance with ID: "%s" ...', self.new_instance_identifier)
        encrypted_rds_instance: RDSInstance | None = RDSInstance.from_id(
            instance_id=self.new_instance_identifier,
            root_password=self.rds_instance.master_password,
        )
        if encrypted_rds_instance is None:
            snapshot = self.rds_instance.take_snapshot().wait_until_created()
            encrypted_snapshot = snapshot.copy_snapshot(
                copy_tags=True,
                encryption_kms_key_arn=self.kms_key_arn,
            ).wait_until_created()
            encrypted_rds_instance = encrypted_snapshot.restore_snapshot(
                instance_identifier=self.new_instance_identifier,
                from_rds_instance=self.rds_instance,
                master_password=self.rds_instance.master_password,
                tags=self.rds_instance.tags,
            ).wait_until_available()

            rds_instance_params = self.rds_instance._describe()
            encrypted_rds_instance.modify_instance(
                DBSecurityGroups=rds_instance_params["DBSecurityGroups"],
                DatabaseInsightsMode=rds_instance_params["DatabaseInsightsMode"],
                EnablePerformanceInsights=rds_instance_params["PerformanceInsightsEnabled"],
                PerformanceInsightsKMSKeyId=self.kms_key_arn,
                MaxAllocatedStorage=rds_instance_params["MaxAllocatedStorage"],
            ).wait_until_available()
        else:
            self.logger.info(
                'Skip provisioning "%s" instance, because it\'s already provisioned', self.new_instance_identifier
            )
        return encrypted_rds_instance

    def create_parameter_group_for_dms(self) -> ParameterGroup:
        parameter_group_name = get_migration_parameter_group_name(self.rds_instance.parameter_group.name)
        migration_parameter_group: ParameterGroup | None = ParameterGroup.from_name(name=parameter_group_name)
        self.logger.info('Creating parameter group "%s" for DMS migration...', parameter_group_name)

        if migration_parameter_group is None:
            migration_parameter_group: ParameterGroup = self.rds_instance.parameter_group.copy()
        if migration_parameter_group.wal_sender_timeout != 0:
            self.logger.info(
                "%s.wal_sender_timeout=%s, setting to 0",
                parameter_group_name,
                migration_parameter_group.wal_sender_timeout,
            )
            migration_parameter_group.set_parameter("wal_sender_timeout", 0)
        if "pglogical" not in migration_parameter_group.shared_preload_libraries:
            self.logger.info(
                "pglogical not in %s.shared_preload_libraries=%s, adding pglogical",
                parameter_group_name,
                migration_parameter_group.shared_preload_libraries,
            )
            migration_parameter_group.set_parameter(
                "shared_preload_libraries",
                build_shared_preload_libraries_param("pglogical", *migration_parameter_group.shared_preload_libraries),
            )
        if migration_parameter_group.rds_logical_replication != 1:
            self.logger.info(
                "%s.rds_logical_replication=%s, setting to 1",
                parameter_group_name,
                migration_parameter_group.rds_logical_replication,
            )
            migration_parameter_group.set_parameter("rds.logical_replication", 1)

        return migration_parameter_group

    def create_pglogical_extension_in_source_db(self):
        db_manager = DBManager.from_rds(rds_instance=self.rds_instance)
        while "pglogical" not in db_manager.get_parameter(
            "shared_preload_libraries"
        ):  # TODO: potential bug if any substring contains 'pglogical'
            self.logger.error(
                "`pglogical` not found in `shared_preload_libraries`, "
                'restart "%s" database to apply it and hit <Enter>.',
                self.rds_instance.instance_id,
            )
            input()

        for database in self.databases:
            source_db_manager = DBManager.from_rds(rds_instance=self.rds_instance, database=database)
            self.logger.info('Creating "pglogical" extension for %s database', database)
            source_db_manager.create_extension("pglogical")

    def create_replication_tasks(self, encrypted_rds_instance: RDSInstance) -> MigrationTaskManager:
        task_manager = MigrationTaskManager()
        dms_replication_instance = ReplicationInstance.from_arn(arn=self.dms_replication_instance_arn)

        for database in self.databases:
            encrypted_instance_db_manager = DBManager.from_rds(rds_instance=encrypted_rds_instance, database=database)

            # Because of the wildcards DMS trying to migrate partitioned tables and partitions as regular tables,
            # we get unique constraint violation, to prevent it we have to exclude partitioned tables
            partitioned_tables = encrypted_instance_db_manager.get_partitioned_tables()
            exclude_partitioned_tables = [
                TableMapping(schema=table["schema"], table=table["table"], action="exclude")
                for table in partitioned_tables
            ]

            source_endpoint = (
                SourceEndpoint(self.rds_instance, database=database, kms_key_arn=self.kms_key_arn)
                .get_or_create_endpoint()
                .wait_until_created()
            )
            target_endpoint = (
                TargetEndpoint(
                    encrypted_rds_instance,
                    database=database,
                    kms_key_arn=self.kms_key_arn,
                )
                .get_or_create_endpoint()
                .wait_until_created()
            )
            migration_task = MigrationTask.create_migration_task(
                name=normalize_aws_id(f"{self.rds_instance.instance_id}-{database}-{MIGRATION_SEED}"),
                source_endpoint=source_endpoint,
                target_endpoint=target_endpoint,
                replication_instance=dms_replication_instance,
                migration_type=MigrationType.migrate_replicate,
                table_mappings=[
                    TableMapping(schema="%", table="%", action="include"),
                    TableMapping(schema="pg_%", table="%", action="exclude"),
                    TableMapping(schema="information_schema", table="%", action="exclude"),
                    TableMapping(schema="pglogical", table="%", action="exclude"),
                    *exclude_partitioned_tables,
                ],
                tags=self.rds_instance.tags,
            )

            self.logger.info(
                'Truncating tables in "%s" database for instance "%s" ...', database, encrypted_rds_instance.instance_id
            )
            encrypted_instance_db_manager.truncate_database()
            self.logger.info(
                'Tables truncated in "%s" database for instance "%s"', database, encrypted_rds_instance.instance_id
            )
            task_manager.add_task(migration_task)

        return task_manager

    def migrate_databases_sequences(self, encrypted_rds_instance: RDSInstance):
        for database in self.databases:
            self.logger.info(
                'Start migrating "%s" database sequences from "%s" to "%s" instance...',
                database,
                self.rds_instance.instance_id,
                encrypted_rds_instance.instance_id,
            )
            source_db_manager = DBManager.from_rds(rds_instance=self.rds_instance, database=database)
            target_db_manager = DBManager.from_rds(rds_instance=encrypted_rds_instance, database=database)
            sequences = source_db_manager.get_sequences()
            target_db_manager.set_sequences(sequences)
            self.logger.info(
                'Sequences migrated for "%s" database from "%s" to "%s" instance',
                database,
                self.rds_instance.instance_id,
                encrypted_rds_instance.instance_id,
            )

    def check_data_consistency(self, encrypted_rds_instance: RDSInstance):
        for database in self.databases:
            source_db_manager = DBManager.from_rds(rds_instance=self.rds_instance, database=database)
            target_db_manager = DBManager.from_rds(rds_instance=encrypted_rds_instance, database=database)
            self.logger.info(
                'Checking data consistency for "%s" database between "%s" and "%s" instances...',
                database,
                self.rds_instance.instance_id,
                encrypted_rds_instance.instance_id,
            )
            tables = source_db_manager.get_all_tables()

            iterator = zip(
                tables, source_db_manager.iter_count(tables), target_db_manager.iter_count(tables), strict=True
            )

            diff_count = {}

            for table, source_count, target_count in iterator:
                if source_count != target_count:
                    diff_count[table] = (source_count, target_count)
                    self.logger.error(
                        'Data inconsistency for table "%s" between "%s" and "%s" instances: '
                        "source count=%s, target count=%s",
                        table,
                        self.rds_instance.instance_id,
                        encrypted_rds_instance.instance_id,
                        source_count,
                        target_count,
                    )

            if not diff_count:
                self.logger.info(
                    'Data consistency check for "%s" database between "%s" and "%s" instances passed',
                    database,
                    self.rds_instance.instance_id,
                    encrypted_rds_instance.instance_id,
                )
            else:
                self.logger.error(
                    'Data consistency check for "%s" database between "%s" and "%s" instances failed',
                    database,
                    self.rds_instance.instance_id,
                    encrypted_rds_instance.instance_id,
                )

    def rollback_parameter_group(self, encrypted_rds_instance: RDSInstance):
        # TODO: DEPRECATED
        original_parameter_group_name = get_original_parameter_group(self.rds_instance.parameter_group.name)
        parameter_group: ParameterGroup | None = ParameterGroup.from_name(name=original_parameter_group_name)
        if parameter_group is None:
            self.logger.warning('Cannot find original parameter group "%s" for rollback', original_parameter_group_name)
            return
        migration_parameter_group = self.rds_instance.parameter_group
        self.rds_instance.set_parameter_group(parameter_group).wait_until_available()
        encrypted_rds_instance.set_parameter_group(parameter_group).wait_until_available()
        input(f'Please reboot "{self.rds_instance.instance_id}" database and hit <Enter>')
        self.rds_instance.wait_until_available()
        input(f'Please reboot "{encrypted_rds_instance.instance_id}" database and hit <Enter>')
        encrypted_rds_instance.wait_until_available()
        migration_parameter_group.delete()
        self.logger.info('Rollback to "%s" parameter group finished', original_parameter_group_name)

    def run_pipeline(self):
        self.check_databases_connections()
        encrypted_rds_instance = self.create_encrypted_instance()
        migration_parameter_group = self.create_parameter_group_for_dms()

        # TODO: Need to set previous parameter group after migration
        if self.rds_instance.parameter_group.name != migration_parameter_group.name:
            self.rds_instance.wait_until_available().set_parameter_group(
                migration_parameter_group
            ).wait_until_available()
            input(f'Please reboot "{self.rds_instance.instance_id}" database and hit <Enter>')
            self.rds_instance.wait_until_available()
        if encrypted_rds_instance.parameter_group.name != migration_parameter_group.name:
            encrypted_rds_instance.wait_until_available().set_parameter_group(
                migration_parameter_group
            ).wait_until_available()
            input(f'Please reboot "{encrypted_rds_instance.instance_id}" database and hit <Enter>')
            encrypted_rds_instance.wait_until_available()
        self.create_pglogical_extension_in_source_db()

        task_manager = self.create_replication_tasks(encrypted_rds_instance)

        if task_manager.run_all():
            self.logger.info("All tasks finished successfully.")
            self.migrate_databases_sequences(encrypted_rds_instance)
            self.check_data_consistency(encrypted_rds_instance)
        else:
            self.logger.warning("One or more tasks finished with errors.")
