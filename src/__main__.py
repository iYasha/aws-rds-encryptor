"""
1. Create source/target endpoint
2. Create migration task
3. Truncate all tables in specific databases
4. Run the migration task
5. Migrate sequences
6. Run validation script that checks count across all tables and databases
"""

from src.db_manager import DBManager
from src.dms.endpoints import SourceEndpoint, TargetEndpoint
from src.dms.enums import MigrationType
from src.dms.replication_instance import ReplicationInstance
from src.dms.migration_task import TableMapping, MigrationTask
from src.dms.task_manager import MigrationTaskManager
from src.rds.instance import RDSInstance
from src.rds.parameter_group import (
    build_shared_preload_libraries_param, ParameterGroup,
    get_migration_parameter_group_name,
)
from src.utils import MIGRATION_SEED

PRIMARY_INSTANCE_IDENTIFIER = '...'
PRIMARY_INSTANCE_PASSWORD = '...'
ENCRYPTED_INSTANCE_IDENTIFIER = '...'
ENCRYPTION_KMS_KEY_ARN = '...'
REPLICATION_INSTANCE_ARN = '...'
DATABASE_TO_MIGRATE: list[str] = ['...']

rds_instance = RDSInstance.from_id(instance_id=PRIMARY_INSTANCE_IDENTIFIER, root_password=PRIMARY_INSTANCE_PASSWORD)
if rds_instance is None:
    raise ValueError(f'Cannot find source RDS instance by identifier={PRIMARY_INSTANCE_IDENTIFIER}')

source_db_manager = DBManager.from_rds(rds_instance=rds_instance)
if not source_db_manager.check_connection():
    raise source_db_manager.invalid_credentials_exception('Cannot connect to source RDS to postgres database.')

for database in DATABASE_TO_MIGRATE:
    db_manager = DBManager.from_rds(rds_instance=rds_instance, database=database)
    if not db_manager.check_connection():
        raise db_manager.invalid_credentials_exception(f'Cannot connect to source RDS to {database} database.')

encrypted_rds_instance: RDSInstance | None = RDSInstance.from_id(
    instance_id=ENCRYPTED_INSTANCE_IDENTIFIER, root_password=PRIMARY_INSTANCE_PASSWORD
)
if encrypted_rds_instance is None:
    snapshot = rds_instance.take_snapshot().wait_until_created()
    encrypted_snapshot = snapshot.copy_snapshot(
        copy_tags=True,
        encryption_kms_key_arn=ENCRYPTION_KMS_KEY_ARN,
    ).wait_until_created()
    encrypted_rds_instance = encrypted_snapshot.restore_snapshot(
        instance_identifier=ENCRYPTED_INSTANCE_IDENTIFIER,
        tags=rds_instance.tags,
    ).wait_until_available()

migration_parameter_group: ParameterGroup | None = ParameterGroup.from_name(
    name=get_migration_parameter_group_name(rds_instance.parameter_group.name)
)

if migration_parameter_group is None:
    migration_parameter_group: ParameterGroup = rds_instance.parameter_group.copy()
if migration_parameter_group.wal_sender_timeout != 0:
    migration_parameter_group.set_parameter('wal_sender_timeout', 0)
if 'pglogical' not in migration_parameter_group.shared_preload_libraries:
    migration_parameter_group.set_parameter(
        'shared_preload_libraries',
        build_shared_preload_libraries_param('pglogical', *migration_parameter_group.shared_preload_libraries)
    )
if migration_parameter_group.rds_logical_replication != 1:
    migration_parameter_group.set_parameter('rds.logical_replication', 1)

if rds_instance.parameter_group.name != migration_parameter_group.name:
    # TODO: Need to set previous parameter group after migration
    rds_instance.set_parameter_group(migration_parameter_group).wait_until_available()
if encrypted_rds_instance.parameter_group.name != migration_parameter_group.name:
    encrypted_rds_instance.set_parameter_group(migration_parameter_group).wait_until_available()

while 'pglogical' not in source_db_manager.get_parameter(
    'shared_preload_libraries'
):  # TODO: potential bug if any substring contains 'pglogical'
    print('`pglogical` not found in `shared_preload_libraries`, restart source database to apply it.')
    input()

for database in DATABASE_TO_MIGRATE:
    source_db_manager = DBManager.from_rds(rds_instance=rds_instance, database=database)
    source_db_manager.create_extension('pglogical')

task_manager = MigrationTaskManager()

for database in DATABASE_TO_MIGRATE:
    source_endpoint = (
        SourceEndpoint(rds_instance, database=database)
        .get_or_create_endpoint()
        .wait_until_created()
    )
    target_endpoint = (
        TargetEndpoint(encrypted_rds_instance, database=database)
        .get_or_create_endpoint()
        .wait_until_created()
    )
    migration_task = MigrationTask.create_migration_task(
        name=f'{rds_instance.instance_id}-{database}-{MIGRATION_SEED}',
        source_endpoint=source_endpoint,
        target_endpoint=target_endpoint,
        replication_instance=ReplicationInstance.from_arn(arn=REPLICATION_INSTANCE_ARN),
        migration_type=MigrationType.migrate_replicate,
        table_mappings=[TableMapping(schema='%', table='%', action='include')],  # TODO: Need to use inputs from user
        tags=rds_instance.tags,
    )
    DBManager.from_rds(rds_instance=encrypted_rds_instance, database=database).truncate_database()
    task_manager.add_task(migration_task)

if task_manager.run_all():
    for database in DATABASE_TO_MIGRATE:
        source_db_manager = DBManager.from_rds(rds_instance=rds_instance, database=database)
        target_db_manager = DBManager.from_rds(rds_instance=encrypted_rds_instance, database=database)
        sequences = source_db_manager.get_sequences()
        target_db_manager.set_sequences(sequences)
        if source_db_manager.check_data_equality(target_db_manager):
            print(f'Data in {database} is equal.')
        else:
            raise NotEqualDataException(f'Data in {database} is not equal.')

else:
    print('One or more tasks finished with errors.')
