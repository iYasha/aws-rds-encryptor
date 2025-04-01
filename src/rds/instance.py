from typing import Optional

import boto3

from src.rds.parameter_group import ParameterGroup
import time
from datetime import datetime, UTC, timedelta

from src.rds.snapshot import RDSSnapshot
from src.utils import MIGRATION_SEED, get_logger


class RDSInstance:
    logger = get_logger('RDSInstance')
    aws_client = boto3.client('rds')

    def __init__(
        self,
        instance_id: str,
        endpoint: str,
        port: int,
        master_username: str,
        master_password: str,
        parameter_group: ParameterGroup,
        tags: list[dict[str, str]] = None,
    ):
        self.instance_id = instance_id
        self.endpoint = endpoint
        self.port = port
        self.master_username = master_username
        self.master_password = master_password
        self.parameter_group = parameter_group
        if tags is None:
            tags = []
        self.tags = tags

    @classmethod
    def from_id(cls, instance_id: str, root_password: str) -> Optional['RDSInstance']:
        assert instance_id, 'Instance ID is required'
        assert root_password, 'Root password is required'

        instances = cls.aws_client.describe_db_instances(
            DBInstanceIdentifier=instance_id,
        )['DBInstances']
        if len(instances) == 0:
            return None
        elif len(instances) > 1:
            raise ValueError(f'Multiple instances found: {instance_id}')

        instance = instances[0]

        return cls(
            instance_id=instance_id,
            endpoint=instance['Endpoint']['Address'],
            port=instance['Endpoint']['Port'],
            master_username=instance['MasterUsername'],
            master_password=root_password,
            parameter_group=ParameterGroup.from_name(instance['DBParameterGroups'][0]['DBParameterGroupName']),
            tags=instance.get('TagList'),
        )

    def get_status(self) -> str:
        instance = self.aws_client.describe_db_instances(
            DBInstanceIdentifier=self.instance_id,
        )['DBInstances'][0]
        return instance['DBInstanceStatus']

    def take_snapshot(self) -> RDSSnapshot:
        snapshot_id = f'{self.instance_id}-{MIGRATION_SEED}-migration'
        snapshot = RDSSnapshot.from_id(snapshot_id)
        if snapshot is not None:
            return snapshot

        response = self.aws_client.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_id,
            DBInstanceIdentifier=self.instance_id,
            Tags=self.tags,
        )['DBSnapshot']
        return RDSSnapshot.from_id(snapshot_id=response['DBSnapshotIdentifier'])

    def set_parameter_group(self, parameter_group: ParameterGroup) -> 'RDSInstance':
        self.aws_client.modify_db_instance(
            DBInstanceIdentifier=self.instance_id,
            DBParameterGroupName=parameter_group.name,
            ApplyImmediately=True,
        )
        self.parameter_group = parameter_group
        return self

    def wait_until_available(self, timeout: int = 60 * 60, pooling_frequency: int = 30) -> 'RDSInstance':
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)

        while datetime.now(tz=UTC) < timeout_dt:
            status = self.get_status()
            self.logger.info(f'[wait_until_available] Instance: {self.instance_id} status: {status}')
            if status == 'available':
                return self
            time.sleep(pooling_frequency)

        raise TimeoutError(f'Instance {self.instance_id} is not available after {timeout} seconds')



if __name__ == '__main__':
    import os
    rds_instance = RDSInstance.from_id(
        instance_id=os.getenv('PRIMARY_INSTANCE_IDENTIFIER'),
        root_password=os.getenv('PRIMARY_INSTANCE_PASSWORD'),
    )
