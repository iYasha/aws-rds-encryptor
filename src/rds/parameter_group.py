from typing import Optional

from src.utils import MIGRATION_SEED
import boto3


def build_shared_preload_libraries_param(*libraries: str) -> str:
    return ','.join(map(str.strip, libraries))


def get_migration_parameter_group_name(parameter_group_name: str) -> str:
    return f'{parameter_group_name}-{MIGRATION_SEED}-migration'


class ParameterGroup:
    aws_client = boto3.client('rds')

    def __init__(self, name: str):
        self.name = name
        self.properties = self._fetch_properties()

    @classmethod
    def from_name(cls, name: str) -> Optional['ParameterGroup']:
        assert name, 'Parameter group name is required'

        response = cls.aws_client.describe_db_parameter_groups(
            DBParameterGroupName=name,
        )['DBParameterGroups']
        if len(response) == 0:
            return None
        if len(response) > 1:
            raise ValueError(f'Multiple parameter groups found: {name}')

        return cls(name=name)

    def copy(self) -> 'ParameterGroup':
        response = self.aws_client.copy_db_parameter_group(
            SourceDBParameterGroupIdentifier=self.name,
            TargetDBParameterGroupIdentifier=get_migration_parameter_group_name(self.name),
            TargetDBParameterGroupDescription=f'{self.name} migration parameter group',
        )
        return ParameterGroup(name=response['DBParameterGroup']['DBParameterGroupName'])

    def _fetch_properties(self):
        response = self.aws_client.describe_db_parameters(
            DBParameterGroupName=self.name,
        )['Parameters']
        return {param['ParameterName']: param['ParameterValue'] for param in response}

    @property
    def wal_sender_timeout(self) -> int:
        return int(self.properties.get('wal_sender_timeout', 0))

    @property
    def shared_preload_libraries(self) -> list[str]:
        return list(map(str.strip, self.properties.get('shared_preload_libraries', '').split(',')))

    @property
    def rds_logical_replication(self) -> int:
        return int(self.properties.get('rds.logical_replication', 0))

    def set_parameter(self, name: str, value: any) -> None:
        self.aws_client.modify_db_parameter_group(
            DBParameterGroupName=self.name,
            Parameters=[{'ParameterName': name, 'ParameterValue': str(value)}],
            ApplyMethod='immediate',
        )
        self.properties = self._fetch_properties()



