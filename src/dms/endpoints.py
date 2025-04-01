import abc
import time
from datetime import datetime, UTC, timedelta
from typing import Literal, Optional

import boto3

from src import config
from src.rds.instance import RDSInstance
from src.utils import get_logger, MIGRATION_SEED, normalize_aws_id


class BaseEndpoint(abc.ABC):

    logger = get_logger('BaseEndpoint')
    aws_client = boto3.client('dms')
    endpoint_type: Literal['source', 'target']
    additional_settings: dict[str, str] = None

    def __init__(self, rds_instance: RDSInstance, database: str):
        self.rds_instance = rds_instance
        self.database = database
        self.endpoint_id = normalize_aws_id(
            f'{self.endpoint_type}-{self.rds_instance.instance_id}-{self.database}-{MIGRATION_SEED}'
        )
        self._arn = None

    @property
    def arn(self) -> str:
        if self._arn is None:
            raise ValueError('You must call .get_or_create_endpoint() first to get the ARN')
        return self._arn

    def create_endpoint(self) -> 'BaseEndpoint':
        assert self.endpoint_type in ('source', 'target'), f'Invalid endpoint type: {self.endpoint_type}'
        response = self.aws_client.create_endpoint(
            EndpointIdentifier=self.endpoint_id,
            EndpointType=self.endpoint_type,
            EngineName='postgres',
            KmsKeyId=config.RDS_DEFAULT_KMS_KEY_ARN,
            PostgreSQLSettings={
                "DatabaseName": self.database,
                "Port": self.rds_instance.port,
                "ServerName": self.rds_instance.endpoint,
                "Username": self.rds_instance.master_username,
                "Password": self.rds_instance.master_password,
                **(self.additional_settings or {}),
            },
            Tags=self.rds_instance.tags
        )
        self._arn = response['Endpoint']['EndpointArn']
        return self

    def _describe(self):
        response = self.aws_client.describe_endpoints(
            Filters=[
                {
                    'Name': 'endpoint-id',
                    'Values': [self.endpoint_id]
                }
            ]
        )
        if len(response['Endpoints']) == 0:
            return None
        elif len(response['Endpoints']) > 1:
            raise ValueError(f'Multiple endpoints found: {self.endpoint_id}')

        return response['Endpoints'][0]

    def get_status(self) -> str :
        return self._describe()['Status']

    def get_endpoint(self) -> Optional['BaseEndpoint']:
        self._arn = self._describe()['EndpointArn']
        return self

    def get_or_create_endpoint(self) -> 'BaseEndpoint':
        endpoint = self.get_endpoint()
        if endpoint is not None:
            return endpoint
        return self.create_endpoint()

    def wait_until_created(self, timeout: int = 60 * 60, pooling_frequency: int = 30) -> 'BaseEndpoint':
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)

        while datetime.now(tz=UTC) < timeout_dt:
            status = self.get_status()
            if status == 'active':
                return self
            self.logger.debug(f'Endpoint {self.endpoint_id} is in status {status}, waiting...')
            time.sleep(pooling_frequency)

        raise TimeoutError(f'Endpoint {self.endpoint_id} creation timeout')


class SourceEndpoint(BaseEndpoint):
    endpoint_type = 'source'

class TargetEndpoint(BaseEndpoint):
    endpoint_type = 'target'
    additional_settings = {
        "AfterConnectScript": "SET session_replication_role = replica",
    }
