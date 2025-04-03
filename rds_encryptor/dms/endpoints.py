import abc
import time
from datetime import UTC, datetime, timedelta
from typing import Literal, Optional

import boto3
from botocore.exceptions import ClientError

from rds_encryptor.rds.instance import RDSInstance
from rds_encryptor.utils import MIGRATION_SEED, get_logger, normalize_aws_id


class BaseEndpoint(abc.ABC):
    logger = get_logger("BaseEndpoint")
    aws_client = boto3.client("dms")
    endpoint_type: Literal["source", "target"]
    additional_settings: dict[str, str] = None

    def __init__(self, rds_instance: RDSInstance, database: str, kms_key_arn: str):
        self.rds_instance = rds_instance
        self.database = database
        self.endpoint_id = normalize_aws_id(
            f"{self.endpoint_type}-{self.rds_instance.instance_id}-{self.database}-{MIGRATION_SEED}"
        )
        self.kms_key_arn = kms_key_arn
        self._arn = None

    @property
    def arn(self) -> str:
        if self._arn is None:
            raise ValueError("You must call .get_or_create_endpoint() first to get the ARN")
        return self._arn

    def create_endpoint(self) -> "BaseEndpoint":
        assert self.endpoint_type in (
            "source",
            "target",
        ), f"Invalid endpoint type: {self.endpoint_type}"
        response = self.aws_client.create_endpoint(
            EndpointIdentifier=self.endpoint_id,
            EndpointType=self.endpoint_type,
            EngineName="postgres",
            KmsKeyId=self.kms_key_arn,
            # For PostgreSQL 15 and later, SSL mode is required and enabled by default
            SslMode="require" if self.rds_instance.get_engine_version()[0] >= 15 else "none",
            PostgreSQLSettings={
                "DatabaseName": self.database,
                "Port": self.rds_instance.port,
                "ServerName": self.rds_instance.endpoint,
                "Username": self.rds_instance.master_username,
                "Password": self.rds_instance.master_password,
                **(self.additional_settings or {}),
            },
            Tags=self.rds_instance.tags,
        )
        self.logger.info('%s endpoint "%s" created', self.endpoint_type.capitalize(), self.endpoint_id)
        self._arn = response["Endpoint"]["EndpointArn"]
        return self

    def _describe(self):
        response = self.aws_client.describe_endpoints(Filters=[{"Name": "endpoint-id", "Values": [self.endpoint_id]}])
        if len(response["Endpoints"]) > 1:
            raise ValueError(f"Multiple endpoints found: {self.endpoint_id}")

        return response["Endpoints"][0]

    def get_status(self) -> str:
        return self._describe()["Status"]

    def get_endpoint(self) -> Optional["BaseEndpoint"]:
        try:
            self._arn = self._describe()["EndpointArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundFault":
                return None
            raise
        return self

    def get_or_create_endpoint(self) -> "BaseEndpoint":
        self.logger.info(
            'Creating %s endpoint "%s" for "%s" database', self.endpoint_type, self.endpoint_id, self.database
        )
        endpoint = self.get_endpoint()
        if self._arn is not None:
            self.logger.info('Endpoint "%s" already exists', self.endpoint_id)
            return endpoint
        return self.create_endpoint()

    def wait_until_created(self, timeout: int = 60 * 60, pooling_frequency: int = 30) -> "BaseEndpoint":
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)
        self.logger.info('Waiting for %s endpoint "%s" to become available ...', self.endpoint_type, self.endpoint_id)

        while datetime.now(tz=UTC) < timeout_dt:
            status = self.get_status()
            if status == "active":
                self.logger.info('Endpoint "%s" is active', self.endpoint_id)
                return self
            self.logger.debug("Endpoint %s is in status %s, waiting...", self.endpoint_id, status)
            time.sleep(pooling_frequency)

        raise TimeoutError(f"Endpoint {self.endpoint_id} creation timeout")


class SourceEndpoint(BaseEndpoint):
    endpoint_type = "source"


class TargetEndpoint(BaseEndpoint):
    endpoint_type = "target"
    additional_settings = {
        "AfterConnectScript": "SET session_replication_role = replica",
    }
