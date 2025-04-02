import time
from datetime import UTC, datetime, timedelta
from typing import Optional

import boto3

from rds_encryptor.utils import get_logger


class ReplicationInstance:
    aws_client = boto3.client("dms")
    logger = get_logger("ReplicationInstance")

    def __init__(self, arn: str):
        self.arn = arn

    def get_status(self) -> str:
        response = self.aws_client.describe_replication_instances(
            Filters=[{"Name": "replication-instance-arn", "Values": [self.arn]}]
        )["ReplicationInstances"]
        if len(response) == 0:
            raise ValueError(f"Replication instance not found: {self.arn}")
        if len(response) > 1:
            raise ValueError(f"Multiple replication instances found: {self.arn}")

        return response[0]["ReplicationInstanceStatus"]

    def wait_until_active(self, timeout: int = 60 * 60, pooling_frequency: int = 60) -> "ReplicationInstance":
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)
        self.logger.info('Waiting for replication instance "%s" to become active ...', self.arn)

        while datetime.now(tz=UTC) < timeout_dt:
            status = self.get_status()
            if status == "available":
                self.logger.info('Replication instance "%s" is active', self.arn)
                return self
            self.logger.debug("Replication instance %s is in status %s, waiting...", self.arn, status)
            time.sleep(pooling_frequency)

        raise TimeoutError(f"Replication instance {self.arn} creation timeout")

    @classmethod
    def from_arn(cls, arn: str) -> Optional["ReplicationInstance"]:
        assert arn, "Replication instance ARN is required"

        response = cls.aws_client.describe_replication_instances(
            Filters=[{"Name": "replication-instance-arn", "Values": [arn]}]
        )["ReplicationInstances"]
        if len(response) == 0:
            raise ValueError(f"Replication instance not found: {arn}")
        if len(response) > 1:
            raise ValueError(f"Multiple replication instances found: {arn}")

        return cls(arn=arn)
