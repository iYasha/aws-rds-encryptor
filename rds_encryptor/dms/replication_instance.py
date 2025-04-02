from typing import Optional

import boto3


class ReplicationInstance:
    aws_client = boto3.client("dms")

    def __init__(self, arn: str):
        self.arn = arn

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
