import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Optional

import boto3
from botocore.exceptions import ClientError

from rds_encryptor.utils import get_logger

if TYPE_CHECKING:
    from rds_encryptor.rds.instance import RDSInstance


class RDSSnapshot:
    logger = get_logger("RDSSnapshot")
    aws_client = boto3.client("rds")

    def __init__(self, snapshot_id: str, arn: str, tags: list[dict] = None):  # noqa: RUF013
        self.snapshot_id = snapshot_id
        self.arn = arn
        if tags is None:
            tags = []
        self.tags = tags

    def get_status(self):
        snapshot = self.aws_client.describe_db_snapshots(
            DBSnapshotIdentifier=self.snapshot_id,
        )["DBSnapshots"][0]
        return snapshot["Status"]

    @classmethod
    def from_id(cls, snapshot_id: str) -> Optional["RDSSnapshot"]:
        assert snapshot_id, "Snapshot ID is required"

        try:
            snapshots = cls.aws_client.describe_db_snapshots(
                DBSnapshotIdentifier=snapshot_id,
            )["DBSnapshots"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "DBSnapshotNotFound":
                return None
            raise
        if len(snapshots) > 1:
            raise ValueError(f"Multiple snapshots found: {snapshot_id}")

        snapshot = snapshots[0]

        return cls(
            snapshot_id=snapshot_id,
            arn=snapshot["DBSnapshotArn"],
            tags=snapshot.get("TagList"),
        )

    def copy_snapshot(
        self,
        encryption_kms_key_arn: str,
        copy_tags: bool = True,
    ) -> "RDSSnapshot":
        target_snapshot_id = f"{self.snapshot_id}-encrypted"
        self.logger.info('Copying and encrypting snapshot "%s" to "%s" ...', self.snapshot_id, target_snapshot_id)
        target_snapshot = self.from_id(snapshot_id=target_snapshot_id)
        if target_snapshot is not None:
            self.logger.info('Snapshot "%s" already exists', target_snapshot_id)
            return target_snapshot

        response = self.aws_client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=self.arn,
            TargetDBSnapshotIdentifier=target_snapshot_id,
            SourceRegion=self.aws_client.meta.region_name,
            KmsKeyId=encryption_kms_key_arn,
            CopyTags=copy_tags,
            Tags=self.tags,
        )
        self.logger.info('Snapshot "%s" is being copied', target_snapshot_id)
        return RDSSnapshot.from_id(response["DBSnapshot"]["DBSnapshotIdentifier"])

    def wait_until_created(self, timeout: int = 60 * 60, pooling_frequency: int = 60) -> "RDSSnapshot":
        self.logger.info('Waiting for snapshot "%s" to become available ...', self.snapshot_id)
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)

        while datetime.now(tz=UTC) < timeout_dt:
            status = self.get_status()
            if status == "available":
                self.logger.info('Snapshot "%s" is available', self.snapshot_id)
                return self
            if status == "failed":
                raise ValueError(f"Snapshot {self.snapshot_id} creation failed")
            time.sleep(pooling_frequency)

        raise TimeoutError(f"Snapshot {self.snapshot_id} creation timeout after {timeout} seconds")

    def restore_snapshot(
        self,
        instance_identifier: str,
        from_rds_instance: "RDSInstance",
        master_password: str,
        tags: list[dict[str, str]] = None,  # noqa: RUF013
    ) -> "RDSInstance":
        from rds_encryptor.rds.instance import RDSInstance

        old_instance = from_rds_instance._describe()

        tags = tags or []
        self.logger.info('Restoring snapshot "%s" to instance "%s" ...', self.snapshot_id, instance_identifier)
        response = self.aws_client.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=instance_identifier,
            DBSnapshotIdentifier=self.snapshot_id,
            PubliclyAccessible=old_instance["PubliclyAccessible"],
            DBSubnetGroupName=old_instance["DBSubnetGroup"]["DBSubnetGroupName"],
            VpcSecurityGroupIds=[sg["VpcSecurityGroupId"] for sg in old_instance["VpcSecurityGroups"]],
            CopyTagsToSnapshot=old_instance["CopyTagsToSnapshot"],
            Tags=tags,
            AvailabilityZone=old_instance["AvailabilityZone"],
        )
        self.logger.info('Instance "%s" is being restored', instance_identifier)
        return RDSInstance.from_id(
            instance_id=response["DBInstance"]["DBInstanceIdentifier"], root_password=master_password
        )
