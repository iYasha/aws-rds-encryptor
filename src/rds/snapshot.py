from typing import Optional, TYPE_CHECKING
import boto3
from datetime import datetime, UTC, timedelta
import time

from src.utils import get_logger

if TYPE_CHECKING:
    from src.rds.instance import RDSInstance


class RDSSnapshot:
    logger = get_logger('RDSSnapshot')
    aws_client = boto3.client('rds')

    def __init__(self, snapshot_id: str, arn: str, tags: list[dict] = None):
        self.snapshot_id = snapshot_id
        self.arn = arn
        if tags is None:
            tags = []
        self.tags = tags

    def get_status(self):
        snapshot = self.aws_client.describe_db_snapshots(
            DBSnapshotIdentifier=self.snapshot_id,
        )['DBSnapshots'][0]
        return snapshot['Status']

    @classmethod
    def from_id(cls, snapshot_id: str) -> Optional['RDSSnapshot']:
        assert snapshot_id, 'Snapshot ID is required'

        snapshots = cls.aws_client.describe_db_snapshots(
            DBSnapshotIdentifier=snapshot_id,
        )['DBSnapshots']
        if len(snapshots) == 0:
            return None
        elif len(snapshots) > 1:
            raise ValueError(f'Multiple snapshots found: {snapshot_id}')

        snapshot = snapshots[0]

        return cls(
            snapshot_id=snapshot_id,
            arn=snapshot['DBSnapshotArn'],
            tags=snapshot.get('TagList'),
        )

    def copy_snapshot(
        self,
        encryption_kms_key_arn: str,
        copy_tags: bool = True,
    ) -> 'RDSSnapshot':
        target_snapshot_id = f'{self.snapshot_id}-encrypted'
        target_snapshot = self.from_id(snapshot_id=target_snapshot_id)
        if target_snapshot is not None:
            return target_snapshot

        response = self.aws_client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=self.arn,
            TargetDBSnapshotIdentifier=target_snapshot_id,
            SourceRegion=self.aws_client.meta.region_name,
            KmsKeyId=encryption_kms_key_arn,
            CopyTags=copy_tags,
            Tags=self.tags,
        )
        return RDSSnapshot.from_id(response['DBSnapshot']['DBSnapshotIdentifier'])

    def wait_until_created(self, timeout: int = 60 * 60, pooling_frequency: int = 30) -> 'RDSSnapshot':
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)

        while datetime.now(tz=UTC) < timeout_dt:
            status = self.get_status()
            self.logger.info(f'[wait_until_created] Snapshot: {self.snapshot_id} status: {status}')
            if status == 'available':
                return self
            elif status == 'failed':
                raise ValueError(f'Snapshot {self.snapshot_id} creation failed')
            time.sleep(pooling_frequency)

        raise TimeoutError(f'Snapshot {self.snapshot_id} creation timeout after {timeout} seconds')


    def restore_snapshot(self, instance_identifier: str, tags: list[dict[str, str]] = None) -> 'RDSInstance':
        from src.rds.instance import RDSInstance

        tags = tags or []
        response = self.aws_client.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=instance_identifier,
            DBSnapshotIdentifier=self.snapshot_id,
            Tags=tags,
        )
        return RDSInstance.from_id(instance_id=response['DBInstance']['DBInstanceIdentifier'])
