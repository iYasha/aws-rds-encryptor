import time
from datetime import UTC, datetime, timedelta
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from rds_encryptor.rds.parameter_group import ParameterGroup
from rds_encryptor.rds.snapshot import RDSSnapshot
from rds_encryptor.utils import MIGRATION_SEED, get_logger


class RDSInstance:
    logger = get_logger("RDSInstance")
    aws_client = boto3.client("rds")

    def __init__(
        self,
        instance_id: str,
        endpoint: str | None,
        port: int | None,
        master_username: str,
        master_password: str,
        parameter_group: ParameterGroup,
        tags: list[dict[str, str]] = None,  # noqa: RUF013
    ):
        self.instance_id = instance_id
        self._endpoint = endpoint
        self._port = port
        self.master_username = master_username
        self.master_password = master_password
        self.parameter_group = parameter_group
        if tags is None:
            tags = []
        self.tags = tags

    @property
    def endpoint(self) -> str:
        if self._endpoint is None:
            raise ValueError("You must call .wait_until_available() first to get the endpoint")
        return self._endpoint

    @property
    def port(self) -> int:
        if self._port is None:
            raise ValueError("You must call .wait_until_available() first to get the port")
        return self._port

    def _describe(self) -> dict:
        instances = self.aws_client.describe_db_instances(
            DBInstanceIdentifier=self.instance_id,
        )["DBInstances"]
        if len(instances) > 1:
            raise ValueError(f"Multiple instances found: {self.instance_id}")
        return instances[0]

    def get_engine_version(self) -> tuple[int, ...]:
        instance = self._describe()
        return tuple(map(int, instance.get("EngineVersion", "0.0").split(".")))

    @classmethod
    def from_id(cls, instance_id: str, root_password: str) -> Optional["RDSInstance"]:
        assert instance_id, "Instance ID is required"
        assert root_password, "Root password is required"

        try:
            instances = cls.aws_client.describe_db_instances(
                DBInstanceIdentifier=instance_id,
            )["DBInstances"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "DBInstanceNotFound":
                return None
            raise
        if len(instances) > 1:
            raise ValueError(f"Multiple instances found: {instance_id}")

        instance = instances[0]
        is_creating = instance["DBInstanceStatus"] == "creating"

        return cls(
            instance_id=instance_id,
            endpoint=None if is_creating else instance["Endpoint"]["Address"],
            port=None if is_creating else instance["Endpoint"]["Port"],
            master_username=instance["MasterUsername"],
            master_password=root_password,
            parameter_group=ParameterGroup.from_name(instance["DBParameterGroups"][0]["DBParameterGroupName"]),
            tags=instance.get("TagList"),
        )

    def get_status(self) -> str:
        instance = self._describe()
        return instance["DBInstanceStatus"]

    def take_snapshot(self) -> RDSSnapshot:
        snapshot_id = f"{self.instance_id}-{MIGRATION_SEED}-migration"
        self.logger.info('Taking snapshot "%s" for instance "%s" ...', snapshot_id, self.instance_id)
        snapshot = RDSSnapshot.from_id(snapshot_id)
        if snapshot is not None:
            self.logger.info('Snapshot "%s" already exists, skipping...', snapshot_id)
            return snapshot

        response = self.aws_client.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_id,
            DBInstanceIdentifier=self.instance_id,
            Tags=self.tags,
        )["DBSnapshot"]
        self.logger.info('Snapshot "%s" created', snapshot_id)
        return RDSSnapshot.from_id(snapshot_id=response["DBSnapshotIdentifier"])

    def set_parameter_group(self, parameter_group: ParameterGroup) -> "RDSInstance":
        self.logger.info(
            'Setting "%s" parameter group for "%s" instance...',
            self.instance_id,
            parameter_group.name,
        )
        self.aws_client.modify_db_instance(
            DBInstanceIdentifier=self.instance_id,
            DBParameterGroupName=parameter_group.name,
            ApplyImmediately=True,
        )
        self.parameter_group = parameter_group
        time.sleep(20)  # Wait for the parameter group to be applied
        self.logger.info(
            'Parameter group "%s" set for "%s" instance',
            parameter_group.name,
            self.instance_id,
        )
        return self

    def modify_instance(
        self,
        **params,
    ) -> "RDSInstance":
        params_str = ", ".join(f"{k}={v}" for k, v in params.items())
        self.logger.info('Modifying "%s" instance with %s ...', self.instance_id, params_str)
        self.aws_client.modify_db_instance(
            DBInstanceIdentifier=self.instance_id,
            ApplyImmediately=True,
            **params,
        )
        self.logger.info('"%s" instance modified', self.instance_id)
        return self

    def wait_until_available(self, timeout: int = 60 * 60, pooling_frequency: int = 30) -> "RDSInstance":
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)
        self.logger.info('Waiting for instance "%s" to become available ...', self.instance_id)

        while datetime.now(tz=UTC) < timeout_dt:
            instance = self._describe()
            status = instance["DBInstanceStatus"]
            if status == "available":
                self.logger.info('Instance "%s" is available', self.instance_id)
                self._endpoint = instance["Endpoint"]["Address"]
                self._port = instance["Endpoint"]["Port"]
                return self
            time.sleep(pooling_frequency)

        raise TimeoutError(f"Instance {self.instance_id} is not available after {timeout} seconds")
