from typing import Optional

import boto3
from botocore.exceptions import ClientError

from rds_encryptor.utils import MIGRATION_SEED, get_logger


def build_shared_preload_libraries_param(*libraries: str) -> str:
    return ",".join(map(str.strip, libraries))


def get_migration_parameter_group_name(parameter_group_name: str) -> str:
    salt = f"-{MIGRATION_SEED}-migration"
    if parameter_group_name.endswith(salt):
        return parameter_group_name
    return f"{parameter_group_name}{salt}"


def get_original_parameter_group(migration_parameter_group_name) -> str:
    return migration_parameter_group_name.replace(f"-{MIGRATION_SEED}-migration", "")


class ParameterGroup:
    aws_client = boto3.client("rds")
    logger = get_logger("ParameterGroup")

    def __init__(self, name: str):
        self.name = name
        self.properties = self._fetch_properties()

    @classmethod
    def from_name(cls, name: str) -> Optional["ParameterGroup"]:
        assert name, "Parameter group name is required"

        try:
            response = cls.aws_client.describe_db_parameter_groups(
                DBParameterGroupName=name,
            )["DBParameterGroups"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "DBParameterGroupNotFound":
                return None
            raise
        if len(response) > 1:
            raise ValueError(f"Multiple parameter groups found: {name}")

        return cls(name=name)

    def copy(self) -> "ParameterGroup":
        new_parameter_group_name = get_migration_parameter_group_name(self.name)
        self.logger.info('Copying parameter group "%s" to "%s" ...', self.name, new_parameter_group_name)
        response = self.aws_client.copy_db_parameter_group(
            SourceDBParameterGroupIdentifier=self.name,
            TargetDBParameterGroupIdentifier=new_parameter_group_name,
            TargetDBParameterGroupDescription=f"{self.name} migration parameter group",
        )
        self.logger.info('Parameter group "%s" copied', new_parameter_group_name)
        return ParameterGroup(name=response["DBParameterGroup"]["DBParameterGroupName"])

    def _fetch_properties(self):
        response = self.aws_client.describe_db_parameters(
            DBParameterGroupName=self.name,
        )
        parameters = response.get("Parameters", [])
        while response.get("Marker"):
            response = self.aws_client.describe_db_parameters(
                DBParameterGroupName=self.name,
                Marker=response["Marker"],
            )
            parameters.extend(response.get("Parameters", []))
        return {
            param["ParameterName"]: {"value": param["ParameterValue"], "apply_type": param["ApplyType"]}
            for param in parameters
            if "ParameterValue" in param
        }

    @property
    def wal_sender_timeout(self) -> int:
        return int(self.properties.get("wal_sender_timeout", {"value": 0})["value"])

    @property
    def shared_preload_libraries(self) -> list[str]:
        return list(
            map(
                str.strip,
                self.properties.get("shared_preload_libraries", {"value": ""})["value"].split(","),
            )
        )

    @property
    def rds_logical_replication(self) -> int:
        return int(self.properties.get("rds.logical_replication", {"value": 0})["value"])

    def set_parameter(self, name: str, value: any) -> None:
        parameter = {"ParameterName": name, "ParameterValue": str(value)}
        old_parameter = self.properties.get(name)
        if old_parameter is not None and old_parameter["apply_type"] != "static":
            parameter["ApplyMethod"] = "immediate"
        else:
            parameter["ApplyMethod"] = "pending-reboot"
        self.aws_client.modify_db_parameter_group(
            DBParameterGroupName=self.name,
            Parameters=[parameter],
        )
        self.properties = self._fetch_properties()

    def delete(self) -> None:
        self.logger.info('Deleting parameter group "%s" ...', self.name)
        self.aws_client.delete_db_parameter_group(
            DBParameterGroupName=self.name,
        )
        self.logger.info('Parameter group "%s" deleted', self.name)
