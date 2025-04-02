import json
import time
from datetime import UTC, datetime, timedelta
from typing import Literal, NamedTuple

import boto3

from rds_encryptor.dms.endpoints import SourceEndpoint, TargetEndpoint
from rds_encryptor.dms.enums import MigrationType, ReplicationTaskStatus
from rds_encryptor.dms.replication_instance import ReplicationInstance
from rds_encryptor.utils import get_logger, normalize_aws_id


class TaskFailedException(Exception):
    def __init__(self, task, status, stop_reason, last_failure_message):
        self.task = task
        self.status = status
        self.stop_reason = stop_reason
        self.last_failure_message = last_failure_message


class TableMapping(NamedTuple):
    schema: Literal["%"] | str
    table: Literal["%"] | str
    action: Literal["include", "exclude"]


DEFAULT_REPLICATE_TASK_SETTINGS = {
    "Logging": {
        "EnableLogging": True,
        "EnableLogContext": True,
        "LogComponents": [
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TRANSFORMATION"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SOURCE_UNLOAD"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "IO"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TARGET_LOAD"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "PERFORMANCE"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SOURCE_CAPTURE"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SORTER"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "REST_SERVER"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "VALIDATOR_EXT"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TARGET_APPLY"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TASK_MANAGER"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TABLES_MANAGER"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "METADATA_MANAGER"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "FILE_FACTORY"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "COMMON"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "ADDONS"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "DATA_STRUCTURE"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "COMMUNICATION"},
            {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "FILE_TRANSFER"},
        ],
    },
    "StreamBufferSettings": {
        "StreamBufferCount": 3,
        "CtrlStreamBufferSizeInMB": 5,
        "StreamBufferSizeInMB": 8,
    },
    "ErrorBehavior": {
        "FailOnNoTablesCaptured": True,
        "ApplyErrorUpdatePolicy": "LOG_ERROR",
        "FailOnTransactionConsistencyBreached": False,
        "RecoverableErrorThrottlingMax": 1800,
        "DataErrorEscalationPolicy": "SUSPEND_TABLE",
        "ApplyErrorEscalationCount": 0,
        "RecoverableErrorStopRetryAfterThrottlingMax": True,
        "RecoverableErrorThrottling": True,
        "ApplyErrorFailOnTruncationDdl": False,
        "DataMaskingErrorPolicy": "STOP_TASK",
        "DataTruncationErrorPolicy": "LOG_ERROR",
        "ApplyErrorInsertPolicy": "LOG_ERROR",
        "EventErrorPolicy": "IGNORE",
        "ApplyErrorEscalationPolicy": "LOG_ERROR",
        "RecoverableErrorCount": -1,
        "DataErrorEscalationCount": 0,
        "TableErrorEscalationPolicy": "STOP_TASK",
        "RecoverableErrorInterval": 5,
        "ApplyErrorDeletePolicy": "IGNORE_RECORD",
        "TableErrorEscalationCount": 0,
        "FullLoadIgnoreConflicts": True,
        "DataErrorPolicy": "LOG_ERROR",
        "TableErrorPolicy": "SUSPEND_TABLE",
    },
    "TTSettings": {"TTS3Settings": None, "TTRecordSettings": None, "EnableTT": False},
    "FullLoadSettings": {
        "CommitRate": 10000,
        "StopTaskCachedChangesApplied": False,
        "StopTaskCachedChangesNotApplied": False,
        "MaxFullLoadSubTasks": 8,
        "TransactionConsistencyTimeout": 600,
        "CreatePkAfterFullLoad": False,
        "TargetTablePrepMode": "DO_NOTHING",
    },
    "ValidationSettings": {
        "ValidationPartialLobSize": 0,
        "PartitionSize": 10000,
        "RecordFailureDelayLimitInMinutes": 0,
        "SkipLobColumns": False,
        "ValidationS3Time": 0,
        "FailureMaxCount": 10000,
        "HandleCollationDiff": False,
        "ValidationQueryCdcDelaySeconds": 0,
        "ValidationMode": "ROW_LEVEL",
        "TableFailureMaxCount": 1000,
        "RecordFailureDelayInMinutes": 5,
        "MaxKeyColumnSize": 8096,
        "EnableValidation": True,
        "ThreadCount": 5,
        "RecordSuspendDelayInMinutes": 30,
        "ValidationS3Mask": 0,
        "ValidationOnly": False,
    },
    "TargetMetadata": {
        "ParallelApplyBufferSize": 0,
        "ParallelApplyQueuesPerThread": 0,
        "ParallelApplyThreads": 0,
        "TargetSchema": "",
        "InlineLobMaxSize": 0,
        "ParallelLoadQueuesPerThread": 0,
        "SupportLobs": True,
        "LobChunkSize": 0,
        "TaskRecoveryTableEnabled": False,
        "ParallelLoadThreads": 0,
        "LobMaxSize": 100_000,  # TODO: Might be issue if your LOB e.g. JSONB field is larger than 100MB
        "BatchApplyEnabled": False,
        "FullLobMode": False,
        "LimitedSizeLobMode": True,
        "LoadMaxFileSize": 0,
        "ParallelLoadBufferSize": 0,
    },
    "BeforeImageSettings": None,
    "ControlTablesSettings": {
        "historyTimeslotInMinutes": 5,
        "HistoryTimeslotInMinutes": 5,
        "StatusTableEnabled": False,
        "SuspendedTablesTableEnabled": False,
        "HistoryTableEnabled": False,
        "ControlSchema": "",
        "FullLoadExceptionTableEnabled": False,
    },
    "LoopbackPreventionSettings": None,
    "CharacterSetSettings": None,
    "FailTaskWhenCleanTaskResourceFailed": False,
    "ChangeProcessingTuning": {
        "StatementCacheSize": 50,
        "CommitTimeout": 1,
        "RecoveryTimeout": -1,
        "BatchApplyPreserveTransaction": True,
        "BatchApplyTimeoutMin": 1,
        "BatchSplitSize": 0,
        "BatchApplyTimeoutMax": 30,
        "MinTransactionSize": 1000,
        "MemoryKeepTime": 60,
        "BatchApplyMemoryLimit": 500,
        "MemoryLimitTotal": 1024,
    },
    "ChangeProcessingDdlHandlingPolicy": {
        "HandleSourceTableDropped": True,
        "HandleSourceTableTruncated": True,
        "HandleSourceTableAltered": True,
    },
    "PostProcessingRules": None,
}

DEFAULT_REPLICATE_TASK_SETTINGS_JSON = json.dumps(DEFAULT_REPLICATE_TASK_SETTINGS)


class MigrationTask:
    logger = get_logger("MigrationTask")
    aws_client = boto3.client("dms")

    def __init__(self, task_id: str, arn: str):
        self.task_id = task_id
        self.arn = arn

    def _describe(self) -> dict:
        response = self.aws_client.describe_replication_tasks(
            Filters=[{"Name": "replication-task-id", "Values": [self.task_id]}]
        )
        if len(response["ReplicationTasks"]) == 0:
            raise ValueError(f"Replication task not found: {self.task_id}")
        if len(response["ReplicationTasks"]) > 1:
            raise ValueError(f"Multiple replication tasks found: {self.task_id}")
        return response["ReplicationTasks"][0]

    def get_status(self) -> ReplicationTaskStatus:
        return ReplicationTaskStatus(self._describe()["Status"])

    def _wait_until(
        self,
        expected_status: ReplicationTaskStatus,
        timeout: int = 4 * 60 * 60,
        pooling_frequency: int = 60,
    ) -> "MigrationTask":
        """
        :param expected_status: ReplicationTaskStatus
        :param timeout: Timeout in seconds. Default is 4 hours
        :param pooling_frequency: Pooling frequency in seconds. Default is 1 minute.
        :return: MigrationTask or raise TimeoutError
        """
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)

        while datetime.now(tz=UTC) < timeout_dt:
            status = self.get_status()
            if status == expected_status:
                self.logger.info("Task %s is in status %s", self.task_id, status)
                return self
            self.logger.debug("Task %s is in status %s, waiting...", self.task_id, status)
            time.sleep(pooling_frequency)

        raise TimeoutError(f"Task {self.task_id} status is not {expected_status} after {timeout} seconds")

    def wait_until_ready(self) -> "MigrationTask":
        self.logger.info("Waiting for task %s to be ready ...", self.task_id)
        return self._wait_until(ReplicationTaskStatus.READY)

    def run_task(self) -> "MigrationTask":
        self.logger.info("Starting task %s ...", self.task_id)
        self.aws_client.start_replication_task(
            ReplicationTaskArn=self.arn, StartReplicationTaskType="start-replication"
        )
        self.logger.info("Task %s started", self.task_id)
        return self

    def wait_until_finished(self, timeout: int = 4 * 60 * 60, pooling_frequency: int = 2 * 60) -> "MigrationTask":
        timeout_dt = datetime.now(tz=UTC) + timedelta(seconds=timeout)

        while datetime.now(tz=UTC) < timeout_dt:
            response = self._describe()
            status = ReplicationTaskStatus(response["Status"])
            stop_reason = response.get("StopReason")
            last_failure_message = response.get("LastFailureMessage")
            full_load_progress = response.get("ReplicationTaskStats", {}).get("FullLoadProgressPercent", 0)
            if status == ReplicationTaskStatus.STOPPED and stop_reason == "Stop Reason NORMAL":
                self.logger.info("[Task %s] Task finished", self.task_id)
                return self
            if status == ReplicationTaskStatus.RUNNING and full_load_progress == 100:
                self.logger.info("[Task %s] Full load completed", self.task_id)
                return self
            if status in (ReplicationTaskStatus.STOPPED, ReplicationTaskStatus.FAILED):
                raise TaskFailedException(
                    task=self,
                    status=status,
                    stop_reason=stop_reason,
                    last_failure_message=last_failure_message,
                )

            self.logger.info(
                "[Task %s] Task status: %s, full load progress %s/100 waiting...",
                self.task_id,
                status,
                full_load_progress,
            )
            time.sleep(pooling_frequency)

        raise TimeoutError(f"Task {self.task_id} status is not STOPPED after {timeout} seconds")

    @classmethod
    def create_migration_task(
        cls,
        name,
        source_endpoint: SourceEndpoint,
        target_endpoint: TargetEndpoint,
        replication_instance: ReplicationInstance,
        migration_type: MigrationType,
        table_mappings: list[TableMapping],
        tags: list[dict[str, str]] = None,  # noqa: RUF013
    ):
        # TODO: Add check if the task already exists
        table_mappings_rules = {
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": str(idx + 1),
                    "rule-name": str(idx + 1),
                    "object-locator": {
                        "schema-name": rule.schema,
                        "table-name": rule.table,
                    },
                    "rule-action": rule.action,
                    "filters": [],
                }
                for idx, rule in enumerate(table_mappings)
            ]
        }
        normalized_id = normalize_aws_id(name)
        cls.logger.info(
            'Creating migration task "%s" from "%s" to "%s" '
            'with migration type "%s" and table mappings "%s" on replication instance "%s" ...',
            normalized_id,
            source_endpoint.endpoint_id,
            target_endpoint.endpoint_id,
            str(migration_type),
            str(table_mappings),
            replication_instance.arn,
        )
        response = cls.aws_client.create_replication_task(
            ReplicationTaskIdentifier=normalized_id,
            SourceEndpointArn=source_endpoint.arn,
            TargetEndpointArn=target_endpoint.arn,
            ReplicationInstanceArn=replication_instance.arn,
            MigrationType=str(migration_type),
            TableMappings=json.dumps(table_mappings_rules),
            ReplicationTaskSettings=DEFAULT_REPLICATE_TASK_SETTINGS_JSON,
            Tags=tags or [],
        )["ReplicationTask"]
        # Replication Task is modifying the replication instance, so we need to wait until it's active
        replication_instance.wait_until_active()
        cls.logger.info('Migration task "%s" created', normalized_id)
        return cls(task_id=response["ReplicationTaskIdentifier"], arn=response["ReplicationTaskArn"])
