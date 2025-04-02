from enum import StrEnum


class MigrationType(StrEnum):
    migrate = "full-load"
    replicate = "cdc"
    migrate_replicate = "full-load-and-cdc"


class ReplicationTaskStatus(StrEnum):
    """
    "moving" - The task is being moved in response to running the MoveReplicationTask operation.
    "creating" - The task is being created in response to running the CreateReplicationTask operation.
    "deleting" - The task is being deleted in response to running the DeleteReplicationTask operation.
    "failed" - The task failed to successfully complete the database migration in response to running the StartReplicationTask operation.
    "failed-move" - The task failed to move in response to running the MoveReplicationTask operation.
    "modifying" - The task definition is being modified in response to running the ModifyReplicationTask operation.
    "ready" - The task is in a ready state where it can respond to other task operations, such as StartReplicationTask or DeleteReplicationTask.
    "running" - The task is performing a database migration in response to running the StartReplicationTask operation.
    "starting" - The task is preparing to perform a database migration in response to running the StartReplicationTask operation.
    "stopped" - The task has stopped in response to running the StopReplicationTask operation.
    "stopping" - The task is preparing to stop in response to running the StopReplicationTask operation.
    "testing" - The database migration specified for this task is being tested in response to running either the StartReplicationTaskAssessmentRun or the StartReplicationTaskAssessment operation.
    """  # noqa: E501

    MOVING = "moving"
    CREATING = "creating"
    DELETING = "deleting"
    FAILED = "failed"
    FAILED_MOVE = "failed-move"
    MODIFYING = "modifying"
    READY = "ready"
    RUNNING = "running"
    STARTING = "starting"
    STOPPED = "stopped"
    STOPPING = "stopping"
    TESTING = "testing"
