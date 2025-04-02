from threading import Thread

from rds_encryptor.dms.migration_task import MigrationTask, TaskFailedException
from rds_encryptor.utils import get_logger


class MigrationTaskManager:
    logger = get_logger("MigrationTaskManager")

    def __init__(self):
        self.tasks: list["MigrationTask"] = []
        self.errors = []

    def add_task(self, task: "MigrationTask"):
        self.tasks.append(task)

    def run_task(self, task: "MigrationTask"):
        try:
            self.logger.info('Starting database migration task "%s" ...', task.task_id)
            task.wait_until_ready().run_task().wait_until_finished()
            self.logger.info('Database migration task "%s" finished successfully', task.task_id)
        except TaskFailedException as e:
            self.errors.append(e)
            self.logger.error(
                'Database migration task "%s" with status %s because "%s" with last failure message "%s"',
                task.task_id,
                e.status,
                e.stop_reason,
                e.last_failure_message,
            )
        except TimeoutError as e:
            self.errors.append(e)
            self.logger.error(
                "[Task %s] Timeout error: %s. "
                "Task might be still running, if so, please increase timeout and try again.",
                task.task_id,
                e,
            )

    def run_all(self) -> bool:
        self.errors = []
        threads = []
        for task in self.tasks:
            thread = Thread(target=self.run_task, args=(task,))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        return not self.errors
