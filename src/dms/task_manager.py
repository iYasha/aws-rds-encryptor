from threading import Thread

from src.utils import get_logger
from src.dms.migration_task import MigrationTask, TaskFailedException


class MigrationTaskManager:
    logger = get_logger('MigrationTaskManager')

    def __init__(self):
        self.tasks: list['MigrationTask'] = []
        self.errors = []

    def add_task(self, task: 'MigrationTask'):
        self.tasks.append(task)

    def run_task(self, task: 'MigrationTask'):
        try:
            task.wait_until_finished()
        except TaskFailedException as e:
            self.errors.append(e)
            self.logger.error(
                f'[Task {e.task.task_id}] Status={e.status}; Stopped reason={e.stop_reason}; Last failure message={e.last_failure_message}'
            )
        except TimeoutError as e:
            self.errors.append(e)
            self.logger.error(f'[Task {task.task_id}] Timeout error: {e}')

    def run_all(self) -> bool:
        self.errors = []
        threads = []
        for task in self.tasks:
            thread = Thread(target=self.run_task, args=(task,))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        self.logger.info('Migration tasks finished')
        return not self.errors

