import logging
log = logging.getLogger('root_logger')

from worker.sqs_poller import poll
from worker.message_manager import MessageManager
from worker.task_executor import TaskExecutor
from worker.worker_logger import WorkerLoggingHandler
from aws.sqs import new_tasks_queue

class Worker:
    def __init__(self):
        #Add worker logging handler
        self.worker_handler = WorkerLoggingHandler()
        log.addHandler(self.worker_handler)
    def run(self):
        while True:
            #poll for a task
            task = new_tasks_queue.poll()
            #remove the SQS details container
            task=task['data']
            #insert job details into the logger
            self.worker_handler.job_id = task['job_id']
            self.worker_handler.task_id = task['task_id']
            log.info('Received Task')
            #create message manager
            m = MessageManager()
            m.start()
            #create executor and run
            executor = TaskExecutor(task)
            executor.run_execution()
            log.info('Finished Task')
            #reset loggers task and job ids
            self.worker_handler.job_id = None
            self.worker_handler.task_id = None
