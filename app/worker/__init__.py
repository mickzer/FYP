import logging
log = logging.getLogger('root_logger')

from worker.sqs_poller import poll
from worker.message_manager import MessageManager
from worker.task_executor import run_execution
from aws.sqs import new_tasks_queue

class Worker:
    def __init__(self):
        self.name = 'worker'
    def run(self):
        while True:
            #poll for a task
            task = poll()
            #remove the SQS details container
            task=task['data']
            log.info('Received Task %s for job-%s' % (str(task['task_id']), task['job_id']))
            #create message manager
            m = MessageManager()
            #execute task
            run_execution(task)
            log.info('Finished Task %s for job-%s' % (str(task['task_id']), task['job_id']))
