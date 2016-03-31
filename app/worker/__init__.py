from worker.worker_logger import create_worker_logger, WorkerLoggingAdapter
log = create_worker_logger('root_logger')
log = WorkerLoggingAdapter(log)

from worker.sqs_poller import poll
from worker.task_executor import TaskExecutor
from worker.worker_logger import WorkerLoggingHandler
from aws.sqs import new_tasks_queue, workers_messaging_queue
from aws.s3 import s3
import time, os

class Worker:
    """
    This class polls the new tasks queue for new tasks. Every time a task
    is received, the existence of the job's task execution script is verified.
    If it does not exist, the task is discarded. If it does exist, it will be
    retrieved and the task will be executed and subsequently marked as completed
    or failed.
    """
    def __init__(self):
        log.info('Starting Worker Agent')
    def run(self):
        while True:
            #poll for a task
            task_msg = new_tasks_queue.poll()
            #get the data from the message retainer
            task = task_msg.get_data()
            task = task['data']
            #add job & log ids to log
            log.set_job_id(task['job_id'])
            log.set_task_id(task['task_id'])
            #check S3 for the task script as a missing task script means a
            #failed job and the task should be discarded
            if not s3.exists('job-'+task['job_id']+'/'+task['job_id']+'.py'):
                log.info('Discarding Task %s for Job <%s>' % (task['task_id'], task['job_id']))
                task_msg.delete()
                continue
            log.info('Received Task')
            #create executor
            executor = TaskExecutor(task_msg)
            #send task message to record the start time of the task
            task['status'] = 'executing'
            workers_messaging_queue.add_message(task, msg_type='task')
            #execute the task
            executor.run_execution()
            #delete the task message from the new tasks queue
            task_msg.delete()
            log.info('Finished Task')
            #remove job and task from log
            log.remove_job_id()
            log.remove_task_id()
            time.sleep(0.1)
