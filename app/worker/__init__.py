import logging
log = logging.getLogger('root_logger')

from worker.sqs_poller import poll
from worker.task_executor import TaskExecutor
from worker.worker_logger import WorkerLoggingHandler
from aws.sqs import new_tasks_queue, workers_messaging_queue
from aws.s3 import s3

class Worker:
    def __init__(self):
        #Add worker logging handler
        self.worker_handler = WorkerLoggingHandler()
        log.addHandler(self.worker_handler)
    def run(self):
        while True:
            #poll for a task
            task_msg = new_tasks_queue.poll()
            #get the data from the message retainer
            task = task_msg.get_data()
            task = task['data']
            #check S3 for the task script as a missing task script means a
            #failed job and the task should be discarded
            if not s3.exists('job-'+task['job_id']+'/'+task['job_id']+'.py'):
                log.info('Discarding Task %s for Job <%s>' % (task['task_id'], task['job_id']))
                new_tasks_queue.delete_current_message()
                continue
            #insert job details into the logger
            self.worker_handler.set_job_id(task['job_id'])
            self.worker_handler.set_task_id(task['task_id'])
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
            #reset loggers task and job ids
            self.worker_handler.set_job_id(None)
            self.worker_handler.set_task_id(None)
