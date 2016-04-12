import logging
log = logging.getLogger('root_logger')

import threading, time
from aws.sqs import workers_messaging_queue
from master.db.models import Session, Task, Job, Log
from datetime import datetime
from master.task_completion_script_executor import TaskCompletionScriptExecutor

class Receiver(threading.Thread):
    """
    Consumes the worker's messaging queue and performs the relevant operation
    depending on the operation received. This can include writing a log message
    to the database, executing a task completion script, marking a job as failed,
    queing a final script execution and marking a job as completed.
    """
    def __init__(self, job_big_operation_controller, async_downloader):
        threading.Thread.__init__(self)
        #should prob throw exception is these are null
        self.job_big_operation_controller = job_big_operation_controller
        self.async_downloader = async_downloader
        self.log_buffer = []
        self.log_buffer_count = 0

    def run(self):
        self.session = Session()
        log.info('Polling workers messaging queue')
        while True:
            if self.log_buffer_count == 5:
                self._commit_buffered_logs()
            #poll queue
            msg_batch = workers_messaging_queue.poll_batch()
            msg_batch_data = msg_batch.get_data()
            log.info('Received worker message batch')
            # log.debug(msg_batch_data, extra={'db_ignore':True})
            for msg in msg_batch_data:
                if 'type' in msg:
                    # self.session = Session()
                    #skip it if the job no longer exists
                    if 'job_id' in msg['data'] and not self.session.query(Job).filter(Job.id == msg['data']['job_id']).first():
                        continue
                    if msg['type'] == 'log':
                        self._add_to_log(msg)
                    elif msg['type'] == 'task':
                        #commit any buffered logs first
                        self._commit_buffered_logs()
                        self._process_task_message(msg)
                time.sleep(0.01)
            msg_batch.delete()

    def _commit_buffered_logs(self):
        for log in self.log_buffer:
            self.session.add(log)
        self.session.commit()
        self.log_buffer = []
        self.log_buffer_count = 0

    def _add_to_log(self, msg):
        #add to DB
        msg['data']['type'] = 'worker'
        l=Log(**msg['data'])
        print l
        try:
            l.date=datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
        except:
            l.date=datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S')
        self.log_buffer.append(l)
        self.log_buffer_count += 1

    def _process_task_message(self, msg):
        task = self.session.query(Task).filter(Task.id == msg['data']['id']).first()
        #is running condition potentially looses data.
        #should do checks for if task is already completed/failed instead
        #currently being used to avoid inaccurate db info as a result of out of
        #order SQS messages
        if task and task.job.is_running():
            #out of order SQS messages
            if msg['data']['status'] == 'executing' and task.status == 'completed':
                task.started = datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
                task.output_data = msg['data']['output_data']
                self.session.commit()
            if task.status != 'completed':
                job = self.session.query(Job).filter(Job.id == task.job_id).first()
                #need to give the instances the SQLAlchemy Session
                task.set_session(self.session)
                job.set_session(self.session)
                task.status = msg['data']['status']
                self.session.commit()
                if task.status == 'executing':
                    #tell the db when the task started executing
                    task.started = datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
                    self.session.commit()
                    log.info('%s Started Executing at %s' % (task, msg['create_time']))
                elif task.status == 'completed':
                    self._task_completed(job, task, msg)
                elif task.status == 'failed':
                    self._task_failed(job, task, msg)
                #check if the whole job is completed
                if job.all_tasks_completed():
                    self._all_tasks_completed(job)

    def _task_completed(self, job, task, msg):
        context = {}
        if job.task_completion_script:
            #execute task completion script
            log.info('Starting Task Completion Script Executor')
            exc = TaskCompletionScriptExecutor(task, job)
            exc.run_execution()
            #See Comment below
            context  = job.task_completion_context
            log.info('Task Completion Script Executor Finished')
        #if job has final script, queue task output for async downlaod
        if job.final_script and task.output_data:
            self.async_downloader.add(job.id, task.task_id)
        #tell db when the task finshed
        #note sure why, but I have to rollback the session and set the
        #updated task completion context like this before committing.
        #need to read up on the SQLAlchemy Session manager. Come back to this.
        self.session.rollback()
        job.task_completion_context = context
        task.finished = datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
        self.session.commit()
        log.info('%s Completed at %s' % (task, msg['create_time']))

    def _task_failed(self, job, task, msg):
        #check to see if we have reached the failed task threshold
        failed_tasks_count = self.session.query(Task).filter(Task.status == 'failed').count()
        #mark job as failed if we've hit the threshold
        if failed_tasks_count >= job.failed_tasks_threshold:
            job.mark_as_failed()
        log.info('%s Failed at %s' % (task, msg['create_time']))

    def _all_tasks_completed(self, job):
        log.info('Tasks Finished for %s' % job)
        job.status = 'tasks completed'
        self.session.commit()
        if job.final_script:
            #remove job from this threads session
            self.session.expunge(job)
            job.session = None
            #give it to the big op controller
            self.job_big_operation_controller.add(job)
        else:
            job.mark_as_completed()
