import logging
log = logging.getLogger('root_logger')

import threading, time
from aws.sqs import workers_messaging_queue
from master.db.models import Session, Task, Job, Log
from datetime import datetime


class Receiver(threading.Thread):
    def __init__(self, job_big_operation_controller, async_downloader):
        threading.Thread.__init__(self)
        #should prob throw exception is these are null
        self.job_big_operation_controller = job_big_operation_controller
        self.async_downloader = async_downloader

    def run(self):
        log.info('Polling workers messaging queue')
        while True:
            #poll queue
            msg=workers_messaging_queue.poll()
            log.info('Received worker message')
            log.debug(msg)
            if 'type' in msg:
                self.session = Session()
                #skip it if the job no longer exists
                if 'job_id' in msg['data'] and not self.session.query(Job).filter(Job.id == msg['data']['job_id']).first():
                    workers_messaging_queue.delete_message()
                    continue
                if msg['type'] == 'log':
                    self.log(msg)
                elif msg['type'] == 'task':
                    self.process_task_output(msg)
                self.session.close()
            time.sleep(0.01)

    def log(self, msg):
        #add to DB
        msg['data']['type'] = 'worker'
        l=Log(**msg['data'])
        try:
            l.date=datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
        except:
            l.date=datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S')
        self.session.add(l)
        self.session.commit()
        #delete message from SQS
        workers_messaging_queue.delete_message()

    def process_task_output(self, msg):
        task = self.session.query(Task).filter(Task.id == msg['data']['id']).first()
        if task and task.job.is_running():
            job = task.job
            #need to give the instances the SQLAlchemy Session
            task.set_session(self.session)
            job.set_session(self.session)
            task.status = msg['data']['status']
            self.session.commit()
            if msg['data']['status'] == 'executing':
                task.started = datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
                self.session.commit()
                log.info('%s Started Executing at %s' % (task, msg['create_time']))
            #download output if it was a successful task and job has a final script
            elif task.status == 'completed':
                if job.final_script:
                    self.async_downloader.add(job.id, task.task_id)
                task.finished = datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
                self.session.commit()
            elif task.status == 'failed':
                #check to see if we have reached the failed task threshold
                failed_tasks_count = self.session.query(Task).filter(Task.status == 'failed').count()
                #mark job as failed if we've hit the threshold
                if failed_tasks_count >= job.failed_tasks_threshold:
                    job.mark_as_failed()
            workers_messaging_queue.delete_message()

            #check if the whole job is completed
            if job.all_tasks_completed():
                log.info('Tasks Finished for %s' % job)
                job.status = 'tasks completed'
                self.session.commit()
                if job.final_script:
                    #remove job from this threads session
                    self.session.expunge(job)
                    #give it to the big op controller
                    self.job_big_operation_controller.add(job)
                else:
                    job.task_only_completion()
