import logging
log = logging.getLogger('root_logger')

from aws.sqs import workers_messaging_queue
from master.db.models import session, Task, Job, WorkerLog
from datetime import datetime
from master.async_downloader import AsyncDownloader

class Receiver:

    def __init__(self):
        self.downloader = AsyncDownloader()

    def receive(self):
        log.info('Polling workers messaging queue')
        #Async downlaoder to download task outputs
        #as they are reported
        self.downloader.start()
        while True:
            #poll queue
            msg=workers_messaging_queue.poll()
            log.info('Received worker message')
            log.debug(msg)
            if 'type' in msg:
                if msg['type'] == 'log':
                    self.log(msg)
                elif msg['type'] == 'task':
                    self.process_task_output(msg)

    def log(self, msg):
        #add to DB
        l=WorkerLog(**msg['data'])
        l.date=datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
        session.add(l)
        session.commit()
        #delete message from SQS
        workers_messaging_queue.delete_message()

    def process_task_output(self, msg):
        task = session.query(Task).filter(Task.id == msg['data']['id']).first()
        if task:
            task.status = msg['data']['status']
            task.finished = datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
            session.commit()
            #download output if it was a successful task and job has a final script
            if task.status == 'completed' and task.job.final_script:
                self.downloader.add(task.job.id, 'job-'+task.job_id+'/task_output/'+str(task.task_id))
            if task.status != 'completed':
                print 'NOOOOOOOOOOOOO!!!!'
            workers_messaging_queue.delete_message()

            #check if the whole job is completed
            if task.job.all_tasks_completed():
                log.info('Tasks Finished for Job <%s>' % task.job.id)
                task_output_download_queue = None
                if task.job.final_script:
                    task_output_download_queue = self.downloader.pause(task.job.id)
                task.job.finish(task_output_download_queue=task_output_download_queue)
                if self.downloader.paused:
                    self.downloader.resume()
