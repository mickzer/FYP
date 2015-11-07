import logging
log = logging.getLogger('root_logger')

from aws.sqs import workers_messaging_queue
from master.db.models import session, Task, Job, WorkerLog
from sqlalchemy import and_
from datetime import datetime
from master.async_downloader import AsyncDownloader

class Receiver:
    def receive(self):
        log.info('Polling workers messaging queue')
        #Async downlaoder to download task outputs
        #as they are reported
        downloader = AsyncDownloader()
        downloader.start()
        while True:
            #poll queue
            msg=workers_messaging_queue.poll()
            log.info('Received worker message')
            log.debug(msg)
            if 'type' in msg:
                if msg['type'] == 'log':
                    #add to DB
                    l=WorkerLog(**msg['data'])
                    l.date=datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
                    session.add(l)
                    session.commit()
                    #delete message from SQS
                    workers_messaging_queue.delete_message()
                elif msg['type'] == 'task':
                    task = session.query(Task).filter(Task.id == msg['data']['id']).first()
                    if task:
                        task.status = msg['data']['status']
                        task.finished = datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
                        session.commit()
                        workers_messaging_queue.delete_message()
                        #download output if it was a successfult task
                        if task.status == 'completed':
                            downloader.add('job-'+task.job_id+'/output/'+str(task.task_id))
                        #check if all tasks are finished
                        uncompleted_tasks = session.query(Task).filter(and_(Task.job_id == task.job_id, Task.status != 'completed')).count()
                        if uncompleted_tasks == 0:
                            job = task.job
                            if job.final_script:
                                if job.final_script == 'merge':
                                    print 'merge task outputs'
                                else:
                                    job.execute_final_script()
                            job.status = 'master script executing'
                            job.finished = datetime.utcnow()

                            log.info('Job (%s) Completed' % (task.job_id))
                            break
