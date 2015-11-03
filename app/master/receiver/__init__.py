import logging
log = logging.getLogger('root_logger')

from aws.sqs import workers_messaging_queue
from master.db.models import session, Task, Job
from sqlalchemy import and_
from datetime import datetime

def receive():
    log.info('Polling workers messaging queue')
    while True:
        #poll queue
        msg=workers_messaging_queue.poll()
        log.info('Received worker message')
        log.debug(msg)
        if 'type' in msg:
            if msg['type'] == 'task':
                task = session.query(Task).filter(Task.id == msg['data']['id']).first()
                if task:
                    task.status = msg['data']['status']
                    task.finished = datetime.strptime(msg['create_time'], '%Y-%m-%d %H:%M:%S.%f')
                    session.commit()
                    workers_messaging_queue.delete_message()
                    #check if all tasks are finished
                    uncompleted_tasks = session.query(Task).filter(and_(Task.job_id == task.job_id, Task.status != 'completed')).count()
                    if uncompleted_tasks == 0:
                        job = session.query(Job).filter(Job.id == task.job_id).first()
                        job.status = 'completed'
                        job.finished = datetime.utcnow()
                        log.info('Job (%s) Completed' % (task.job_id))
                        break
