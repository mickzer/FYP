import global_conf, os
from master.db.models import Task
from master.db.models import Session
from sqlalchemy.sql import func

def create_task(job_id, file_name=None, task_id=None):
    session = Session()
    #get last task id if not set
    if task_id is None:
        max_task=session.query(Task).filter(Task.task_id==func.max(Task.task_id).select()).first()
        if max_task:
            task_id = max_task.task_id+1
        else:
            task_id=1
    task = Task(job_id=job_id, file_name=file_name, task_id=task_id)
    session.add(task)
    session.commit()
    task.set_session(session)
    return task

def create_temp_task_file(job_id, file_name):
    return open(global_conf.CWD+'job-'+job_id+'/input/'+file_name, 'w+')

def delete_temp_task_file(job_id, file_name):
    os.remove(global_conf.CWD+'job-'+job_id+'/input/'+file_name)
