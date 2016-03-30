import global_conf, os
from master.db.models import Task
from master.db.models import Session
from sqlalchemy import desc

def create_task(job_id, file_name=None, attributes={}, task_id=None):
    session = Session()
    #get last task id if not set
    if not task_id:
        max_task=session.query(Task).filter(Task.job_id==job_id).order_by(desc(Task.task_id)).first()
        print str(max_task) + ', ' + str(max_task.task_id)
        if max_task:
            task_id = max_task.task_id+1
        else:
            task_id=1
    task = Task(job_id=job_id, file_name=file_name, attributes=attributes, task_id=task_id)
    session.add(task)
    session.commit()
    task.set_session(session)
    return task

def create_temp_file(job_id, file_name):
    return open(global_conf.CWD+'job-'+job_id+'/split/'+file_name, 'w+')

def delete_temp_file(job_id, file_name):
    os.remove(global_conf.CWD+'job-'+job_id+'/split/'+file_name)

def upload_temp_file(file_name, job_id):
    key = '/job-'+job_id+'/'+file_name
    filepath = global_conf.CWD+'job-'+job_id+'/split/'+file_name
    return s3.put(filepath, key)
