import logging
log = logging.getLogger('root_logger')

from master.db.models import session, Job
from aws.s3 import s3
import os

def create_job(**kwargs):
    try:
        j = Job(
            name=kwargs['name'],
            executable_key_path=kwargs['executable_key_path'],
            input_key_path=kwargs['input_key_path'],
            final_script=kwargs['final_script_key_path']
            )
        session.add(j)
        session.commit()
        log.info('Created Job %s' % (j))
        return j.id
    except Exception, e:
        log.error('DB Error Creating Job', exc_info=True)

def submit_job(job_id):
    #put script in findable s3 folder
    job = session.query(Job).filter(Job.id == job_id).first()
    if job:
        log.info('Submitting Job %s' % (job))
        #copy script to the job folder
        log.info('Copying Executable to the S3 Job Folder')
        s3.copy(job.executable_key_path, 'job-'+job.id+'/'+job.id+'.py')
        log.info('Creating Tasks for Job: %s' % (job))
        job.submit()
