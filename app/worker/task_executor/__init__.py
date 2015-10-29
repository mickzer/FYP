import logging
log = logging.getLogger('root_logger')

import subprocess, os, shutil, global_conf
from aws.s3 import s3
from aws.sqs import workers_messaging_queue, new_tasks_queue


def get_script(job_id):
    log.info('Getting Exceutable Script')
    if os.path.exists(global_conf.CWD+'worker_scripts/'+job_id+'.py'):
        log.info('Exceutable Script was Cached')
        return True
    else:
        if not os.path.isdir(global_conf.CWD+'worker_scripts/'):
            os.makedirs(global_conf.CWD+'worker_scripts/')
        log.info('Downloading Exceutable Script from S3')
        if s3.get('job-'+job_id+'/'+job_id+'.py', file_path=global_conf.CWD+'worker_scripts/'+job_id+'.py'):
            log.info('Exceutable Script Downloaded')
            return True
        log.error('Failed to Download Executable Script')
        return False

def get_input_split(task):
    #create the directory structure
    #ensure/create worker_data
    if not os.path.isdir(global_conf.CWD+'worker_data/'):
        os.makedirs(global_conf.CWD+'worker_data/')
    #ensure/create job dir
    if not os.path.isdir(global_conf.CWD+'worker_data/job-'+task['job_id']):
        os.makedirs(global_conf.CWD+'worker_data/job-'+task['job_id'])
    #ensure/create task dir
    task_dir = global_conf.CWD+'worker_data/job-'+task['job_id']+'/task-'+str(task['task_id'])
    if not os.path.isdir(task_dir):
        os.makedirs(task_dir)
    #download input split from S3
    log.info('Downloading Input Split')
    if s3.get('job-'+task['job_id']+'/split_input/'+task['file_name'], task_dir+'/data'):
        log.info('Input Split Downloaded')
        return True
    log.error('Failed to get input split from S3')
    return False

def before_execute(task):
    if not get_script(task['job_id']):
        return False
    if not get_input_split(task):
        return False
    return True

def upload_output(task):
    log.info('Uploading Task Output to S3')
    task_dir = global_conf.CWD+'worker_data/job-'+task['job_id']+'/task-'+str(task['task_id'])
    key = '/job-'+task['job_id']+'/output/'+str(task['task_id'])
    if s3.put(task_dir+'/output', key):
        return True
    log.error('Failed to Upload Task Output')
    return False

def message_completion(task):
    log.info('Adding task completion to SQS Queue')
    task['status'] = 'completed'
    if workers_messaging_queue.add_message(task, msg_type='task'):
        return True
    log.error('Failed to add task completion to SQS Queue')
    return False

def delete_task():
    log.info('Deleting task from new tasks SQS Queue')
    if new_tasks_queue.delete_message():
        return True
    log.error('Failed to delete task from new tasks SQS Queue')
    return False

def delete_local_data(task):
    log.info('Deleting local task data')
    try:
        shutil.rmtree(global_conf.CWD+'worker_data/job-'+task['job_id'])
        return True
    except Exception, e:
         log.error('Error Deleting Local Task Data', exc_info=True)
         return False

def after_execute(task):
    if not upload_output(task):
        return False
    if not message_completion(task):
        return False
    if not delete_task():
        return False
    if not delete_local_data(task):
        return False
    return True

def execute(task):
    task_dir = global_conf.CWD+'worker_data/job-'+task['job_id']+'/task-'+str(task['task_id'])
    #create output file
    std_out = open(task_dir+'/output', 'w+')
    #open the input file
    std_in = open(task_dir+'/data')
    log.info('Executing Script')
    cmd = 'python -u '+global_conf.CWD+'worker_scripts/'+task['job_id']+'.py'
    #run the script
    process = subprocess.Popen(cmd.split(), stdout=std_out, stdin=std_in)
    process.wait()
    log.info('Script Execution Completed')
    #
    return True

def run_execution(task):
    log.info('Starting Before-Execute Stage')
    if not before_execute(task):
        return False
    log.info('Finished Before-Execute Stage')
    log.info('Starting Execute Stage')
    if not execute(task):
        return False
    log.info('Finished Execute Stage')
    log.info('Starting After-Execute Stage')
    if not after_execute(task):
        return False
    log.info('Finished After-Execute Stage')
