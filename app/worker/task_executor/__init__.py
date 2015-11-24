import logging
log = logging.getLogger('root_logger')

import subprocess, os, shutil, sys, traceback, global_conf
from aws.s3 import s3
from aws.sqs import workers_messaging_queue, new_tasks_queue
from executor import Executor

class TaskExecutor(Executor):
    def __init__(self, task):
        Executor.__init__(self)
        self.task = task
        self.job_module = global_conf.CWD+'app/'+self.task['job_id']+'/'
        self.job_dir = global_conf.CWD+'worker_data/job-'+self.task['job_id']+'/'
        self.task_dir = self.job_dir+'task-'+str(self.task['task_id'])+'/'

    def get_script(self):
        log.info('Getting Exceutable Script')
        if os.path.exists(self.job_module+'__init__.py'):
            log.info('Exceutable Script was Cached')
            return True
        else:
            if not os.path.isdir(self.job_module):
                os.makedirs(self.job_module)
            log.info('Downloading Exceutable Script from S3')
            #download script and create a module from it called the job_id
            if s3.get('job-'+self.task['job_id']+'/'+self.task['job_id']+'.py', file_path=self.job_module+'__init__.py'):
                log.info('Exceutable Script Downloaded')
                return True
            log.error('Failed to Download Executable Script')
            return False

    def get_input(self):
        log.info('Creating Local Directory Structure')
        #create the directory structure
        #ensure/create worker_data
        if not os.path.isdir(global_conf.CWD+'worker_data/'):
            os.makedirs(global_conf.CWD+'worker_data/')
        #ensure/create task dir
        if not os.path.isdir(self.task_dir):
            os.makedirs(self.task_dir)
        #download input split from S3
        log.info('Downloading Input Split')
        if s3.get('job-'+self.task['job_id']+'/split_input/'+self.task['file_name'], self.task_dir+'data'):
            log.info('Input Split Downloaded')
            return True
        log.error('Failed to get input split from S3')
        return False

    def execute(self):
        log.info('Executing Task Script')
        input_split = open(self.task_dir+'data', 'r+')
        #import task script
        #equivalent to -> from job_id import run as task_script
        task_script = getattr(__import__(self.task['job_id'], fromlist=['run']), 'run')
        #send the STDOUT to the output file for running the function
        f = open(self.task_dir+'output', 'w+')
        #give 3 attempts at running task
        for i in range(0,3):
            sys.stdout = f
            try:
                task_script(input_split, log)
                break
            except Exception, e:
                #claim back the STDOUT
                sys.stdout = sys.__stdout__
                log.error('Task Script Failed on Attempt %s:\n%s' % (i+1, traceback.format_exc()))
                if i == 2:
                    return False
        #claim back the STDOUT
        sys.stdout = sys.__stdout__
        log.info('Script Execution Completed')
        return True

    def upload_output(self):
        log.info('Uploading Task Output to S3')
        key = '/job-'+self.task['job_id']+'/task_output/'+str(self.task['task_id'])
        if s3.put(self.task_dir+'output', key):
            return True
        log.error('Failed to Upload Task Output')
        return False

    def delete_local_data(self):
        log.info('Deleting local task data')
        try:
            shutil.rmtree(self.job_dir)
            return True
        except Exception, e:
             log.error('Error Deleting Local Task Data', exc_info=True)
             return False

    def message_completion(self):
        log.info('Adding task completion to SQS Queue')
        self.task['status'] = 'completed'
        if workers_messaging_queue.add_message(self.task, msg_type='task'):
            return True
        log.error('Failed to add task completion to SQS Queue')
        return False

    def delete_task(self):
        log.info('Deleting task from new tasks SQS Queue')
        if new_tasks_queue.delete_message():
            return True
        log.error('Failed to delete task from new tasks SQS Queue')
        return False

    #redefine after_execute to add new functions
    def after_execute(self):
        r = super(TaskExecutor, self).after_execute()
        return r and self.message_completion() and self.delete_task()

    def failed_execution(self):
        #add failed task message
        log.info('Adding task failure to SQS Queue')
        self.task['status'] = 'failed'
        workers_messaging_queue.add_message(self.task, msg_type='task')
        #delete task
        new_tasks_queue.delete_message()
