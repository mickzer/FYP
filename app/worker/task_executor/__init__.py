import logging
from worker.worker_logger import WorkerLoggingAdapter
log = logging.getLogger('root_logger')
log = WorkerLoggingAdapter(log)

import subprocess, os, shutil, sys, traceback, global_conf
from aws.s3 import s3
from aws.sqs import workers_messaging_queue, new_tasks_queue
from executor import Executor

class TaskExecutor(Executor):
    """
    This class executes a user's task execution script.
    """
    def __init__(self, task_msg):
        Executor.__init__(self)
        self.task_msg = task_msg
        self.task = task_msg.get_data()['data']
        self.job_module = global_conf.CWD+'app/'+self.task['job_id']+'/'
        self.job_dir = global_conf.CWD+'worker_data/job-'+self.task['job_id']+'/'
        self.task_dir = self.job_dir+'task-'+str(self.task['task_id'])+'/'
        #put job and task in log
        log.set_job_id(self.task['job_id'])
        log.set_task_id(self.task['task_id'])

    def _before_execute(self):
        return self._get_script() and self._get_input()

    def _get_script(self):
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
                #ensure the script is accessible
                if not os.path.exists(self.job_module+'__init__.py'):
                    log.error('Executable Script is not incorrect location. \
                    Check that working directory is correctly configured')
                    return False
                return True
            log.error('Failed to Download Executable Script')
            return False

    def _get_input(self):
        log.info('Creating Local Directory Structure')
        #create the directory structure
        #ensure/create worker_data
        if not os.path.isdir(global_conf.CWD+'worker_data/'):
            os.makedirs(global_conf.CWD+'worker_data/')
        #ensure/create task dir
        if not os.path.isdir(self.task_dir):
            os.makedirs(self.task_dir)
        #now check if we need to get any input
        if not self.task['file_name']:
            log.info('No data specified for this task')
            return True
        #download input split from S3
        log.info('Downloading Input Split')
        if s3.get('job-'+self.task['job_id']+'/split_input/'+self.task['file_name'], self.task_dir+'data'):
            log.info('Input Split Downloaded')
            return True
        log.error('Failed to get input split from S3')
        return False

    def _execute(self):
        log.info('Executing Task Script')
        if self.task['file_name']:
            input_split = open(self.task_dir+'data', 'r+')
        else:
            input_split = None
        #send the STDOUT to the output file for running the function
        f = open(self.task_dir+'output', 'w+')
        sys.stdout = f
        #import task script
        task_script = None
        try:
            #equivalent to -> from job_id import run as task_script
            task_script = getattr(__import__(self.task['job_id'], fromlist=['run']), 'run')
        except Exception, e:
            #claim back the STDOUT
            sys.stdout = sys.__stdout__
            log.error('Task Failed on Import of Tasks Script: %s' % (traceback.format_exc()))
            f.close()
            return False

        #give 3 attempts at running task
        for i in range(0,3):
            try:
                task_script(input_split, self.task['attributes'], log)
                break
            except Exception, e:
                #claim back the STDOUT
                sys.stdout = sys.__stdout__
                log.error('Task Script Failed on Attempt %s:\n%s' % (i+1, traceback.format_exc()))
                if i == 2:
                    f.close()
                    return False
        #close the output files
        f.close()
        if input_split:
            input_split.close()
        #claim back the STDOUT
        sys.stdout = sys.__stdout__
        log.info('Script Execution Completed')
        return True

    def _after_execute(self):
        r = self._upload_output() and self._delete_local_data()
        return r and self._message_completion() and self._delete_task()

    def _upload_output(self):
        if os.stat(self.task_dir+'output').st_size > 0:
            log.info('Uploading Task Output to S3')
            key = '/job-'+self.task['job_id']+'/task_output/'+str(self.task['task_id'])
            if s3.put(self.task_dir+'output', key):
                self.task['output_data'] = True
                return True
            log.error('Failed to Upload Task Output')
            return False
        log.info('No Task Output Exists')
        self.task['output_data'] = False
        return True

    def _delete_local_data(self):
        log.info('Deleting local task data')
        try:
            shutil.rmtree(self.job_dir)
            return True
        except Exception, e:
             log.error('Error Deleting Local Task Data', exc_info=True)
             return False

    def _message_completion(self):
        log.info('Adding task completion to SQS Queue')
        self.task['status'] = 'completed'
        if workers_messaging_queue.add_message(self.task, msg_type='task'):
            return True
        log.error('Failed to add task completion to SQS Queue')
        return False

    def _delete_task(self):
        log.info('Deleting task from new tasks SQS Queue')
        if self.task_msg.delete():
            return True
        log.error('Failed to delete task from new tasks SQS Queue')
        return False

    def _failed_execution(self):
        #add failed task message
        log.info('Adding task failure to SQS Queue')
        self.task['status'] = 'failed'
        workers_messaging_queue.add_message(self.task, msg_type='task')
        self._delete_task()
        self._delete_local_data()
