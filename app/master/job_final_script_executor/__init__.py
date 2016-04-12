import logging
from master.master_logger import MasterLoggingAdapter
log = logging.getLogger('root_logger')
log = MasterLoggingAdapter(log)

import global_conf, os, shutil, sys
from aws.s3 import s3
from executor import Executor
from sqlalchemy import and_, not_

class JobFinalScriptExecutor(Executor):
    """
    Executes a user's final exeuction script for a job.
    """
    def __init__(self, job):
        Executor.__init__(self)
        self.job = job
        self.job_module = global_conf.CWD+'app/'+self.job.id+'/'
        self.job_dir = global_conf.CWD+'job-'+self.job.id+'/'
        log.set_job_id(self.job.id)

    def _before_execute(self):
        return self._get_script() and self._get_input_data()

    def _get_script(self):
        #create job module folder
        if not os.path.isdir(self.job_module):
            os.makedirs(self.job_module)
        log.info('Downloading Final Script from S3')
        #download script and create a module from it called the job_id
        if s3.get(self.job.final_script, file_path=self.job_module+'__init__.py'):
            log.info('Final Script Downloaded')
            return True
        log.error('Failed to Download Final Script')
        return False

    def _get_input_data(self):
        #download all files not in the list of downloaded failed_tasks_count
        if not hasattr(self.job, 'downloaded_task_outputs') or not self.job.downloaded_task_outputs:
            self.job.downloaded_task_outputs = list()
        print str(self.job.downloaded_task_outputs)
        #get tasks whos outputs haven't been downloaded yet
        #temporarily sticking this import here to avoid circular import
        from master.db.models import Task, Job
        tasks = self.job.session.query(Task).filter(and_(Task.job_id == self.job.id, Task.output_data, not_(Task.task_id.in_(self.job.downloaded_task_outputs)))).all()
        for task in tasks:
            path = 'job-'+self.job.id+'/task_output/' + str(task.task_id)
            #download file
            if not s3.get(path, file_path=global_conf.CWD+path):
                return False
        return True

    def _execute(self):
        log.info('Executing Final Script')
        #open task outputs
        task_output_dir = self.job_dir+'task_output/'
        task_outputs = [open(task_output_dir+f, 'r+') for f in os.listdir(task_output_dir)]
        #import final script
        #equivalent to -> from job_id import run as final_script
        final_script = getattr(__import__(self.job.id, fromlist=['run']), 'run')
        #create folder for the ouput file(s)
        if not os.path.isdir(self.job_dir+'output/'):
            os.makedirs(self.job_dir+'output/')
        #send the STDOUT to the output file for running the function
        f = open(self.job_dir+'output/final_script_output', 'w+')
        #give 3 attempts at running task
        for i in range(0,3):
            sys.stdout = f
            try:
                final_script(task_outputs, log)
                break
            except Exception, e:
                #claim back the STDOUT
                sys.stdout = sys.__stdout__
                log.error('Final Script Failed on Attempt %s:%s'  % (i, traceback.format_exc()), exc_info=True)
                if i == 3:
                    return False
        #claim back the STDOUT
        sys.stdout = sys.__stdout__
        log.info('Final Script Execution Completed')
        return True

    def _after_execute(self):
        return self._upload_output() and self._delete_local_data()

    def _upload_output(self):
        log.info('Uploading Final Script Output(s) to S3')
        key = '/job-'+self.job.id+'/output/'
        for f in os.listdir(self.job_dir+'/output'):#SHOULD USE WITH IN PLACES LIKE THIS
            if not s3.put(self.job_dir+'/output/'+f, key+f):
                log.error('Failed to Upload Final Script Output(s)')
                return False
        log.info('Uploaded Final Script Output(s)')
        return True

    def _delete_local_data(self):
        log.info('Deleting Task & Final Script Output Data')
        try:
            shutil.rmtree(self.job_dir)
            shutil.rmtree(self.job_module)
            log.info('Deleted Final Script Module')
        except Exception, e:
             log.error('Error Deleting Local Data', exc_info=True)
             self.session.commit()
        return True

    #this function deletes the task input splits and task outputs from S3
    def _delete_task_data_s3(self):
        #delete task_script
        job_key = 'job-'+self.job.id+'/'
        a = s3.delete(job_key+self.job.id+'.py')
        #delete split_inputs
        b = s3.delete_directory(job_key+'split_input/')
        #delete task outputs
        c = s3.delete_directory(job_key+'task_output/')
        return a and b and c

    def _failed_execution(self):
        log.info('Final Script Execution failed for %s' % (self.job))
        self._delete_local_data()
        self.job.mark_as_failed()
