import logging
log = logging.getLogger('root_logger')

import global_conf, os, shutil, sys
from aws.s3 import s3
from executor import Executor

class JobFinalScriptExecutor(Executor):

    def __init__(self, job, task_output_download_queue):
        Executor.__init__(self)
        self.job = job
        self.task_output_download_queue = task_output_download_queue
        self.job_module = global_conf.CWD+'app/'+self.job.id+'/'
        self.job_dir = global_conf.CWD+'job-'+self.job.id+'/'


    def get_script(self):
        log.info('Getting Exceutable Script')
        #create job module folder
        if not os.path.isdir(self.job_module):
            os.makedirs(self.job_module)
        log.info('Downloading Exceutable Script from S3')
        #download script and create a module from it called the job_id
        if s3.get(self.job.final_script, file_path=self.job_module+'__init__.py'):
            log.info('Exceutable Script Downloaded')
            return True
        log.error('Failed to Download Executable Script')
        return False

    def get_input(self):
        #download all files in the queue - the rest will have been
        #asynchronously downloaded
        while not self.task_output_download_queue.empty():
            #get and download file
            f = self.task_output_download_queue.get()
            if not s3.get(f, file_path=global_conf.CWD+f):
                return False
        return True

    def execute(self):
        log.info('Executing Final Script')
        #open task outputs
        task_output_dir = self.job_dir+'task_output/'
        task_outputs = [open(task_output_dir+f, 'r+') for f in os.listdir(task_output_dir)]
        #import final script
        #equivalent to -> from job_id import run as task_script
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
                log.error('Final Script Failed on Attempt %s' % (i), exc_info=True)
                if i == 3:
                    return False
        #claim back the STDOUT
        sys.stdout = sys.__stdout__
        log.info('Final Script Execution Completed')
        return True

    def upload_output(self):
        log.info('Uploading Final Script Output(s) to S3')
        key = '/job-'+self.job.id+'/output/'
        for f in os.listdir(self.job_dir+'/output'):#SHOULD USE WITH IN PLACES LIKE THIS
            if not s3.put(self.job_dir+'/output/'+f, key+f):
                log.error('Failed to Upload Final Script Output(s)')
                return False
        log.info('Uploaded Final Script Output(s)')
        return True

    def delete_local_data(self):
        log.info('Deleting Task & Final Script Output Data')
        try:
            shutil.rmtree(self.job_dir)
            log.info('Deleted Task & Final Script Output Data')
        except Exception, e:
             log.error('Error Deleting Task & Final Script Output Data', exc_info=True)
             return False
        log.info('Deleting Final Script Module')
        try:
            shutil.rmtree(self.job_module)
            log.info('Deleted Final Script Module')
        except Exception, e:
             log.error('Error Deleting Final Script Module', exc_info=True)
             return False
        return True

    #this function deletes the task input splits and task outputs from S3
    def delete_task_data_s3(self):
        #delete task_script
        job_key = 'job-'+self.job.id+'/'
        a = s3.delete(job_key+self.job.id+'.py')
        #delete split_inputs
        b = s3.delete_directory(job_key+'split_input/')
        #delete task outputs
        c = s3.delete_directory(job_key+'task_output/')
        return a and b and c

    #redefine after_execute to add new functions
    def after_execute(self):
        r = super(JobFinalScriptExecutor, self).after_execute()
        return r and self.delete_task_data_s3()

    def failed_execution(self):
        pass
