import logging, global_conf
from master.master_logger import MasterLoggingAdapter
log = logging.getLogger('root_logger')
log = MasterLoggingAdapter(log)

import subprocess, os, shutil, sys
from aws.s3 import s3
from master.db.models import Task
from filechunkio import FileChunkIO
import math

from executor import Executor

class DataPreparationScriptExecutor(Executor):
    """
    Executes a user's supplied data exeuction script.
    """
    def __init__(self, job):
        Executor.__init__(self)
        self.job = job
        self.module_name = 'job-'+job.id+'-dps'
        self.module_path = global_conf.CWD+'app/'+self.module_name
        self.input_dir = global_conf.CWD+'job-'+job.id+'/input/'
        #add job_id to log messages
        log.set_job_id(job.id)

    def _before_execute(self):
        return self._get_script()

    def _get_script(self):
        #create job module folder
        if not os.path.isdir(self.module_path):
            os.makedirs(self.module_path)
        log.info('Downloading Data Prep Script from S3')
        #download script and create a module from it called the job_id
        if s3.get(self.job.data_prep_script, file_path=self.module_name+'/__init__.py'):
            log.info('Data Prep Script Downloaded')
            return True
        return False

    def _execute(self):
        if self.job.input_key_path:
            #get a r/w pointer to all the job input files
            input_files = [open(self.input_dir+f, 'r+') for f in os.listdir(self.input_dir)]
        else:
            input_files = None
        #import data prep script
        #equivalent to -> from job_id import prepare as custom_prepare
        custom_prepare = getattr(__import__(self.module_name, fromlist=['prepare']), 'prepare')
        try:
            custom_prepare(input_files, self.job.id, log)
        except Exception, e:
            log.error('Custom Data Preperation Script Failed:', exc_info=True)
            return False
        return True

    def _after_execute(self):
        return self._delete_local_data()

    def _delete_local_data(self):
        try:
            shutil.rmtree(self.module_path)
        except:
            pass
        return True

    def _failed_execution(self):
        log.info('Data Preparation Script failed for %s' % (self.job))
        self.job.mark_as_failed()

class JobDataPreparer:
    """
    Handles the data preparation stage of a job
    """
    def __init__(self, job):
        self.job = job
        self.input_dir = global_conf.CWD+'job-'+job.id+'/input/'
        self.split_dir = global_conf.CWD+'job-'+job.id+'/split/'
        if not os.path.isdir(self.split_dir):
            os.makedirs(self.split_dir)
        #add job_id to log messages
        log.set_job_id(job.id)

    def prepare_data(self):
        log.info('Preparing Job Data')
        try:
            self._get_job_data()
            if self.job.data_prep_script:
                self._custom_prepare()
            else:
                self._default_prepare()
            self._delete_local_data()
            self.job.mark_as_tasks_executing()
        except Exception, e:
            log.error('Job Failed.', exc_info=True)
            self._failed_preparation()
        log.info('Finished Preparing Data')

    def _failed_preparation(self):
        log.info('Job Data Preparation failed for %s' % (self.job))
        self.job.mark_as_failed()

    def _get_job_data(self):
        if self.job.input_key_path:
            log.info('Downloading Input Data')
            #download input directory from s3 into a folder input
            r=s3.get_directory(self.job.input_key_path, self.input_dir)
            log.info('Input files downloaded from S3')
        else:
            log.info('No Input Data was supplied with this job')
            if not self.job.data_prep_script:
                msg = 'No Input Data & No Data Preparation Script was supplied'
                log.info(msg)
                raise Exception(msg)

    def _default_prepare(self):
        """
        Splits the input file into chunks of the job's task split size and
        creates and submits a task for each of them.
        """
        def split_file(file_path, chunk_size, task_id):
            file_size = os.stat(file_path).st_size
            chunk_count = int(math.ceil(file_size / float(chunk_size)))
            #Send the file parts, using FileChunkIO to create a file-like object
            # that points to a certain byte range within the original file. We
            # set bytes to never exceed the original file size.
            for i in range(chunk_count):
                offset = chunk_size * i
                bytes = min(chunk_size, file_size - offset)
                with FileChunkIO(file_path, 'r', offset=offset, bytes=bytes) as fp:
                    f = open(self.split_dir+str(task_id), 'w')
                    f.write(fp.readall())
                    f.close()
                    task_id += 1
            return task_id
        #-------
        log.info('Performing Default Data Preparation')
        #go through each downloaded file in the directory & split it
        task_id = 1
        for f in os.listdir(self.input_dir):
            log.info('Splitting File %s' % (f))
            task_id = split_file(self.input_dir+f, self.job.task_split_size, task_id)

        #create a task for each split
        tasks = []
        try:
            for f in os.listdir(self.split_dir):
                t=Task(job_id=self.job.id, file_name=f, task_id=f)
                #-1 means the last element
                self.job.session.add(t)
                self.job.session.commit()
                log.set_task_id(t.id)
                log.info('Created %s' % (t))
                #give task the session to work with later
                t.set_session(self.job.session)
                tasks.append(t)
                log.remove_task_id()

            for task in tasks:
                task.submit()

        except Exception, e:
            log.error('Error Creating Task', exc_info=True)

    def _custom_prepare(self):
        exc = DataPreparationScriptExecutor(self.job)
        exc.run_execution()

    def _delete_local_data(self):
        try:
            #delete input folder after tasks have been created and
            #splits saved to S3
            shutil.rmtree(global_conf.CWD+'job-'+self.job.id)
        except:
            pass #deleteion failures are not important
