import logging, global_conf
log = logging.getLogger('root_logger')

import subprocess, os, shutil, sys
from aws.s3 import s3
from master.db.models import Task

class JobDataPreparer:
    def __init__(self, job):
        self.job = job
        self.input_dir = global_conf.CWD+'job-'+job.id+'/input/'

    def prepare_data(self):
        log.info('Preparing Job Data')
        try:
            self._get_job_data()
            if self.job.data_prep_script:
                self._custom_data_prepare()
            else:
                self._default_prepare()
            self._delete_local_data()
            self._job_executing()
        except Exception, e:
            log.error('Job Failed..... Need to actually implement this', exc_info=True)
        log.info('Finished Preparing Data')

    def _get_job_data(self):
        log.info('Downloading Input Data')
        #download input directory from s3 into a folder input
        r=s3.get_directory(self.job.input_key_path, self.input_dir)
        log.info('Input files downloaded from S3')

    def _default_prepare(self):
        def split_file(file_path):
            """Does a bash command to split a file_path
            using the bash split command as it's probably more efficient
            than anything python can do!
            Only problem is that it makes windows a no go
            """
            log.info('Splitting file %s' % (file_path))
            try:
                #rename file to put _ at the end
                #turns /input/asd.txt -> /input/asd_.txt
                file_name = os.path.splitext(os.path.basename(file_path))
                new_file_path = file_path[:file_path.rfind('/')+1]+file_name[0]+'_'+file_name[1]
                os.rename(file_path, new_file_path)
                #split file with block size using numerical indexes with the file prefix as split prefixes ie. file_name.txt -> file_name0, file_name1...
                cmd = "split %s -b %s -d %s" % (os.path.basename(new_file_path), self.job.task_split_size, os.path.splitext(os.path.basename(new_file_path))[0])
                process = subprocess.Popen(cmd.split(), cwd=self.input_dir, stdout=subprocess.PIPE)
                #wait until the split finishes
                process.wait()
                #delete the original file afterwards
                os.remove(new_file_path)
            except Exception, e:
                log.error('Error Splitting Input:', exc_info=True)
        #-------
        log.info('Performing Default Data Preparation')
        #go through each downloaded file in the directory & split it
        for f in os.listdir(self.input_dir):
            split_file(self.input_dir+f)

        #create a task for each split
        tasks = []
        try:
            for f in os.listdir(self.input_dir):
                t=Task(job_id=self.job.id, file_name=f, task_id=f[f.rfind('_')+1:])
                #-1 means the last element
                self.job.session.add(t)
                self.job.session.commit()
                log.info('Created %s' % (t))
                #give task the session to work with later
                t.set_session(self.job.session)
                tasks.append(t)

            for task in tasks:
                task.submit()

        except Exception, e:
            log.error('Error Creating Task', exc_info=True)

    def _custom_data_prepare(self):
        self._get_prep_script()
        #get a r/w pointer to all the job input files
        input_files = [open(self.input_dir+f, 'r+') for f in os.listdir(self.input_dir)]
        print input_files
        #import data prep script
        #equivalent to -> from job_id import prepare as custom_prepare
        custom_prepare = getattr(__import__(self.job.id, fromlist=['prepare']), 'prepare')
        log.info('Running Custom Data Preparation Script')
        try:
            custom_prepare(input_files, self.job.id, log)
        except Exception, e:
            log.error('Custom Data Preperation Script Failed:', exc_info=True)
        log.info('Custom Data Preparation Script Completed')

    def _get_prep_script(self):
        #create job module folder
        job_module = global_conf.CWD+'app/'+self.job.id+'/'
        if not os.path.isdir(job_module):
            os.makedirs(job_module)
        log.info('Downloading Data Prep Script from S3')
        #download script and create a module from it called the job_id
        s3.get(self.job.data_prep_script, file_path=job_module+'__init__.py')
        log.info('Data Prep Script Downloaded')
        #need to add exception stuff later

    def _job_executing(self):
        #job is submitted so mark as executing
        self.job.status = 'executing tasks'
        self.job.session.commit()

    def _delete_local_data(self):
        #delete input folder after tasks have been created and
        #splits saved to S3
        shutil.rmtree(global_conf.CWD+'job-'+self.job.id)
        #delete the job module for any custom scripts that may have been
        shutil.rmtree(global_conf.CWD+'app/'+self.job.id)
