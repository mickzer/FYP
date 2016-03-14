import logging
log = logging.getLogger('root_logger')

import threading, time
from Queue import Queue
from master.db.models import Session
from master.job_data_preparer import JobDataPreparer

class JobBigOperationController(threading.Thread):
    def __init__(self, async_downloader):
        threading.Thread.__init__(self)
        self.queue = Queue()
        #should prob throw exception is this is null
        #or not running
        self.async_downloader = async_downloader

    def add(self, job):
        log.info('Queuing Big Job Operation  for %s' % (job))
        self.queue.put(job)
        return True

    #NEED TO PAUSE ON JOB CREATION TOO
    def run(self):
        log.info('JobBigOperationController Started')
        while True:
            job = self.queue.get()
            log.info('Running Big Operation for %s' % (job))
            session = Session()
            if hasattr(job, 'output_log') and job.output_log:
                job.set_session(session)
                job.output_log_to_s3()
            #Job in created state needs to be submitted
            elif job.status == 'created':
                job.set_session(session)
                job.submit()
                data_preparer = JobDataPreparer(job)
                data_preparer.prepare_data()

            #Job in tasks completed needs to have it's final script executed
            elif job.status == 'tasks completed':
                job.set_session(session)
                #pause async_downloader
                job.downloaded_task_outputs = self.async_downloader.pause(job.id)
                job.execute_final_script()
                self.async_downloader.resume()
            else:
                #raise some exception
                log.error('%s shouldn\'t be here' % (job))
            session.close()
            time.sleep(0.01)
