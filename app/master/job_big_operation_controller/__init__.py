import logging
log = logging.getLogger('root_logger')

import threading, time
from Queue import Queue
from master.db.models import Session
from master.job_data_preparer import JobDataPreparer

class JobBigOperationController(threading.Thread):
    """
    Only allows the operation of a single 'big operation' at any one time.
    """
    def __init__(self, async_downloader):
        threading.Thread.__init__(self)
        #queue of jobs to perform operations on
        self.queue = Queue()
        self.async_downloader = async_downloader
        self.session = Session()

    def add(self, job):
        log.info('Queuing Big Job Operation  for %s' % (job))
        self.queue.put(job)
        return True
    def run(self):
        log.info('JobBigOperationController Started')
        while True:
            job = self.queue.get()
            log.info('Running Big Operation for %s' % (job))
            if hasattr(job, 'output_log') and job.output_log:
                job.set_session(self.session)
                job.output_log_to_s3()
            #Job in created state needs to be submitted
            elif job.status == 'created':
                job.set_session(self.session)
                job.submit()
                #TRY
                # job.session.rollback()
                data_preparer = JobDataPreparer(job)
                self.async_downloader.pause()
                data_preparer.prepare_data()
                self.async_downloader.resume()

            #Job in tasks completed needs to have it's final script executed
            elif job.status == 'tasks completed':
                job.set_session(self.session)
                #pause async_downloader
                job.downloaded_task_outputs = self.async_downloader.pause(job.id)
                print 'DOWNLOADED: '+str(job.downloaded_task_outputs)
                job.execute_final_script()
                self.async_downloader.resume()
            else:
                #raise some exception
                log.error('%s shouldn\'t be here' % (job))
            time.sleep(0.01)
