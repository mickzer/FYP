import logging
log = logging.getLogger('root_logger')

import threading, time
from Queue import Queue
from master.db.models import Session

class JobBigOperationController(threading.Thread):
    def __init__(self, async_downloader):
        threading.Thread.__init__(self)
        self.queue = Queue()
        #should prob throw exception is this is null
        #or not running
        self.async_downloader =  async_downloader

    def add(self, job):
        self.queue.put(job)
        log.info('Big Job Operation Queued for %s' % (job))
        return True

    def run(self):
        log.info('JobBigOperationController Started')
        while True:
            job = self.queue.get()
            log.info('Running Big Operation for %s' % (job))
            session = Session()
            #Job in created state needs to be submitted
            if job.status == 'created':
                job.set_session(session)
                job.submit()
            #Job in tasks completed needs to have it's final script executed
            elif job.status == 'tasks completed':
                job.set_session(session)
                #pause async_downloader
                job.downloaded_task_outputs = self.async_downloader.pause(job.id)
                job.finish()
            else:
                #raise some exception
                log.error('%s shouldn\'t be here' % (job))
            self.async_downloader.resume()
            time.sleep(0.01)
