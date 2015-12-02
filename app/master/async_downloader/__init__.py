import logging
log = logging.getLogger('root_logger')
import threading, time, random, global_conf
from Queue import Queue
from aws.s3 import s3

#NOTE: TAKE ANOTHER LOOK AT RUN - Are the QUEUEs being delete when they shouldnt?

class AsyncDownloader(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.queued_files = {}
        self.downloaded_files = {}
        #paused flag
        self.paused = False
        #paused locking condition
        self.pause_cond = threading.Condition(threading.Lock())

    def add(self, job_id, task_id):
        if job_id not in self.queued_files:
            self.queued_files[job_id] = Queue()
        if job_id not in self.downloaded_files:
            self.downloaded_files[job_id] = list()
        self.queued_files[job_id].put(task_id)
        log.info('Queued for Async Download: ' + str(task_id))

    def pause(self, job_id):
        self.paused = True
        #acquire the lock which will make the calling thread
        #wait until the current download finishes
        self.pause_cond.acquire()
        #return the list of downloaded files for this job
        #and delete all records from here
        if job_id in self.queued_files:
            del self.queued_files[job_id]
            r = self.downloaded_files[job_id]
            del self.downloaded_files[job_id]
            return r
        return None

    def resume(self):
        self.paused = False
        # Notify so thread will wake after lock released
        self.pause_cond.notify()
        # Now release the lock
        self.pause_cond.release()

    def run(self):
        log.info('AsyncDownloader Running')
        while True:
            with self.pause_cond:
                while self.paused:
                    self.pause_cond.wait()
                #get all the queues that aren't empty
                #think this code could be redundant as I delete empty queues later
                qs = []
                if self.queued_files:
                    for key in self.queued_files:
                        if not self.queued_files[key].empty():
                            qs.append({key: self.queued_files[key]})
                if qs:
                    #pick a random queue
                    q = random.choice(qs)
                    #download the next file in the queue
                    job_id = q.keys()[0]
                    task_id = q[job_id].get()
                    path = 'job-'+job_id+'/task_output/' + str(task_id)
                    log.info('Async Downloading: ' + path)
                    #download from s3
                    s3.get(path, file_path=global_conf.CWD+path)
                    #add downloaded file to downloaded_files
                    self.downloaded_files[job_id].append(task_id)
                    #delete the key from self.queued_files if queue is empty
                    if q[job_id].empty():
                        del self.queued_files[job_id]
                time.sleep(0.01)
