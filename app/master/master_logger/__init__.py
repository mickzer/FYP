import logging, threading, Queue, time, requests
from datetime import datetime

class MasterLoggingAdapter(logging.LoggerAdapter):
    """
    Provides an interface to attach job ids and task ids to log messages.
    """
    def __init__(self, log):
        super(MasterLoggingAdapter, self).__init__(log, {})

    def set_job_id(self, job_id):
        self.extra['job_id'] = job_id

    def remove_job_id(self):
        try:
            del self.extra['job_id']
        except:
            pass

    def set_task_id(self, task_id):
        self.extra['task_id'] = task_id

    def remove_task_id(self):
        try:
            del self.extra['task_id']
        except:
            pass

    def process(self, msg, kwargs):
        kwargs['extra'] = self.extra
        if 'job_id' in self.extra and 'task_id' in self.extra:
            return '<Job: %s, Task: %s> %s' % (self.extra['job_id'], self.extra['task_id'], msg), kwargs
        elif 'job_id' in self.extra:
            return '<Job: %s> %s' % (self.extra['job_id'], msg), kwargs
        else:
            return msg, kwargs


from master.db.models import Session, Log

class AsyncDbPublisher(threading.Thread):
    """
    Publishes log messages to the DB asynchronously in batches of 10.
    """
    def __init__(self):
         threading.Thread.__init__(self)
         self._queue = Queue.Queue()
         self.session = Session()
         self.buffer = []

    def run(self):
        count = 0
        while True:
            data = self._queue.get(True)
            l=Log(**data)
            self.buffer.append(l)
            count += 1
            #buffer 10 insertions before committing
            if(count == 10):
                self.session = Session()
                for msg in self.buffer:
                    self.session.add(msg)
                self.session.commit()
                count = 0
                self.buffer = []
            time.sleep(0.01)

    def publish(self, data):
        self._queue.put(data)


class MasterLoggingHandler(logging.Handler):
    """ A python logging handler which sends all logging
        messages to  the database
    """
    def __init__(self):
        logging.Handler.__init__(self)
        #asynchronously adds log messages to the DB
        self._async_publisher = AsyncDbPublisher()
        self._async_publisher.setDaemon(True)
        self._async_publisher.start()
        #get the instance id from the ec2 meta-data service
        self._instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text

    def emit(self, record, **kwargs):
        extra = record.__dict__
        if 'db_ignore' in extra and extra['db_ignore']:
            return
        data = {
            'msg':record.msg,
            'level':record.levelname,
            'pathname':record.pathname,
            'instance_id': self._instance_id,
            'type': 'master',
            'date': datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S.%f') #unix time to datetime
        }
        if 'job_id' in extra:
            data['job_id'] = extra['job_id']
        if 'task_id' in extra:
            data['task_id'] = extra['task_id']
        self._async_publisher.publish(data)

def create_master_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler = logging.StreamHandler()

    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(MasterLoggingHandler())

    return logger
