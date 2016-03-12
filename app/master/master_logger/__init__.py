import logging, threading, Queue, time, requests
from datetime import datetime
from master.db.models import Session, Log

class AsyncDbPublisher(threading.Thread):
    """Container for threading handlers to make it run
       Asychronously
    """
    def __init__(self):
         threading.Thread.__init__(self)
         self._queue = Queue.Queue()
         self.session = Session()

    def run(self):
        count = 0
        while True:
            data = self._queue.get(True)
            l=Log(**data)
            self.session.add(l)
            count += 1
            #buffer 10 insertions before committing
            if(count == 10):
                self.session.commit()
                count = 0
            time.sleep(0.01)

    def publish(self, data):
        self._queue.put(data)


class MasterLoggingHandler(logging.Handler):
    """ A python logging handler which sends all logging
        messages to RDS
    """
    def __init__(self):
        logging.Handler.__init__(self)
        #asynchronously adds log messages to the DB
        self._async_publisher = AsyncDbPublisher()
        self._async_publisher.setDaemon(True)
        self._async_publisher.start()
        #get the instance id from ec2 meta-data service
        self._instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text

    def emit(self, record, **kwargs):
        data = {
            'msg':record.msg,
            'level':record.levelname,
            'pathname':record.pathname,
            'date': datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S.%f') #unix time to datetime
        }
        data['instance_id'] = self._instance_id
        data['type'] = 'master'
        if 'job_id' in record.__dict__:
            data['job_id'] = record.__dict__['job_id']
        if 'task_id' in record.__dict__:
            data['task_id'] = record.__dict__['task_id']
        self._async_publisher.publish(data)

master_logging_handler = MasterLoggingHandler()

def create_master_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler = logging.StreamHandler()

    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(master_logging_handler)

    return logger
