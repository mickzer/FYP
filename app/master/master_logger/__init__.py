import logging, threading, Queue, time, requests
from datetime import datetime
from master.db.models import session, Log

class AsyncDbPublisher(threading.Thread):
    """Container for threading handlers to make it run
       Asychronously
    """
    def __init__(self):
         threading.Thread.__init__(self)
         self._queue = Queue.Queue()

    def run(self):
        while True:
           data = self._queue.get(True)
           l=Log(**data)
           session.add(l)
           session.commit()
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
        self.async_publisher = AsyncDbPublisher()
        self.async_publisher.setDaemon(True)
        self.async_publisher.start()
        #get the instance id from ec2 meta-data service
        self.instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text

    def emit(self, record):
        data = {
            'msg':record.msg,
            'level':record.levelname,
            'pathname':record.pathname,
            'date': datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S.%f') #unix time to datetime
        }
        data['instance_id'] = self.instance_id
        data['type'] = 'master'
        self.async_publisher.publish(data)
