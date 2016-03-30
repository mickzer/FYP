import logging, logging.handlers, requests, re, os
from aws.sqs import workers_messaging_queue

class WorkerLoggingAdapter(logging.LoggerAdapter):
    def __init__(self, log):
        super(WorkerLoggingAdapter, self).__init__(log, {})

    def set_job_id(self, job_id):
        self.extra['job_id'] = job_id

    def remove_job_id(self):
        del self.extra['job_id']

    def set_task_id(self, task_id):
        self.extra['task_id'] = task_id

    def remove_task_id(self):
        del self.extra['task_id']

    def process(self, msg, kwargs):
        kwargs['extra'] = self.extra
        if 'job_id' in self.extra and 'task_id' in self.extra:
            return '<Job: %s, Task: %s> %s' % (self.extra['job_id'], self.extra['task_id'], msg), kwargs
        elif 'job_id' in self.extra:
            return '<Job: %s> %s' % (self.extra['job_id'], msg), kwargs
        else:
            return msg, kwargs


class WorkerLoggingHandler(logging.Handler):
    """ A python logging handler which sends all logging
        messages to SQS
    """
    def __init__(self):
        logging.Handler.__init__(self)
        #get the instance id from ec2 meta-data service
        self._job_module_format = re.compile('^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.I)
        self._instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text

    def emit(self, record):
        #dont send SQS log messages to SQS
        relpath = os.path.relpath(record.pathname)
        if relpath == 'aws/sqs/__init__.py':
            return
        data = {
            'msg':record.msg,
            'level':record.levelname,
            'pathname':record.pathname,
            'instance_id': self._instance_id
        }
        #check if the log message is from a task script
        if self._job_module_format.match(relpath.split('/')[0]):
            data['task_message'] = True
        extra = record.__dict__
        if 'job_id' in extra:
            data['job_id'] = extra['job_id']
        if 'task_id' in extra:
            data['task_id'] = extra['task_id']

        workers_messaging_queue.add_message(data, msg_type='log')

def create_worker_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler = logging.StreamHandler()

    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(WorkerLoggingHandler())

    return logger
