import logging, logging.handlers, requests, re, os
from aws.sqs import workers_messaging_queue

class WorkerLoggingHandler(logging.Handler):
    """ A python logging handler which sends all logging
        messages to SQS
    """
    def __init__(self):
        logging.Handler.__init__(self)
        #get the instance id from ec2 meta-data service
        self.job_module = re.compile('^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.I)
        self.instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text
        self.job_id = None
        self.task_id = None

    def emit(self, record):
        #dont send SQS log messages to SQS
        #think this code is faulty
        relpath = os.path.relpath(record.pathname)
        if relpath == 'aws/sqs/__init__.py':
            return
        data = {'msg':record.msg, 'level':record.levelname, 'pathname':record.pathname}
        #check if the log message is from a task script
        if self.job_module.match(relpath.split('/')[0]):
            data['task_message'] = True
        #add task and jobs to message if set
        if self.job_id:
            data['job_id'] = self.job_id
            data['task_id'] = self.task_id

        data['instance_id'] = self.instance_id
        workers_messaging_queue.add_message(data, msg_type='log')
