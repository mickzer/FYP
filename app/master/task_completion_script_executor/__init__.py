import logging
from master.master_logger import MasterLoggingAdapter
log = logging.getLogger('root_logger')
log = MasterLoggingAdapter(log)

import global_conf, os
from aws.s3 import s3
from executor import Executor
from master.db.models import Session, Job

class TaskCompletionScriptExecutor(Executor):
    """
    This class executes a user's supplied task execution scirpt.
    """
    def __init__(self, task, job):
        self._task = task
        self.script_module = 'job-'+self._task.job_id+'-tcs'
        self._job = job
        log.set_job_id(self._job.id)
        log.set_task_id(self._task.id)

    def _before_execute(self):
        return self._get_script()

    def _get_script(self):
        #create job module folder
        if not os.path.isdir(self.script_module):
            os.makedirs(self.script_module)
        if not os.path.exists(self.script_module+'/__init__.py'):
            path = global_conf.CWD+'app/'+self.script_module+'/__init__.py'
            if s3.get(self._task.job.task_completion_script, file_path=path):
                log.info('Task Completion Script Script Downloaded')
                return True
            return False
        else:
            log.info('Task Completion Script Cached')
            return True

    def _execute(self):
        log.info('Task Completion Script Executing')
        #import task completion script
        #equivalent to -> from job_id-tcs import run as task_completion_script
        task_completion_script = getattr(__import__(self.script_module, fromlist=['run']), 'run')
        try:
            self._job.task_completion_context = task_completion_script(self._task, self._job.task_completion_context, log)
            log.info('Task Completion Script Execution Finished')
            #change gets committed to db by receiver
            return True
        except Exception, e:
            log.error('Task Completion Script Failed', exc_info=True)
            return False

    def _after_execute(self):
        return True

    def _failed_execution(self):
        #nothing else to do on failure
        pass
