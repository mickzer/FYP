import logging
log = logging.getLogger('root_logger')
from abc import ABCMeta, abstractmethod

class Executor:
    """
    An abstract base class to manage the operation of all 'Executors'.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _before_execute(self):
        pass

    @abstractmethod
    def _execute(self):
        pass

    @abstractmethod
    def _after_execute(self):
        pass

    @abstractmethod
    def _failed_execution(self):
        pass

    def _run_stages(self):
        """
        Executes the stages of execution in order.
        """
        log.info('Starting Before-Execute Stage')
        if not self._before_execute():
            return False
        log.info('Finished Before-Execute Stage')
        log.info('Starting Execute Stage')
        if not self._execute():
            return False
        log.info('Finished Execute Stage')
        log.info('Starting After-Execute Stage')
        if not self._after_execute():
            return False
        log.info('Finished After-Execute Stage')
        return True

    def run_execution(self):
        """
        Runs an execution and runs the failure routine if it didn't work.
        """
        if not self._run_stages():
            self._failed_execution()
            return False
        return True
