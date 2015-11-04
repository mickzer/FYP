import logging
log = logging.getLogger('root_logger')
from abc import ABCMeta, abstractmethod

class Executor:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_script(self):
        pass

    @abstractmethod
    def get_input(self):
        pass

    def before_execute(self):
        return self.get_script() and self.get_input()

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def upload_output(self):
        pass

    @abstractmethod
    def delete_local_data(self):
        pass

    def after_execute(self):
        return self.upload_output() and self.delete_local_data()

    def run_execution(self):
        log.info('Starting Before-Execute Stage')
        if not self.before_execute():
            return False
        log.info('Finished Before-Execute Stage')
        log.info('Starting Execute Stage')
        if not self.execute():
            return False
        log.info('Finished Execute Stage')
        log.info('Starting After-Execute Stage')
        if not self.after_execute():
            return False
        log.info('Finished After-Execute Stage')
