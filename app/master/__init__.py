from master.master_logger import create_master_logger
log = create_master_logger('root_logger')

from master.web_server import WebServer
from master.async_downloader import AsyncDownloader
from master.receiver import Receiver
from master.job_big_operation_controller import JobBigOperationController

class Master:
    """
    Starts all of the components that make the master agent
    """
    def __init__(self):
        log.info('Starting Master Agent')
        self.async_downloader = AsyncDownloader()
        self.async_downloader.setDaemon(True)

        self.job_big_operation_controller = JobBigOperationController(self.async_downloader)
        self.job_big_operation_controller.setDaemon(True)

        self.recevier = Receiver(self.job_big_operation_controller, self.async_downloader)
        self.recevier.setDaemon(True)

        self.web_server = WebServer(self.job_big_operation_controller)

    def start(self):

        self.async_downloader.start()

        self.job_big_operation_controller.start()

        self.recevier.start()

        self.web_server.start()
