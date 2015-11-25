import logging
log = logging.getLogger('root_logger')

from master.master_logger import MasterLoggingHandler

class Master:
    def __init__(self):
        #add master logging handler
        log.addHandler(MasterLoggingHandler())

    def receive(self):
        from master.receiver import Receiver
        r=Receiver()
        r.receive()
