import logging
log = logging.getLogger('root_logger')

import time
from aws.sqs import new_tasks_queue

def poll():
	while True:
		rs = new_tasks_queue.get_message()
		if rs:
			return rs
		time.sleep(5)
