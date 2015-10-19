import logging
log = logging.getLogger('root_logger')

import global_conf

import boto.sqs
from boto.sqs.message import Message
from datetime import datetime
import json

class Sqs:
	def __init__(self, queue_name):
		self.queue_name = queue_name
		log.info('Connecting to SQS (%s)' % (self.queue_name))
		con = boto.sqs.connect_to_region(global_conf.REGION)
		self.queue  = con.get_queue(queue_name)
		#if can't find queue, create it
		if not self.queue:
			log.info('Creating Queue (%s)' % (self.queue_name))
			con.create_queue(queue_name)
		self.queue  = con.get_queue(queue_name)
	def add_message(self, data):
		m = Message()
		d = {
			'data': data,
			'create_time': str(datetime.utcnow())
		}
		d = json.dumps(d)
		log.debug(d)
		m.set_body(d)
		try:
			self.queue.write(m)
		except Exception, e:
			log.error('Failed to add Message to SQS (%s)' % (self.queue_name), exc_info=True)
			return False
		log.info('Added Message to SQS (%s)' % (self.queue_name))
		return True
	def get_message(self):
		log.info('Getting Message from SQS (%s)' % (self.queue_name))
		try:
			return json.loads(self.queue.get_messages(num_messages=1)[0].get_body())
		except Exception, e:
			log.error('Failed to get Message to SQS (%s)' % (self.queue_name), exc_info=True)