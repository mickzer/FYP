import logging
log = logging.getLogger('root_logger')

import global_conf

import boto.sqs
from boto.sqs.message import Message
from datetime import datetime
import json, time

class Sqs:
	def __init__(self, queue_name):
		self.queue_name = queue_name
		self.current_message = None
		log.info('Connecting to SQS (%s)' % (self.queue_name))
		try:
			self.con = boto.sqs.connect_to_region(global_conf.REGION)
			self.queue  = self.con.get_queue(queue_name)

			#if can't find queue, create it
			if not self.queue:
				log.info('Creating Queue (%s)' % (self.queue_name))
				self.con.create_queue(queue_name)
			self.queue  = self.con.get_queue(queue_name)
		except Exception, e:
			log.error('SQS Error', exc_info=True)
			return
	def add_message(self, data, msg_type=None):
		"""This function adds a message to the Sqs object's queue.

	    Args:
	       data (dict):  dict of the message

	    Returns:
	       bool.  The return code::

	          True -- Success!
	          False -- Failure
	    """
		m = Message()
		d = {
			'data': data,
			'create_time': str(datetime.utcnow())
		}
		if msg_type:
			d['type'] = msg_type
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
		"""This function gets a single message from the Sqs object's queue.
		   It also stores the message in the Sqs object's member variable - current_message

	    Returns:
	       dict or False.  The return code::

	          dict -- Message
	          False -- No Message/Failure
	    """
		log.info('Getting Message from SQS (%s)' % (self.queue_name))
		try:
			msg = self.queue.get_messages(num_messages=1)
			if msg:
				self.current_message = msg[0]
				return json.loads(self.current_message.get_body())
			return False
		except Exception, e:
			log.error('Failed to get Message to SQS (%s)' % (self.queue_name), exc_info=True)
			return False
	def retain_message(self):
		"""This function keeps a message hidden for 30 more seconds

	    Returns:
	       bool.  The return code::

	          True -- Success
	          False -- Failure
	    """
		if self.current_message:
			try:
				self.con.change_message_visibility(self.queue, self.current_message.receipt_handle, 30)
				return True
			except Exception, e:
				log.error('Failed to retain message', exc_info=True)
				return False
		return False
	def delete_message(self):
		"""This function deletes a message from the queue

	    Returns:
	       bool.  The return code::

	          True -- Success
	          False -- Failure
	    """
		if self.current_message:
			try:
				self.queue.delete_message(self.current_message)
				self.current_message = None
				return True
			except Exception, e:
				log.error('Failed to delete message', exc_info=True)
				return False
		return False
	def poll(self):
		"""This function polls a queue

	    Returns:
	       bool.  The return code::

	          dict - message
	    """
		while True:
			rs = self.get_message()
			if rs:
				return rs
			time.sleep(5)

new_tasks_queue = Sqs(global_conf.NEW_TASKS_QUEUE)
workers_messaging_queue = Sqs(global_conf.WORKERS_MESSAGING_QUEUE)
