import logging
log = logging.getLogger('root_logger')

import global_conf

import boto.sqs
from boto.sqs.message import Message
from datetime import datetime
import json, time, threading

class Sqs:
    def __init__(self, queue_name):
        self._lock = threading.Lock()
        self._queue_name = queue_name
        log.info('Connecting to SQS (%s)' % (self._queue_name))
        try:
            self._con = boto.sqs.connect_to_region(global_conf.REGION)
            self._queue  = self._con.get_queue(queue_name)

            #if can't find queue, create it
            if not self._queue:
                log.info('Creating Queue (%s)' % (self._queue_name))
                self._con.create_queue(queue_name)
            self._queue  = self._con.get_queue(queue_name)
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
        # log.debug(d)
        m.set_body(d)
        try:
            self._queue.write(m)
        except Exception, e:
            log.error('Failed to add Message to SQS (%s)' % (self._queue_name), exc_info=True)
            return False
        log.info('Added Message to SQS (%s)' % (self._queue_name))
        return True
    def get_message(self, auto_retain=True, as_dict=True):
        """This function gets a single message from the Sqs object's queue.
           It also stores the message in the Sqs object's member variable - current_message

        Returns:
           dict or False.  The return code::

              dict -- Message
              False -- No Message/Failure
        """
        log.debug('Getting Message from SQS (%s)' % (self._queue_name))
        try:
            msg = self._queue.get_messages(num_messages=1)
            if msg:
                if auto_retain:
                    #create + start asynchronous message retainer
                    retainer = AutoRetainingMessage(msg[0], json.loads(msg[0].get_body()))
                    retainer.setDaemon(True)
                    retainer.start()
                    return retainer
                elif as_dict:
                    return json.loads(msg[0].get_body())
                else:
                    return msg[0]
            return False
        except Exception, e:
            log.error('Failed to get Message to SQS (%s)' % (self._queue_name), exc_info=True)
            return False
    def get_message_batch(self, auto_retain=True, as_dict=True):
        """This function gets and returns at most 10 messages from the Sqs object's queue.

        Returns:
           list or False.  The return code::
              list -- Messages
              False -- No Message/Failure
        """
        log.debug('Getting Message Batch from SQS (%s)' % (self._queue_name))
        try:
            msg_batch = self._queue.get_messages(num_messages=10)
            if msg_batch:
                if auto_retain:
                    #create + start asynchronous message retainer
                    retainer = AutoRetainingMessageBatch(msg_batch, [json.loads(msg.get_body()) for msg in msg_batch])
                    retainer.setDaemon(True)
                    retainer.start()
                    return retainer
                elif as_dict:
                    return [json.loads(msg.get_body()) for msg in msg_batch]
                else:
                    return msg_batch
            return False
        except Exception, e:
            log.error('Failed to get Message Batch to SQS (%s)' % (self._queue_name), exc_info=True)
            return False
    def retain_message_batch(self, message_batch):
        """
        Retains the message_batch parameter for 30 more seconds
        """
        if message_batch:
            #api call expects input as a list of tuples where tuple[0] is the
            #message and tuple[1] is the new visibility timeout
            msg_tuples = []
            for message in message_batch:
                msg_tuples.append((message, 30))
            try:
                log.info('Retaining Message batch')
                self._queue.change_message_visibility_batch(msg_tuples)
                return True
            except Exception, e:
                log.error('Failed to retain message batch', exc_info=True)
                return False
        return False
    def delete_message_batch(self, message_batch):
        """
        Deletes a message batch from the queue
        """
        if message_batch:
            try:
                log.info('Deleting message batch')
                self._queue.delete_message_batch(message_batch)
                return True
            except Exception, e:
                log.error('Failed to delete message batch', exc_info=True)
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
    def poll_batch(self):
        """This function polls a queue for a message batch

        Returns:
           bool.  The return code::

              dict - message
        """
        while True:
            rs = self.get_message_batch()
            if rs:
                return rs
            time.sleep(5)
    def delete_all_messages(self):
        """
        Purges a queue
        """
        while True:
            m = self.get_message()
            if not m:
                break
            self.delete_message()

class AutoRetainingMessageBatch(threading.Thread):
    """
    This class keeps an SQS message batch hidden from the queue by issuing an api
    call to SQS every 30 seconds. It stops retaining when the message batch
    is deleted
    """
    def __init__(self, message_batch, message_batch_data):
        threading.Thread.__init__(self)
        self._message_lock = threading.Lock()
        self._message_batch = message_batch
        self._message_batch_data = message_batch_data
        self._stop = False
    def get_data(self):
        return self._message_batch_data
    def delete(self):
        try:
            self._message_lock.acquire()
            workers_messaging_queue.delete_message_batch(self._message_batch)
            self._stop = True
            self._message_lock.release()
            return True
        except:
            self.retain = False
            return False
    def run(self):
        #loop will break when the message been deleted
        while True:
            try:
                time.sleep(25)
                self._message_lock.acquire()
                if self._stop:
                    break
                workers_messaging_queue.retain_message_batch(self._message_batch)
                self._message_lock.release()
            except:
                break

class AutoRetainingMessage(threading.Thread):
    """
    This class keeps an SQS message hidden from the queue by issuing an api
    call to SQS every 30 seconds. It stops retaining when the message
    is deleted
    """
    def __init__(self, message, message_data):
        threading.Thread.__init__(self)
        self._message_lock = threading.Lock()
        self._message = message
        self._message_data = message_data
    def get_data(self):
        return self._message_data
    def delete(self):
        try:
            self._message_lock.acquire()
            self._message.delete()
            self._message_lock.release()
            return True
        except:
            return False
    def run(self):
        #loop will break when the message been deleted
        while True:
            try:
                self._message_lock.acquire()
                self._message.change_visibility(30)
                self._message_lock.release()
                time.sleep(25)
            except:
                break

new_tasks_queue = Sqs(global_conf.NEW_TASKS_QUEUE)
workers_messaging_queue = Sqs(global_conf.WORKERS_MESSAGING_QUEUE)
