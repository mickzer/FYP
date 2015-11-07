import logging
log = logging.getLogger('root_logger')
import threading, global_conf
from Queue import Queue
from aws.s3 import s3

class AsyncDownloader(threading.Thread):

	def __init__(self):
		threading.Thread.__init__(self)
		self.files = Queue()
		self.run_thread = True

	def add(self, path):
		self.files.put(path)
		log.info('Queued for Async Download: ' + path)

	def stop(self):
		self.run_thread = False

	def run(self):
		log.info('AsyncDownloader Running')
		while self.run_thread:
			f = self.files.get()
			log.info('Async Downloading ' + f)
			self.download(f)

	def download(self, path):
		s3.get(path, file_path=global_conf.CWD+path)
