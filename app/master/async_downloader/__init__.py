import logging
log = logging.getLogger('root_logger')
import threading, time, random, global_conf
from Queue import Queue
from aws.s3 import s3

class AsyncDownloader(threading.Thread):

	def __init__(self):
		threading.Thread.__init__(self)
		self.files = {}
		#paused flag
		self.paused = False
		#paused locking condition
		self.pause_cond = threading.Condition(threading.Lock())

	def add(self, job_id, path):
		if job_id not in self.files:
			self.files[job_id] = Queue()
		self.files[job_id].put(path)
		log.info('Queued for Async Download: ' + path)

	def pause(self, job_id):
		self.paused = True
		#acquire the lock which will make the calling thread
		#wait until the current download finishes
		self.pause_cond.acquire()
		#return the queue of remaining task outputs
		#to be downloaded for the param job
		if job_id in self.files:
			r = self.files[job_id]
			del self.files[job_id]
			return r
		return None

	def resume(self):
		self.paused = False
		# Notify so thread will wake after lock released
		self.pause_cond.notify()
		# Now release the lock
		self.pause_cond.release()

	def run(self):
		log.info('AsyncDownloader Running')
		while True:
			with self.pause_cond:
				while self.paused:
					self.pause_cond.wait()
				#get all the queues that aren't empty
				qs = []
				if self.files:
					for key in self.files:
						if not self.files[key].empty():
							qs.append({key: self.files[key]})
				if qs:
					#pick a random queue
					q = random.choice(qs)
					#download the next file in the queue
					job_id = q.keys()[0]
					f = q[job_id].get()
					log.info('Async Downloading: ' + f)
					#download from s3
					s3.get(f, file_path=global_conf.CWD+f)
					#delete the key from self.files if queue is empty
					if q[job_id].empty():
						del self.files[job_id]
				time.sleep(0.5)
