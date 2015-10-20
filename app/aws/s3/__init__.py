import logging
log = logging.getLogger('root_logger')

import global_conf

import math, os
import boto
from boto.s3.key import Key
from filechunkio import FileChunkIO

class S3:
	def __init__(self):
		self.bucket_name = global_conf.BUCKET
		log.info('Connecting to S3 (%s)' % (self.bucket_name))
		c = boto.connect_s3()
		try:
			self.bucket = c.get_bucket(self.bucket_name)
		except Exception, e:
			log.error('%s (%s)' % (str(e), self.bucket_name), exc_info=True)
	def put(self, file_path, key=None):
		try:
			size = os.stat(file_path).st_size
			#log.info('Uploading File %s bytes (%s)' %(size, self.bucket_name))
			if size < 104857600:
				k = Key(self.bucket)
				k.key = key if key else os.path.basename(file_path)
				sent = k.set_contents_from_filename(file_path)
				log.info('Uploading to S3 (%s)' % (self.bucket_name))
				return sent == size
			else:
				log.info('Multipart Uploading to S3 (%s)' % (self.bucket_name))
				mp = self.bucket.initiate_multipart_upload(os.path.basename(file_path))
				chunk_size = 52428800
				chunk_count = int(math.ceil(size / float(chunk_size)))
				#Send the file parts, using FileChunkIO to create a file-like object
				# that points to a certain byte range within the original file. We
				# set bytes to never exceed the original file size.
				for i in range(chunk_count):
				     offset = chunk_size * i
				     bytes = min(chunk_size, size - offset)
				     with FileChunkIO(file_path, 'r', offset=offset, bytes=bytes) as fp:
				         mp.upload_part_from_file(fp, part_num=i + 1)
				# Finish the upload
				mp.complete_upload()
				return True
		except Exception, e:
			log.error('Failed to upload to S3 (%s)' % (self.bucket_name), exc_info=True)
			return False
	def get(self, key, save_to_file=False, file_path=None):
		log.info('Downloading from S3 (%s)' % (self.bucket_name))
		try:
			k = self.bucket.get_key(key)
			if k:
				if save_to_file:
					k.get_contents_to_filename(file_path)
					return True
				else:
					return k
			else:
				return False
		except Exception, e:
			log.error('Failed to download from S3 (%s)' % (self.bucket_name), exc_info=True)
			return False