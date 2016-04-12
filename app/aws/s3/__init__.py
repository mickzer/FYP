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
        try:
            c = boto.connect_s3()
            self.bucket = c.get_bucket(self.bucket_name)
        except Exception, e:
            log.error('S3 Error: %s (%s)' % (str(e), self.bucket_name), exc_info=True)

    def exists(self, key):
        try:
            k = self.bucket.get_key(key)
            return True if k else False
        except Exception, e:
            log.error('S3 Error: %s (%s)' % (str(e), self.bucket_name), exc_info=True)
            return False

    def copy(self, src_key, dest_key):
        if src_key[0] == '/':
            src_key=src_key[1:]
        if dest_key[0] == '/':
            dest_key=dest_key[1:]
        try:
            self.bucket.copy_key(src_key_name=src_key, src_bucket_name=self.bucket_name, new_key_name=dest_key)
            return True
        except Exception, e:
            log.error('S3 Copy Error: %s (%s)' % (str(e), self.bucket_name), exc_info=True)
            return False

    def put(self, file_path, key=None):
        """This function uploads a file to S3.

        Args:
           file_path (str):  The path of the file for upload

        Kwargs:
           key (str): Specific key to use for the file.

        Returns:
           bool.  The return code::

              True -- Success!
              False -- Failure
        """
        try:
            key_name = key if key else os.path.basename(file_path)
            size = os.stat(file_path).st_size
            if size < 104857600: #100 mb
                k = Key(self.bucket)
                k.key = key_name
                sent = k.set_contents_from_filename(file_path)
                log.info('Uploading %s to S3 (%s)' % (key_name, self.bucket_name))
                return sent == size
            else:
                log.info('Multipart Uploading %s to S3 (%s)' % (key_name, self.bucket_name))
                mp = self.bucket.initiate_multipart_upload(key_name)
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
    def get(self, key, file_path=None):
        """This function downloads a file from S3.

        Args:
           key(str):  The key of the file to download

        Kwargs:
           file_path (str): Used if the file should be saved

        Returns:
           bool or Key.  The return code::

                 dict -- A dict with the key, value from S3
              True -- Success
              False -- Failure
        """
        log.info('Downloading file (%s) from S3 (%s)' % (key, self.bucket_name))
        try:
            k = self.bucket.get_key(key)
            if k:
                if file_path:
                    if not os.path.exists(os.path.dirname(file_path)):
                        os.makedirs(os.path.dirname(file_path))
                    k.get_contents_to_filename(file_path)
                    log.info('Download Complete (%s) from S3 (%s)' % (key, self.bucket_name))
                    return True
                else:
                    return {'key': os.path.basename(key), 'value': k.get_contents_as_string()}
            else:
                return False
        except Exception, e:
            log.error('Failed to download key (%s) from S3 (%s)' % (key, self.bucket_name), exc_info=True)
            return False

    def delete(self, key):
        try:
            log.info('Deleting File (%s) from S3 (%s)' % (key, self.bucket_name))
            key = self.bucket.get_key(key)
            if key:
                key.delete()
                log.info('Deleted File (%s) from S3 (%s)' % (key, self.bucket_name))
                return True
            else:
                log.error('Failed to Delete File (%s) from S3 (%s) - Does Not Exist' % (key, self.bucket_name))
        except Exception, e:
            log.error('Failed to Delete File (%s) from S3 (%s)' % (key, self.bucket_name), exc_info=True)
            return False

    def get_directory(self, directory_key, file_path=None):
        """This function downloads all the files in a directory from S3.

        Args:
           key(str):  The key of the file to download

        Kwargs:
           file_path (str): Used if the files should be saved in a folder

        Returns:
           bool or Key.  The return code::

                 list-- A list of dicts with the key, value from S3
              True -- Success
              False -- Failure
        """
        try:
            log.info('Downloading directory (%s) from S3 (%s)' % (directory_key, self.bucket_name))
            if file_path:
                if file_path[-1] != '/':
                    file_path += '/'
                if not os.path.exists(file_path):
                    os.makedirs(file_path)
            #ensure dir key is of the format /key/
            if directory_key[0] == '/':
                directory_key = directory_key[1:]
            if directory_key[-1] != '/':
                directory_key += '/'
            #list all keys in dir
            keys = self.bucket.list(directory_key)
            result = []
            for k in keys:
                #for each key except the directory itself
                if k.key != directory_key:
                    if file_path:
                        #download it to flie_path
                        self.get(k.key, file_path=file_path+os.path.basename(k.key))
                    else:
                        #download it and save it in results
                        k = self.get(k.key)
                        if k:
                            result.append(k)
            if file_path:
                return True
            else:
                return result if result else False
        except Exception, e:
            log.error('Failed to download directory (%s) from S3 (%s)' % (directory_key, self.bucket_name), exc_info=True)
            return False

    def delete_directory(self, directory_key):
        """This function deletes a directory of objects from S3.

        Args:
           key(str):  The key of the directory to delete

        Returns:
           bool or Key.  The return code::
              True -- Success
              False -- Failure
        """
        try:
            #ensure dir key is of the format /key/
            if directory_key[0] == '/':
                directory_key = directory_key[1:]
            if directory_key[-1] != '/':
                directory_key += '/'
            log.info('Deleting directory (%s) from S3 (%s)' % (directory_key, self.bucket_name))
            keys = self.bucket.list(directory_key)
            for key in keys:
                key.delete()
        except Exception, e:
            log.error('Failed to delete directory (%s) from S3 (%s)' % (directory_key, self.bucket_name), exc_info=True)
            return False
        log.info('Deleted directory (%s) from S3 (%s)' % (directory_key, self.bucket_name))
        return True

s3=S3()
