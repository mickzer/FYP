import logger
log = logger.create_logger('root_logger')
log.debug('Logger Created')

import time
start = int(round(time.time() * 1000))

from aws.s3 import S3
s3 = S3()
print(s3.get('hg38.2bit', save_to_file=True, file_path='m.2bit'))
length = int(round(time.time() * 1000)) - start
log.debug('Took %s ms' % (length))