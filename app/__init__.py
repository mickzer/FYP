import logger
log = logger.create_logger('root_logger')
log.debug('Logger Created')

from aws.s3 import S3
s3 = S3()