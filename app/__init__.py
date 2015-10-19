import logger
log = logger.create_logger('root_logger')
log.debug('Logger Created')

from aws.sqs import Sqs
sqs = Sqs('fyp')
d = {'name': 'michael', 'age': 21}
print sqs.get_message()