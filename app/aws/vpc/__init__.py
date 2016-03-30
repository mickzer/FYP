import logging
log = logging.getLogger('root_logger')

import global_conf

import boto.vpc

class VPC:
	def __init__(self):
		self.con = boto.vpc.connect_to_region('eu-west-1')
	def get_subnets(self, vpc_id):
		try:
			subnets = self.con.get_all_subnets(filters={'vpcId': vpc_id})
			subnets = [s.id for s in subnets]
			return subnets
		except Exception, e:
			log.error('Failed to get subnets for  - %s' % (vpc_id), exc_info=True)
			return False

vpc = VPC()
