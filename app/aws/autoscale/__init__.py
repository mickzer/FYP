import logging
log = logging.getLogger('root_logger')

import global_conf

import boto.ec2.autoscale as ec2_autoscale
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import AutoScalingGroup

class AutoScale:
	def __init__(self):
		try:
			log.info('Connecting to EC2 AutoScaling')
			self.con = ec2_autoscale.connect_to_region(global_conf.REGION)
		except Exception, e:
			log.error('AutoScale Error', exc_info=True)
	def create_launch_configuration(self, lc_name, sg_ids, instance_type, user_data=None, profile=None):
		try:
			lc = LaunchConfiguration(
				name=lc_name,
				image_id='ami-69b9941e', #hard coding amazon linux ami ftm
				security_groups=sg_ids,
				instance_type=instance_type,
				user_data=user_data,
				instance_profile_name=profile,
				associate_public_ip_address=True,
				key_name='MyPair') #IAM ROLE
				#add spot price & user datain the future bitches
			self.con.create_launch_configuration(lc)
			log.info('Created Launch Configuration - %s' % (lc.name))
			return lc
		except Exception, e:
			log.error('Failed to Create Launch Configuration - %s' % (lc_name), exc_info=True)
			return False

	def delete_launch_configuration(self, launch_conifg_name):
		try:
			self.con.delete_launch_configuration(launch_conifg_name)
			log.info('Deleted Launch Configuration - %s' % (launch_conifg_name))
			return True
		except Exception, e:
			log.error('Failed to Delete Launch Configuration - %s' % (launch_conifg_name), exc_info=True)
			return False

	def create_autoscaling_group(self, launch_config, group_name, subnets, num_instances=1):
		if launch_config:
			try:
				asg = AutoScalingGroup(
					group_name=group_name,
					desired_capacity=num_instances,
					max_size=num_instances,
					min_size=num_instances,
					launch_config=launch_config,
					vpc_zone_identifier=subnets)
				self.con.create_auto_scaling_group(asg)
				log.info('Created Auto Scaling Group - %s' % (asg.name))
				return True
			except Exception, e:
				log.error('Failed to Create Auto Scaling Group - %s' % (asg.name), exc_info=True)
				return False

	def delete_autoscaling_group(self, asg_name):
		try:
			self.con.delete_auto_scaling_group(asg_name)
			log.info('Deleted Auto Scaling Group - %s' % (asg_name))
			return True
		except Exception, e:
			log.error('Failed to Delete Auto Scaling Group - %s' % (asg_name), exc_info=True)
			return False
	def describe_autoscaling_group(self, asg_name):
		try:
			r=self.con.get_all_groups(names=[asg_name])
			if r:
				return r[0]
			return []
		except Exception, e:
			log.error('Failed to Describe Auto Scaling Group - %s' % (asg_name), exc_info=True)
			return False

autoscale = AutoScale()
