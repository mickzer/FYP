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
	def create_launch_configuration(self, **kwargs):
        """This function creates a launch configuration.

	    Returns:
	       bool.  The return code::
	          True -- Success
	          False -- Failure
	    """
		try:
			lc = LaunchConfiguration(**kwargs)
			self.con.create_launch_configuration(lc)
			log.info('Created Launch Configuration - %s' % (lc.name))
			return lc
		except Exception, e:
			log.error('Failed to Create Launch Configuration - %s' % (kwargs['name']), exc_info=True)
			return False

	def delete_launch_configuration(self, launch_conifg_name):
        """This function deletes a launch configuration.
        Args:
	       launch_conifg_name(str):  The name of the launch configuration to
           delete

	    Returns:
	       bool.  The return code::
	          True -- Success
	          False -- Failure
	    """
		try:
			self.con.delete_launch_configuration(launch_conifg_name)
			log.info('Deleted Launch Configuration - %s' % (launch_conifg_name))
			return True
		except Exception, e:
			log.error('Failed to Delete Launch Configuration - %s' % (launch_conifg_name), exc_info=True)
			return False

	def create_autoscaling_group(self, launch_config_name, group_name, subnets, num_instances=1):
        """This function creates an Auto Scaling Group.
        Args:
	       launch_conifg_name(str):  The name of the launch configuration
           group_name(str): The name to give the ASG
           subnets(list): The subnets the ASG should deploy instances to.
        Kwargs:
            num_instances(int): The number of instances the group should maintain
	    Returns:
	       bool or Key.  The return code::
	          True -- Success
	          False -- Failure
	    """
		if launch_config:
			try:
				asg = AutoScalingGroup(
					group_name=group_name,
					desired_capacity=num_instances,
					max_size=num_instances,
					min_size=num_instances,
					launch_config=launch_config_name,
					vpc_zone_identifier=subnets)
				self.con.create_auto_scaling_group(asg)
				log.info('Created Auto Scaling Group - %s' % (asg.name))
				return True
			except Exception, e:
				log.error('Failed to Create Auto Scaling Group - %s' % (asg.name), exc_info=True)
				return False

	def delete_autoscaling_group(self, asg_name):
        """This function deletes an Auto Scaling Group.
        Args:
	       asg_name(str):  The name of the ASG to delete

	    Returns:
	       bool.  The return code::
	          True -- Success
	          False -- Failure
	    """
		try:
			self.con.delete_auto_scaling_group(asg_name)
			log.info('Deleted Auto Scaling Group - %s' % (asg_name))
			return True
		except Exception, e:
			log.error('Failed to Delete Auto Scaling Group - %s' % (asg_name), exc_info=True)
			return False
	def describe_autoscaling_group(self, asg_name):
        """This function returns metadata about an Auto Scaling Group.
        Args:
	       asg_name(str):  The name of the ASG to describe

	    Returns:
	       dict or bool.  The return code:
	          dict  -- data
	          False -- Failure
	    """
		try:
			r=self.con.get_all_groups(names=[asg_name])
			if r:
				return r[0]
			return []
		except Exception, e:
			log.error('Failed to Describe Auto Scaling Group - %s' % (asg_name), exc_info=True)
			return False

autoscale = AutoScale()
