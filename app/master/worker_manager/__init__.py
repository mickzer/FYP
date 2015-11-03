import logging
log = logging.getLogger('root_logger')

import global_conf

from aws.autoscale import autoscale
from aws.vpc import vpc

def launch_workers():
    log.info('Creating Workers')
    #hard coding security groups ftm
    #will need to get this progrmatically later
    sgs = ['sg-36933c52']
    #hard coding instace profile too ftm
    instance_profile = 'MyRole'
    #get user data from file
    user_data=''
    try:
        with open(global_conf.CWD+'resources/user-data.sh') as f:
            user_data=f.read()
    except Exception, e:
        log.error('Couldn\'t open user data script for workers')
    #create launch configuration for workers
    lc = autoscale.create_launch_configuration(
        lc_name=global_conf.WORKER_LAUNCH_CONFIG,
        sg_ids = sgs,
        instance_type=global_conf.WORKER_EC2_TYPE,
        profile=instance_profile,
        user_data=user_data
    )
    #not sure how I will be getting the vpc
    #id yet so hard coding ftm
    vpc_id = 'vpc-3c4a3a59'
    #get subnets from api
    subnets = vpc.get_subnets(vpc_id)
    #create asg with launch config
    if lc:
        autoscale.create_autoscaling_group(
            launch_config=lc,
            group_name=global_conf.WORKERS_AUTOSCALING_GROUP,
            subnets=subnets,
            num_instances=global_conf.NUM_WORKERS
        )
