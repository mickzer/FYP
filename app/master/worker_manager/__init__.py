import logging
log = logging.getLogger('root_logger')

import global_conf

from aws.autoscale import autoscale
from aws.vpc import vpc

def launch_workers():
    log.info('Creating Workers')
    #get user data from file
    user_data=''
    try:
        with open(global_conf.CWD+'resources/user-data.sh') as f:
            user_data=f.read()
    except Exception, e:
        log.error('Couldn\'t open user data script for workers')
    #create launch configuration for workers
    lc = autoscale.create_launch_configuration(
        name=global_conf.WORKER_LAUNCH_CONFIG,
        image_id=global_conf.WORKER_AMI_ID,
        security_groups=global_conf.WORKER_SECURITY_GROUPS,
        instance_type=global_conf.WORKER_EC2_TYPE,
        instance_profile_name=global_conf.WORKER_INSTANCE_PROFILE,
        user_data=user_data,
        key_name=global_conf.WORKER_KEY_PAIR,
        spot_price=global_conf.WORKER_SPOT_PRICE,
        associate_public_ip_address=True
    )
    #get subnets from api
    subnets = vpc.get_subnets(global_conf.VPC_ID)
    #create asg with launch config
    if lc:
        autoscale.create_autoscaling_group(
            launch_config=lc,
            group_name=global_conf.WORKERS_AUTOSCALING_GROUP,
            subnets=subnets,
            num_instances=global_conf.NUM_WORKERS
        )
