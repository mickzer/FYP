#app confs
CWD = '/home/ec2-user/fyp/'
SPLIT_SIZE=128000000 #128 Mb
#aws confs
REGION = 'eu-west-1'
#should be set on instance first boot
#and should not be changable
VPC_ID = 'vpc-3c4a3a59'
#S3 confs
BUCKET = 'michaeloneillfyp'
#SQS confs
NEW_TASKS_QUEUE='new_tasks_queue'
WORKERS_MESSAGING_QUEUE='workers_messaging_queue'
#EC2 confs
NUM_WORKERS=40
WORKER_EC2_TYPE = 'm1.medium'
WORKER_INSTANCE_PROFILE='MyRole'
WORKER_LAUNCH_CONFIG='FYP-WORKER-AMZN-LNX'
WORKERS_AUTOSCALING_GROUP='FYP-WORKERS'
WORKER_SPOT_PRICE=0.05
WORKER_AMI_ID='ami-a93484da'#'ami-69b9941e'
WORKER_KEY_PAIR='MyPair'
WORKER_INSTANCE_PROFILE='MyRole'
WORKER_SECURITY_GROUPS=['sg-36933c52']
