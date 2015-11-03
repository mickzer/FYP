#app confs
CWD = '/home/ec2-user/fyp/'
SPLIT_SIZE=134217728 #128MB
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
NUM_WORKERS=10
WORKER_EC2_TYPE = 't2.micro'
WORKER_INSTANCE_PROFILE='MyRole'
WORKER_LAUNCH_CONFIG='FYP-WORKER-AMZN-LNX'
WORKERS_AUTOSCALING_GROUP='FYP-WORKERS'
