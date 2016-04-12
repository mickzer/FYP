import sys

#app confs
CWD = '/home/ec2-user/fyp/'
#Db confs
DB_CON_STR = ''
#aws confs
REGION = ''
VPC_ID = ''
#S3 confs
BUCKET = ''
#SQS confs
NEW_TASKS_QUEUE='new_tasks_queue'
WORKERS_MESSAGING_QUEUE='workers_messaging_queue'
#EC2 confs
NUM_WORKERS=-1
WORKER_EC2_TYPE = ''
#ec2 instance type for worker nodes
WORKER_INSTANCE_PROFILE=''
WORKER_LAUNCH_CONFIG=''
WORKERS_AUTOSCALING_GROUP=''
WORKER_SPOT_PRICE=-1
WORKER_AMI_ID='ami-a93484da' #stable Amazon Linux
WORKER_KEY_PAIR='MyPair'
WORKER_SECURITY_GROUPS=[]


def verify_conf():
    if not CWD or len(CWD)==0:
        return False
    if not DB_CON_STR or len(DB_CON_STR)==0:
        return False
    if not REGION or len(REGION)==0:
        return False
    if not VPC_ID or len(VPC_ID)==0:
        return False
    if not BUCKET or len(BUCKET)==0:
        return False
    if not NEW_TASKS_QUEUE or len(NEW_TASKS_QUEUE)==0:
        return False
    if not WORKERS_MESSAGING_QUEUE or len(WORKERS_MESSAGING_QUEUE)==0:
        return False
    if not NUM_WORKERS or NUM_WORKERS < 0:
        return False
    if not WORKER_EC2_TYPE or len(WORKER_EC2_TYPE)==0:
        return False
    if not WORKER_INSTANCE_PROFILE or len(WORKER_INSTANCE_PROFILE)==0:
        return False
    if not WORKER_LAUNCH_CONFIG or len(WORKER_LAUNCH_CONFIG)==0:
        return False
    if not WORKERS_AUTOSCALING_GROUP or len(WORKERS_AUTOSCALING_GROUP)==0:
        return False
    if not WORKER_SPOT_PRICE or WORKER_SPOT_PRICE<0:
        return False
    if not WORKER_AMI_ID or len(WORKER_AMI_ID)==0:
        return False
    if not WORKER_KEY_PAIR or len(WORKER_KEY_PAIR)==0:
        return False
    if not WORKER_SECURITY_GROUPS or len(WORKER_SECURITY_GROUPS)==0:
        return False
    return True


if not verify_conf():
    print 'PLEASE UPDATE global_conf'
    sys.exit()
