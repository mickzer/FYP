#!/bin/bash
cd /home/ec2-user
aws s3 cp s3://michaeloneillfyp/fyp_scripts/fyp fyp_compressed
tar -zxvf fyp_compressed
cd fyp
. venv/bin/activate
cd app
python __init__.py assume_worker
