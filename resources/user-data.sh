#!/bin/bash
cd /home/ec2-user
echo "S3 Download" > me.txt
aws s3 cp s3://michaeloneillfyp/fyp_scripts/fyp fyp_compressed
echo "Extracting" > me.txt
tar -zxvf fyp_compressed
echo "Entering VENV" > me.txt
cd fyp
. venv/bin/activate
cd app
echo "Running the Bad Boy" > me.txt
python __init__.py assume_worker
