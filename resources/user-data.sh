#!/bin/bash
cd /home/ec2-user
yum -y install git
#UPDATE THIS LINE WITH YOUR GITHUB USERNAME
git clone https://github.com/YOUR_GITHUB_USERNAME/FYP fyp
pip install -r fyp/resources/requirements.txt
cp fyp/resources/worker /etc/init.d/fyp
chmod u+x /etc/init.d/fyp
mkdir /var/log/fyp
/etc/init.d/fyp start
