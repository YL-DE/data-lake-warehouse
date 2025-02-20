#!/bin/bash
mkdir -p /home/hadoop/bootstrap
set -ex
sudo yum -y install python37


sudo pip-3.7 install boto3
sudo pip-3.7 install awscli
sudo pip-3.7 install requests
sudo pip-3.7 install datetime
sudo yum install -y gcc postgresql-devel
sudo pip-3.7 install -U psycopg2-binary




sudo touch /stderr
sudo touch /stdout
sudo chmod 777 /stderr /stdout