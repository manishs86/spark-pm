#!/bin/bash

# Specify the cluster name
CLUSTER_NAME=any-arbitrary-name

# Insert the IAM KEYNAME
# You get this key from AWS console's IAM section
KEY_NAME=your-key-name


# If you have not previously created the default Amazon EMR service role and EC2 instance profile,
# type `aws emr create-default-roles` to create them before typing the create-cluster subcommand.
aws emr create-cluster \
--name $CLUSTER_NAME \
--use-default-roles \
--release-label emr-5.31.0 \
--instance-count 4 \
--applications Name=Spark Name=Hadoop Name=Zeppelin Name=Livy \
--ec2-attributes KeyName=$KEY_NAME \
--instance-type m5.xlarge \
--bootstrap-actions Path="s3://your-actions-bucket/bootstrap.sh", \
Args=["instance.isMaster=true","echo running on master node"]\
--auto-terminate
