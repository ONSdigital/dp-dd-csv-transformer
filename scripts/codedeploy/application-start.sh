#!/bin/bash

AWS_REGION=
CONFIG_BUCKET=
ECR_REPOSITORY_URI=
GIT_COMMIT=

INSTANCE=$(curl -s http://instance-data/latest/meta-data/instance-id)
CONFIG=$(aws --region $AWS_REGION ec2 describe-tags --filters "Name=resource-id,Values=$INSTANCE" "Name=key,Values=Configuration" --output text | awk '{print $5}')

(aws s3 cp s3://$CONFIG_BUCKET/dp-dd-csv-transformer/$CONFIG.asc . && gpg --decrypt $CONFIG.asc > $CONFIG) || exit $?

source $CONFIG && docker run -d                    \
  --env=AWS_REGION=$AWS_REGION                     \
  --env=BIND_ADDR=$BIND_ADDR                       \
  --env=KAFKA_ADDR=$KAFKA_ADDR                     \
  --env=KAFKA_CONSUMER_GROUP=$KAFKA_CONSUMER_GROUP \
  --env=KAFKA_CONSUMER_TOPIC=$KAFKA_CONSUMER_TOPIC \
  --env=HIERARCHY_ENDPOINT=$HIERARCHY_ENDPOINT     \
  --env=USE_GZIP=$USE_GZIP                         \
  --name=dp-dd-csv-transformer                     \
  --net=$DOCKER_NETWORK                            \
  --restart=always                                 \
  $ECR_REPOSITORY_URI/dp-dd-csv-transformer:$GIT_COMMIT
