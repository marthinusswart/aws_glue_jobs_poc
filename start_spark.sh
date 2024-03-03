#!/bin/bash

PROFILE_NAME=aws_poc_user
WORKSPACE_LOCATION=/home/matt/source/python/aws_glue_jobs_poc/source

echo Profile: $PROFILE_NAME
echo Workspace: $WORKSPACE_LOCATION

docker run -it -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pyspark amazon/aws-glue-libs:glue_libs_4.0.0_image_01 pyspark
