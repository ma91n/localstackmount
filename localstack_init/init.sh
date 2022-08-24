#!/bin/sh
export DEFAULT_REGION=ap-northeast-1

echo "create test bucket"
awslocal s3api create-bucket --bucket local-test --create-bucket-configuration LocationConstraint=ap-northeast-1
awslocal s3 cp /localstack_init/put1.txt s3://local-test/2022/08/22/put1.txt --acl public-read
awslocal s3 cp /localstack_init/put1.txt s3://local-test/2022/08/23/put3.txt --acl public-read
