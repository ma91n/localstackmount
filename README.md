# localstackmount
S3 on LocalStack mount by goofys 


## Quick start

```sh
# if you want run localstack
# docker-compose up -d

go install github.com/ma91n/localstackmount@latest
localstackmount

# s3 list-buckets (aws --profile local --endpoint-url http://localhost:4566 s3api list-buckets)
ls ~/mount/localstack

# s3 list-objects (aws --profile local --endpoint-url http://localhost:4566 s3api list-objects --bucket <your bucket>)
ls ~/mount/localstack/<your bucket>

# s3 create object as folder
mkdir ~/mount/localstack/<your bucket>/my-folder

# s3 create object
cd ~/mount/localstack/<your bucket>/my-folder/
echo "hello localstackmount" > hello.txt

# s3 get object
cat hello.txt
```
