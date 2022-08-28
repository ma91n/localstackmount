# localstackmount
S3 on LocalStack mount by go-fuse 


## OSX settings

Mac OSX is not supported FUSE.

Install osxfuse below link.

https://osxfuse.github.io/2020/10/05/OSXFUSE-3.11.2.html

## WSL settings

Make sure you edit

`/etc/fuse.conf` and add `user_allow_other`.

* https://github.com/microsoft/WSL/issues/8580
* https://github.com/microsoft/WSL/issues/8498

## Quick start

```sh
# if you want run localstack
# docker-compose up -d localstack

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

## docker compose

WIP 

```sh
docker compose up -d
docker exec -it localstackmount-mount-1 bash
cd ~/mount/localstack/
ls
# <your buckets>
```


## Limitations

* [ ] does not store file `mode` / `owner` / `group`
* [ ] does not support `symlink` or `hardlink`
* [ ] `ctime`, atime and `mtime` are partially implement.
* [ ] cannot `rename` virtual directory. only support directory object.

