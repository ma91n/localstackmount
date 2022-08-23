package lib

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
)

type S3Session struct {
	svc *s3.S3
}

func NewS3Session(region string) *S3Session {
	return &S3Session{
		svc: s3.New(session.Must(session.NewSession()), &aws.Config{
			Credentials:      credentials.NewStaticCredentials("test", "test", ""),
			Endpoint:         aws.String("http://localhost:4566"),
			Region:           aws.String(region),
			S3ForcePathStyle: aws.Bool(true),
		}),
	}
}

func (s *S3Session) Exists(bucket, key string) bool {
	_, err := s.svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err == nil
}

func (s *S3Session) Put(bucket, key string, r io.ReadSeeker) error {
	_, err := s.svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	return nil
}

func (s *S3Session) PutBytes(bucket, key string, b []byte) error {
	return s.Put(bucket, key, bytes.NewReader(b))
}

func (s *S3Session) Get(bucket, key string) ([]byte, error) {
	obj, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	defer obj.Body.Close()

	body, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("read obj body: %w", err)
	}

	return body, nil
}

func (s *S3Session) List(bucket, prefix string) ([]string, error) {
	objects, err := s.svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	keys := make([]string, 0, len(objects.Contents))
	for _, v := range objects.Contents {
		keys = append(keys, *v.Key)
	}

	return keys, nil
}

func (s *S3Session) ListBuckets() ([]string, error) {
	out, err := s.svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("list bucket: %w", err)
	}

	bucketNames := make([]string, 0, len(out.Buckets))
	for _, v := range out.Buckets {
		bucketNames = append(bucketNames, *v.Name)
	}
	return bucketNames, nil
}
