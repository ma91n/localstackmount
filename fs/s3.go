package fs

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/patrickmn/go-cache"
	"io"
	"time"
)

var LocalStackEndpoint = "http://localhost:4566"

type S3Object struct {
	// S3 key
	Key string

	// S3 LastModified
	LastModified *time.Time

	// Size in bytes of the object
	Size int64 `type:"integer"`
}

type S3Session struct {
	svc *s3.S3

	cache *cache.Cache
}

func NewS3Session(region string) *S3Session {
	return &S3Session{
		svc: s3.New(session.Must(session.NewSession()), &aws.Config{
			Credentials:      credentials.NewStaticCredentials("test", "test", ""),
			Endpoint:         &LocalStackEndpoint,
			Region:           &region,
			S3ForcePathStyle: aws.Bool(true),
		}),
		cache: cache.New(5*time.Second, 10*time.Second), // TODO 適切な値を決める
	}
}

func (s *S3Session) Exists(bucket, key string) bool {
	_, err := s.svc.HeadObject(&s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	return err == nil
}

func (s *S3Session) ExistsBucket(bucket string) bool {
	if get, found := s.cache.Get(cacheKey("exists-bucket", bucket)); found {
		return get.(bool)
	}

	_, err := s.svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: &bucket,
	})

	s.cache.Set(cacheKey("exists-bucket", bucket), err == nil, 1*time.Minute) // 通常バケットは削除されないと思うので長めに取る
	return err == nil
}

func (s *S3Session) Put(bucket, key string, r io.ReadSeeker) error {
	// TODO 一致するprefixがあればキャッシュから削除

	_, err := s.svc.PutObject(&s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (s *S3Session) PutBytes(bucket, key string, b []byte) error {
	// TODO 一致するprefixがあればキャッシュから削除
	return s.Put(bucket, key, bytes.NewReader(b))
}

func (s *S3Session) Get(bucket, key string) ([]byte, error) {
	obj, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
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

func (s *S3Session) List(bucket, prefix string) ([]S3Object, error) {
	if get, found := s.cache.Get(cacheKey(bucket, prefix)); found {
		return get.([]S3Object), nil
	}

	objects, err := s.svc.ListObjects(&s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	resp := make([]S3Object, 0, len(objects.Contents))
	for _, v := range objects.Contents {
		resp = append(resp, S3Object{
			Key:          *v.Key,
			LastModified: v.LastModified,
			Size:         *v.Size,
		})
	}

	s.cache.Set(cacheKey(bucket, prefix), resp, cache.DefaultExpiration)
	return resp, nil
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

func (s *S3Session) Delete(bucket, key string) error {
	// TODO 一致するprefixがあればキャッシュから削除

	_, err := s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (s *S3Session) CreateBucket(bucket string) error {
	_, err := s.svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(endpoints.ApNortheast1RegionID), // TODO changeable
		},
	})
	if err != nil {
		return fmt.Errorf("create bucket: %v", err)
	}
	return nil
}

func cacheKey(bucket, key string) string {
	return fmt.Sprintf("%s:%s", bucket, key)
}
