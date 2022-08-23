package lib

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/pkg/errors"
	"io"
	"log"
	"syscall"
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
			S3ForcePathStyle: aws.Bool(true), // LocalStackのEndpointを指定する時に必要
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

func (s *S3Session) CreateDirectory(key string, mode uint32, ctx *fuse.Context) *Directory {
	return &Directory{
		Key:      key,
		Meta:     NewMeta(fuse.S_IFDIR|mode, ctx),
		FileMeta: make(map[string]string, 0),
		sess:     s,
	}
}

func (s *S3Session) NewDirectory(bucket, key string) (*Directory, error) {
	log.Println("new directory", key)

	if key == "" {
		return nil, errors.New("Key shouldn't be empty")
	}

	body, err := s.Get(bucket, key)
	if err != nil {
		return nil, err
	}

	var dir *Directory
	if err = json.Unmarshal(body, &dir); err != nil {
		return nil, err
	}
	dir.sess = s

	return dir, nil
}

func (s *S3Session) CreateFile(key, parent string, mode uint32, context *fuse.Context) *File {
	log.Println("create file")

	return &File{
		Key:        key,
		Meta:       NewMeta(fuse.S_IFREG|mode, context),
		ExtentSize: 6 * 1024, // 8KB
		Extent:     make(map[int64]*Extent, 0),
		sess:       s,
	}
}

func (s *S3Session) NewFile(bucket, key string) (*File, error) {
	log.Println("new file")

	obj, err := s.Get(bucket, key)
	if err != nil {
		return nil, err
	}

	var node *File
	if err = json.Unmarshal(obj, &node); err != nil {
		return nil, err
	}

	node.sess = s
	for _, e := range node.Extent {
		e.sess = s
	}

	return node, nil
}
func (s *S3Session) CreateExtent(size int64) *Extent {
	return &Extent{
		body: make([]byte, size),
		sess: s,
	}
}

func (s *S3Session) NewNode(bucket, key string) (*Node, error) {
	obj, err := s.Get(bucket, key)
	if err != nil {
		return nil, err
	}

	var node *Node
	if err = json.Unmarshal(obj, &node); err != nil {
		return nil, err
	}

	return node, err
}

// NewTypedNode returns Directory, File or Symlink
func (s *S3Session) NewTypedNode(bucket, key string) (interface{}, error) {
	obj, err := s.Get(bucket, key)
	if err != nil {
		return nil, err
	}

	var tmpNode *Node
	if err = json.Unmarshal(obj, &tmpNode); err != nil {
		return nil, err
	}

	var node interface{}

	switch tmpNode.Meta.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		node = &Directory{sess: s}
	case syscall.S_IFREG:
		node = &File{sess: s}
	//case syscall.S_IFLNK:
	//	node = &SymLink{sess: s}
	default:
		panic("Not implemented")
	}

	err = json.Unmarshal(obj, &node)
	return node, err
}

func (s *S3Session) Upload(bucket, key string, r io.ReadSeeker) error {
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
