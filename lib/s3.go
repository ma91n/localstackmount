package lib

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"io"
	"log"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
)

type S3Session struct {
	svc *s3.S3

	Bucket string
}

func (s *S3Session) KeyGen(object []byte) string {
	return fmt.Sprintf("%x", murmur3.Sum64(object))
}

func (s *S3Session) RootKey() string {
	return s.KeyGen([]byte("test"))
}

func NewS3Session(region, bucket string) (*S3Session, error) {
	sess := &S3Session{
		svc: s3.New(session.Must(session.NewSession()), &aws.Config{
			Credentials:      credentials.NewStaticCredentials("test", "test", ""),
			Endpoint:         aws.String("http://localhost:4566"),
			Region:           aws.String(region),
			S3ForcePathStyle: aws.Bool(true), // LocalStackのEndpointを指定する時に必要
		}),
		Bucket: bucket,
	}

	if !sess.Exists(sess.RootKey()) {
		root := &Directory{
			Key: sess.RootKey(),
			Meta: Meta{
				Mode:  fuse.S_IFDIR | 0755,
				Size:  0,
				UID:   0,
				GID:   0,
				Atime: time.Now(),
				Ctime: time.Now(),
				Mtime: time.Now(),
			},
			FileMeta: make(map[string]string, 0),
			sess:     sess,
		}

		if err := root.Save(); err != nil {
			return nil, err
		}
	}

	return sess, nil
}

func (s *S3Session) Exists(key string) bool {
	_, err := s.svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	return err == nil
}

func (s *S3Session) CreateDirectory(key, _ string, mode uint32, context *fuse.Context) *Directory {
	return &Directory{
		Key:      key,
		Meta:     NewMeta(fuse.S_IFDIR|mode, context),
		FileMeta: make(map[string]string, 0),
		sess:     s,
	}
}

func (s *S3Session) NewDirectory(key string) (*Directory, error) {
	log.Println("new directory", key)

	if key == "" {
		return nil, errors.New("Key shouldn't be empty")
	}

	body, err := s.Download(key)
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

func (s *S3Session) NewFile(key string) (*File, error) {
	log.Println("new file")

	obj, err := s.Download(key)
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

func (s *S3Session) CreateSymLink(key, _ string, linkTo string, context *fuse.Context) *SymLink {
	return &SymLink{
		Key:    key,
		Meta:   NewMeta(fuse.S_IFLNK, context),
		LinkTo: linkTo,
		sess:   s,
	}
}

func (s *S3Session) NewSymLink(key string) (*SymLink, error) {
	obj, err := s.Download(key)
	if err != nil {
		return nil, err
	}

	var node *SymLink
	if err = json.Unmarshal(obj, &node); err != nil {
		return nil, err
	}

	node.sess = s
	return node, nil
}

func (s *S3Session) NewNode(key string) (*Node, error) {
	obj, err := s.Download(key)
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
func (s *S3Session) NewTypedNode(key string) (interface{}, error) {
	obj, err := s.Download(key)
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
	case syscall.S_IFLNK:
		node = &SymLink{sess: s}
	default:
		panic("Not implemented")
	}

	err = json.Unmarshal(obj, &node)
	return node, err
}

func (s *S3Session) PathWalk(relPath string) (key string, err error) {
	log.Printf("PathWalk: resPath:%s\n", relPath)

	key = s.RootKey()

	// root
	if relPath == "." || relPath == "" {
		return key, nil
	}

	log.Println("key", key)
	node, err := s.NewDirectory(key)
	if err != nil {
		return "", err
	}

	// "a/b/c" => [0:a, 1:b, 2:c] , len = 3
	paths := strings.Split(relPath, string(filepath.Separator))
	log.Println("PathWalk", paths)

	for i, p := range paths {
		var ok bool
		if key, ok = node.FileMeta[p]; !ok {
			return "", errors.New("file not found")
		}

		if i == len(paths)-1 { // key points 2:c in example.
			break
		}

		node, err = s.NewDirectory(key)
		if err != nil {
			return "", err
		}
	}

	return
}

func (s *S3Session) Upload(key string, r io.ReadSeeker) error {
	_, err := s.svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	return nil
}

func (s *S3Session) Download(key string) ([]byte, error) {
	obj, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
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

func (s *S3Session) List(prefix string) ([]string, error) {
	objects, err := s.svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	keys := make([]string, 0, len(objects.Contents))
	for _, v := range objects.Contents {
		keys = append(keys, *v.Key)
	}

	log.Println("prefix", prefix, keys)

	return keys, nil
}
