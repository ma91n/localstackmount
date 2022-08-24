package lib

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"io"
	"log"
	"os"
	"time"
)

type S3File struct {
	nodefs.File

	bucket string
	key    string

	sess *S3Session

	temp *os.File
}

func (f *S3File) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	log.Println("Read off:", off)

	data, err := f.sess.Get(f.bucket, f.key)
	if err != nil {
		return nil, fuse.EIO
	}
	end := int(off) + len(dest)
	if end > len(data) {
		end = len(data)
	}

	return fuse.ReadResultData(data[off:end]), fuse.OK
}

func (f *S3File) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	log.Println("write:", string(data), "off:", off)

	if f.temp == nil {
		// 追記するには一度getする必要がある
		data, err := f.sess.Get(f.bucket, f.key)
		if err != nil {
			return 0, fuse.EIO
		}

		temp, err := os.CreateTemp("", "localstackmount")
		if err != nil {
			log.Printf("temp dir: %v\n", err)
			return 0, fuse.EIO
		}
		_, _ = temp.Write(data)

		f.temp = temp
	}

	length, err := f.temp.WriteAt(data, off)
	if err != nil {
		log.Printf("temp write: %v\n", err)
		return 0, fuse.EIO
	}
	return uint32(length), fuse.OK
}

func (f *S3File) Release() {
	log.Println("Release")

	if f.temp != nil {
		defer os.Remove(f.temp.Name())
	}
}

func (f *S3File) Flush() fuse.Status {
	log.Println("flush")
	if f.temp == nil {
		return fuse.OK
	}

	body, err := io.ReadAll(f.temp)
	if err != nil {
		return fuse.EIO
	}
	log.Println("read from temp:", string(body))

	if err := f.sess.PutBytes(f.bucket, f.key, body); err != nil {
		return fuse.EIO
	}

	return fuse.OK
}

func (f *S3File) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	// TODO metadataにatime, mimeなどを格納する？
	// https://stackoverflow.com/questions/13455168/is-there-a-way-to-touch-a-file-in-amazon-s3
	return fuse.OK
}

func (f *S3File) String() string {
	return "S3File"
}
