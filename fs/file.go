package fs

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
	log.Println("s3file Read off:", off)

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
	log.Println("s3file Write", "off:", off)

	if f.temp == nil {
		// 追記するには一度getする必要がある
		get, err := f.sess.Get(f.bucket, f.key)
		if err != nil {
			return 0, fuse.EIO
		}

		temp, err := os.CreateTemp("", "localstackmount")
		if err != nil {
			return 0, fuse.EIO
		}

		if _, err := temp.Write(get); err != nil {
			return 0, fuse.EIO
		}
		f.temp = temp
	}

	length, err := f.temp.WriteAt(data, off)

	if _, err := f.temp.Seek(0, 0); err != nil { // 書き込んで分をflushで読み取らせるため、seekで位置を戻す
		log.Println("seek err:", err)
		return 0, fuse.EIO
	}

	if err != nil {
		return 0, fuse.EIO
	}
	return uint32(length), fuse.OK
}

func (f *S3File) Release() {
	log.Println("s3file Release")

	if f.temp != nil {
		defer func() {
			_ = os.Remove(f.temp.Name())
			f.temp = nil
		}()
	}
}

func (f *S3File) Flush() fuse.Status {
	log.Println("s3file Flush")
	if f.temp == nil {
		return fuse.OK
	}
	defer func() {
		_ = os.Remove(f.temp.Name())
		f.temp = nil
	}()

	body, err := io.ReadAll(f.temp)
	if err != nil {
		return fuse.EIO
	}

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

func (f *S3File) Truncate(size uint64) fuse.Status {
	log.Println("s3file Truncate size:", size)

	if f.temp != nil {
		_ = os.Remove(f.temp.Name())
	}

	temp, err := os.CreateTemp("", "localstackmount")
	if err != nil {
		return fuse.EIO
	}
	f.temp = temp

	return fuse.OK
}

func (f *S3File) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	log.Println("s3file Allocate")
	return fuse.OK
}

func (f *S3File) Fsync(flags int) (code fuse.Status) {
	log.Println("s3file Fsync flags:", flags)
	return fuse.OK
}

func (f *S3File) String() string {
	return "S3File"
}
