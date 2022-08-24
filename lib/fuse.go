package lib

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/spaolacci/murmur3"
	"hash/fnv"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type FileSystem struct {
	pathfs.FileSystem

	sess *S3Session
}

func NewFileSystem(sess *S3Session) *pathfs.PathNodeFs {
	return pathfs.NewPathNodeFs(&FileSystem{
		FileSystem: pathfs.NewDefaultFileSystem(),
		sess:       sess,
	}, nil)
}

func (f *FileSystem) GetAttr(name string, ctx *fuse.Context) (*fuse.Attr, fuse.Status) {
	/*	log.Printf("GetAttr name:%s", name)
	 */
	if len(name) == 0 {
		// rootの場合
		return &fuse.Attr{
			Ino:       inodeHash(rootKey()),
			Size:      uint64(15), // TODO
			Blocks:    1,
			Atimensec: 1,
			Mtimensec: 1,
			Ctimensec: 1,
			Mode:      fuse.S_IFDIR | 0755,
			Nlink:     1,
			Owner: fuse.Owner{
				Uid: ctx.Owner.Uid,
				Gid: ctx.Owner.Gid,
			},
		}, fuse.OK
	}

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, currentPath := items[0], strings.Join(items[1:], string(filepath.Separator))

	list, err := f.sess.List(bucket, currentPath)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if len(list) == 0 {
		return nil, fuse.ENOENT
	}

	//log.Println("GetAttr name:", name, "list:", list)

	if list[0] == currentPath {
		//log.Println("GetAttr name:", name, "判定:ファイル")
		// 完全一致の場合はS3オブジェクトであるのでファイルを返す
		return &fuse.Attr{
			Ino:       inodeHash(keyGen([]byte(name))),
			Size:      uint64(15), // TODO
			Blocks:    1,
			Atimensec: 1,
			Mtimensec: 1,
			Ctimensec: 1,
			Mode:      fuse.S_IFREG | 0755,
			Nlink:     1,
			Owner: fuse.Owner{
				Uid: ctx.Owner.Uid,
				Gid: ctx.Owner.Gid,
			},
		}, fuse.OK
	} else if len(list) >= 1 {
		//log.Println("GetAttr name:", name, "判定:ディレクトリ")
		return &fuse.Attr{
			Ino:       inodeHash(rootKey()),
			Size:      uint64(15), // TODO
			Blocks:    1,
			Atimensec: 1,
			Mtimensec: 1,
			Ctimensec: 1,
			Mode:      fuse.S_IFDIR | 0755,
			Nlink:     1,
			Owner: fuse.Owner{
				Uid: ctx.Owner.Uid,
				Gid: ctx.Owner.Gid,
			},
		}, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (f *FileSystem) Open(name string, flags uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	log.Println("Open name:", name)

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, currentPath := items[0], strings.Join(items[1:], string(filepath.Separator))

	get, err := f.sess.Get(bucket, currentPath)
	if err != nil {
		log.Println("get err:", err)
		return nil, fuse.ENOENT
	}
	log.Println("get object:", string(get))

	return nodefs.NewDataFile(get), fuse.OK
}

func (f *FileSystem) Rename(oldName string, newName string, _ *fuse.Context) fuse.Status {
	// TODO objectのみは許可する。ディレクトリの変更は許容しない
	return fuse.EIO
}

func (f *FileSystem) Mkdir(name string, mode uint32, ctx *fuse.Context) fuse.Status {
	// TODO S3でディレクトリの表現をマネジメントコンソールを見て確認
	return fuse.OK
}

func (f *FileSystem) Create(name string, flags uint32, mode uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	log.Printf("Create name:%s", name)

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, currentPath := items[0], strings.Join(items[1:], string(filepath.Separator))

	if f.sess.Exists(bucket, currentPath) {
		return nil, fuse.EINVAL // TODO already existsを表現したい
	}

	if err := f.sess.PutBytes(bucket, currentPath, make([]byte, 0)); err != nil {
		log.Printf("put bytes:%v", err)
		return nil, fuse.EIO
	}

	log.Printf("create ok")
	return nodefs.NewDefaultFile(), fuse.OK
}

type S3File struct {
	nodefs.File

	bucket string
	key    string

	sess S3Session

	temp *os.File
}

func (f S3File) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
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

func (f S3File) Write(data []byte, off int64) (written uint32, code fuse.Status) {

	if f.temp == nil {
		// 追記するには一度getする必要がある
		data, err := f.sess.Get(f.bucket, f.key)
		if err != nil {
			return 0, fuse.EIO
		}

		temp, err := os.CreateTemp("", "localstackmount")
		if err != nil {
			return 0, fuse.ENOSYS
		}
		_, _ = temp.Write(data)

		f.temp = temp
	}

	length, _ := f.temp.WriteAt(data, off)
	return uint32(length), fuse.OK
}

func (f S3File) Flush() fuse.Status {
	if f.temp == nil {
		// 書き込まれたデータがない？
		return fuse.OK
	}
	defer os.Remove(f.temp.Name()) // clean up

	if err := f.sess.Put(f.bucket, f.key, f.temp); err != nil {
		return fuse.EIO
	}

	return fuse.OK
}

func (f S3File) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	// TODO metadataにatime, mimeなどを格納する？
	// https://stackoverflow.com/questions/13455168/is-there-a-way-to-touch-a-file-in-amazon-s3
	return fuse.OK
}

func (f *FileSystem) OpenDir(name string, _ *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	log.Printf("OpenDir name:%s", name)

	if len(name) == 0 {
		// Rootの場合、バケット名を返す
		buckets, err := f.sess.ListBuckets()
		if err != nil {
			return nil, fuse.ENOENT
		}

		entries := make([]fuse.DirEntry, 0, len(buckets))
		for _, bucketName := range buckets {
			entries = append(entries, fuse.DirEntry{
				Name: bucketName,
				Ino:  inodeHash(bucketName),
			})
		}
		return entries, fuse.OK
	}

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, currentPath := items[0], strings.Join(items[1:], string(filepath.Separator))
	log.Printf("bucket:%s key:%s\n", bucket, currentPath)

	objKeys, err := f.sess.List(bucket, currentPath)
	if err != nil {
		return nil, fuse.ENOENT
	}

	entries := make([]fuse.DirEntry, 0, len(objKeys))
	for _, objKey := range objKeys {
		dirName := strings.Split(objKey, string(filepath.Separator))[0]

		if currentPath != "" && strings.HasPrefix(objKey, currentPath) {
			// rootパス以外の場合
			dirName = NextParentPath(objKey, currentPath)
		}

		log.Println("OpenDir objKey:", objKey, "dirName:", dirName, "currentPath:", currentPath)
		entries = append(entries, fuse.DirEntry{
			Name: dirName,
			Ino:  inodeHash(path.Join(name, dirName)),
		})
		continue
	}

	// TODO entries の重複排除をすべき

	return entries, fuse.OK
}

func (f *FileSystem) Access(name string, mode uint32, _ *fuse.Context) (code fuse.Status) {
	log.Printf("Access: name:%s\n", name) // local-test/2022/08/22/put1.txt

	if name == "" {
		// root
		return fuse.OK
	}

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, key := items[0], strings.Join(items[1:], string(filepath.Separator))

	list, err := f.sess.List(bucket, key)
	if err != nil {
		log.Println("access ng")
		return fuse.ENOENT
	}
	if len(list) > 0 {
		log.Println("access ok")
		return fuse.OK
	}

	log.Println("access ng")
	return fuse.ENOENT
}

func (f *FileSystem) Truncate(name string, size uint64, _ *fuse.Context) (code fuse.Status) {
	log.Println("Truncate")

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, currentPath := items[0], strings.Join(items[1:], string(filepath.Separator))
	log.Printf("bucket:%s key:%s\n", bucket, currentPath)

	exists := f.sess.Exists(bucket, currentPath)
	if exists {
		// TODO 削除処理
		return fuse.OK
	}
	return fuse.ENOENT
}

func (f *FileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	log.Println("Utimens name:", name)

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, currentPath := items[0], strings.Join(items[1:], string(filepath.Separator))
	log.Printf("bucket:%s key:%s\n", bucket, currentPath)

	if f.sess.Exists(bucket, currentPath) {
		// TODO S3上のメタファイルを書き換える？
		return fuse.OK
	}
	return fuse.ENOENT
}

func (f *FileSystem) String() string {
	return "localstackmount"
}

func inodeHash(o string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(o))
	return h.Sum64()
}

func keyGen(input []byte) string {
	return fmt.Sprintf("%x", murmur3.Sum64(input))
}

func rootKey() string {
	return keyGen([]byte("localstackmount"))
}
