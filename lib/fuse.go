package lib

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/spaolacci/murmur3"
	"hash/fnv"
	"log"
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
	pos := Parse(name)
	log.Printf("GetAttr pos:%+v\n", pos)

	if pos.IsMountRoot {
		return &fuse.Attr{
			Ino:  inodeHash(rootKey()),
			Mode: fuse.S_IFDIR | 0755,
			Owner: fuse.Owner{
				Uid: ctx.Owner.Uid,
				Gid: ctx.Owner.Gid,
			},
		}, fuse.OK
	}

	if pos.IsBucketRoot {
		if f.sess.ExistsBucket(pos.Bucket) {
			return &fuse.Attr{
				Ino:  inodeHash(name),
				Mode: fuse.S_IFDIR | 0755,
				Owner: fuse.Owner{
					Uid: ctx.Owner.Uid,
					Gid: ctx.Owner.Gid,
				},
			}, fuse.OK
		}
		return nil, fuse.ENOENT
	}

	list, err := f.sess.List(pos.Bucket, pos.Key)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if len(list) == 0 {
		return nil, fuse.ENOENT
	}

	if list[0] == pos.Key {
		//log.Println("GetAttr name:", name, "判定:ファイル")
		// 完全一致の場合はS3オブジェクトであるのでファイルを返す
		return &fuse.Attr{
			Ino:    inodeHash(name),
			Size:   uint64(15), // TODO
			Blocks: 1,
			Mode:   fuse.S_IFREG | 0755,
			Owner: fuse.Owner{
				Uid: ctx.Owner.Uid,
				Gid: ctx.Owner.Gid,
			},
		}, fuse.OK
	}

	//log.Println("GetAttr name:", name, "判定:ディレクトリ")
	return &fuse.Attr{
		Ino:  inodeHash(name),
		Mode: fuse.S_IFDIR | 0755,
		Owner: fuse.Owner{
			Uid: ctx.Owner.Uid,
			Gid: ctx.Owner.Gid,
		},
	}, fuse.OK

}

func (f *FileSystem) Open(name string, flags uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	log.Println("Open name:", name)
	pos := Parse(name)

	get, err := f.sess.Get(pos.Bucket, pos.Key)
	if err != nil {
		return nil, fuse.ENOENT
	}

	log.Println("get object:", string(get))
	return &S3File{
		File:   nodefs.NewDataFile(get),
		bucket: pos.Bucket,
		key:    pos.Key,
		sess:   f.sess,
	}, fuse.OK
}

func (f *FileSystem) Rename(oldName string, newName string, _ *fuse.Context) fuse.Status {
	pos := Parse(oldName)
	destPos := Parse(newName)
	if pos.IsMountRoot || pos.IsBucketRoot || destPos.IsMountRoot || destPos.IsBucketRoot {
		return fuse.EPERM
	}

	if f.sess.Exists(destPos.Bucket, destPos.Key) {
		return fuse.EINVAL // TODO already exists
	}

	if !f.sess.Exists(pos.Bucket, pos.Key) {
		return fuse.ENOENT
	}

	get, err := f.sess.Get(pos.Bucket, pos.Key)
	if err != nil {
		return fuse.EIO
	}

	if err := f.sess.PutBytes(destPos.Bucket, destPos.Key, get); err != nil {
		return fuse.EIO
	}

	if err := f.sess.Delete(pos.Bucket, pos.Key); err != nil {
		return fuse.EIO
	}

	return fuse.OK
}

func (f *FileSystem) Mkdir(name string, mode uint32, ctx *fuse.Context) fuse.Status {
	// TODO S3でディレクトリの表現をマネジメントコンソールを見て確認
	return fuse.OK
}

func (f *FileSystem) Create(name string, flags uint32, mode uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	log.Printf("Create name:%s", name)

	pos := Parse(name)

	if f.sess.Exists(pos.Bucket, pos.Key) {
		return nil, fuse.EINVAL // TODO already existsを表現したい
	}

	if err := f.sess.PutBytes(pos.Bucket, pos.Key, make([]byte, 0)); err != nil {
		return nil, fuse.EIO
	}

	return &S3File{
		File:   nodefs.NewDevNullFile(),
		bucket: pos.Bucket,
		key:    pos.Key,
		sess:   f.sess,
	}, fuse.OK
}

func (f *FileSystem) OpenDir(name string, _ *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	pos := Parse(name)

	log.Printf("OpenDir name:%+v", pos)

	if pos.IsMountRoot {
		buckets, err := f.sess.ListBuckets()
		if err != nil {
			return nil, fuse.EIO
		}
		log.Println("mount root:", buckets)

		entries := make([]fuse.DirEntry, 0, len(buckets))
		for _, bucketName := range buckets {
			entries = append(entries, fuse.DirEntry{
				Name: bucketName,
				Ino:  inodeHash(bucketName),
			})
		}
		return entries, fuse.OK
	}

	objKeys, err := f.sess.List(pos.Bucket, pos.Key)
	if err != nil {
		return nil, fuse.EIO
	}

	m := make(map[string]fuse.DirEntry, len(objKeys))
	for _, objKey := range objKeys {
		dirName := strings.Split(objKey, string(filepath.Separator))[0]

		if !pos.IsBucketRoot && strings.HasPrefix(objKey, pos.Key) {
			dirName = NextParentPath(objKey, pos.Key)
		}

		log.Println("OpenDir objKey:", objKey, "dirName:", dirName, "currentPath:", pos.Key)
		m[dirName] = fuse.DirEntry{
			Name: dirName,
			Ino:  inodeHash(path.Join(name, dirName)),
		}
		continue
	}

	entries := make([]fuse.DirEntry, 0, len(objKeys))
	for _, v := range m {
		entries = append(entries, v)
	}
	return entries, fuse.OK
}

func (f *FileSystem) Access(name string, mode uint32, _ *fuse.Context) (code fuse.Status) {
	pos := Parse(name)
	log.Printf("Access pos:%+v\n", pos)

	if pos.IsMountRoot {
		return fuse.OK
	}

	if pos.IsBucketRoot {
		log.Println("is bucket root")
		if f.sess.ExistsBucket(pos.Bucket) {
			return fuse.OK
		}
		return fuse.ENOENT
	}

	list, err := f.sess.List(pos.Bucket, pos.Key)
	if err != nil {
		return fuse.EIO
	}
	if len(list) > 0 {
		return fuse.OK
	}

	return fuse.ENOENT
}

func (f *FileSystem) Unlink(name string, _ *fuse.Context) (code fuse.Status) {
	pos := Parse(name)
	log.Printf("Unlink pos:%+v\n", pos)

	if pos.IsMountRoot || pos.IsBucketRoot {
		return fuse.EPERM
	}

	if !f.sess.Exists(pos.Bucket, pos.Key) {
		return fuse.ENOENT
	}

	if err := f.sess.Delete(pos.Bucket, pos.Key); err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *FileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	pos := Parse(name)
	log.Println("Utimens pos:", pos)

	if f.sess.Exists(pos.Bucket, pos.Key) {
		return fuse.OK // TODO S3上のメタファイルを書き換え？

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
