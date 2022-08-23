package lib

import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash/fnv"
	"log"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	uuid "github.com/satori/go.uuid"
)

type FileSystem struct {
	pathfs.FileSystem

	Sess *S3Session
}

func NewFileSystem(sess *S3Session) *pathfs.PathNodeFs {
	return pathfs.NewPathNodeFs(&FileSystem{
		FileSystem: pathfs.NewDefaultFileSystem(),
		Sess:       sess,
	}, nil)
}

func (f *FileSystem) GetAttr(name string, ctx *fuse.Context) (*fuse.Attr, fuse.Status) {
	log.Printf("GetAttr name:%s", name)

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

	list, err := f.Sess.List(bucket, currentPath)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if len(list) == 0 {
		return nil, fuse.ENOENT
	}

	log.Println("GetAttr name:", name, "list:", list)

	if list[0] == currentPath {
		log.Println("GetAttr name:", name, "判定:ファイル")
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
		log.Println("GetAttr name:", name, "判定:ディレクトリ")
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

	get, err := f.Sess.Get(bucket, currentPath)
	if err != nil {
		log.Println("get err:", err)
		return nil, fuse.ENOENT
	}
	log.Println("get object:", string(get))

	return nodefs.NewDataFile(get), fuse.OK
}

//func (f *FileSystem) getParent(name string) (*Directory, fuse.Status) {
//	parent := filepath.Dir(name)
//	key, err := f.Sess.PathWalk(parent)
//	if err != nil {
//		return nil, fuse.ENOENT
//	}
//
//	dir, err := f.Sess.NewDirectory(key)
//	if err != nil {
//		return nil, fuse.EACCES
//	}
//
//	return dir, fuse.OK
//}

func (f *FileSystem) Rename(oldName string, newName string, _ *fuse.Context) fuse.Status {
	return fuse.EIO
}

func (f *FileSystem) Mkdir(name string, mode uint32, ctx *fuse.Context) fuse.Status {
	return fuse.OK
	//dir, status := f.getParent(name)
	//if status != fuse.OK {
	//	return status
	//}
	//
	//// Set
	//newKey := generateObjectKey()
	//dir.FileMeta[filepath.Base(name)] = newKey
	//
	//newDir := f.Sess.CreateDirectory(newKey, mode, ctx)
	//
	//// Save
	//if err := newDir.Save(); err != nil {
	//	return fuse.EIO
	//}
	//
	//if err := dir.Save(); err != nil {
	//	return fuse.EIO
	//}
	//
	//return fuse.OK
}

func (f *FileSystem) Symlink(value string, linkName string, ctx *fuse.Context) fuse.Status {
	return fuse.EIO
}

func (f *FileSystem) Create(name string, flags uint32, mode uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	return nil, fuse.OK
	//// TODO: flags??
	//dir, status := f.getParent(name)
	//if status != fuse.OK {
	//	return nil, status
	//}
	//
	//// Set
	//newKey := generateObjectKey()
	//dir.FileMeta[filepath.Base(name)] = newKey
	//
	//file := f.Sess.CreateFile(newKey, dir.Key, mode, ctx)
	//
	//if err := file.Save(); err != nil {
	//	return nil, fuse.EIO
	//}
	//
	//if err := dir.Save(); err != nil {
	//	return nil, fuse.EIO
	//}
	//
	//return NewOpenedFile(file), fuse.OK
}

func (f *FileSystem) OpenDir(name string, _ *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	log.Printf("OpenDir name:%s", name)

	if len(name) == 0 {
		// Rootの場合、バケット名を返す
		buckets, err := f.Sess.ListBuckets()
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

	objKeys, err := f.Sess.List(bucket, currentPath)
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

	return entries, fuse.OK
}

func (f *FileSystem) OnMount(_ *pathfs.PathNodeFs) {}

func (f *FileSystem) OnUnmount() {}

func (f *FileSystem) Chmod(name string, mode uint32, _ *fuse.Context) (code fuse.Status) {
	log.Println("Chmod")
	return fuse.EIO
}

func (f *FileSystem) Chown(name string, uid uint32, gid uint32, _ *fuse.Context) fuse.Status {
	log.Println("Chown")

	return fuse.EIO
}

func (f *FileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time, _ *fuse.Context) fuse.Status {
	log.Println("Utimens")

	return fuse.EIO
}

func (f *FileSystem) Access(name string, mode uint32, _ *fuse.Context) (code fuse.Status) {
	log.Printf("Access: name:%s\n", name) // local-test/2022/08/22/put1.txt

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, key := items[0], strings.Join(items[1:], string(filepath.Separator))

	list, err := f.Sess.List(bucket, key)
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

	//key, err := f.Sess.PathWalk(name)
	//if err != nil {
	//	return fuse.ENOENT
	//}
	//
	//node, err := f.Sess.NewFile(key)
	//if err != nil {
	//	return fuse.ENOENT
	//}
	//
	//node.Meta.Size = int64(size)
	//if err = node.Save(); err != nil {
	//	return fuse.EIO
	//}
	//
	return fuse.OK
}

func (f *FileSystem) Readlink(_ string, _ *fuse.Context) (string, fuse.Status) {
	log.Println("Readlink")
	return "", fuse.ENOENT
}

func (f *FileSystem) Rmdir(name string, ctx *fuse.Context) (code fuse.Status) {
	return f.Unlink(name, ctx)
}

func (f *FileSystem) Unlink(name string, _ *fuse.Context) (code fuse.Status) {
	log.Println("Unlink")
	return fuse.EIO
}

func (f *FileSystem) String() string {
	return "localstackmount"
}

func (f *FileSystem) GetXAttr(name string, attribute string, ctx *fuse.Context) (data []byte, code fuse.Status) {
	return nil, fuse.EIO
}

func (f *FileSystem) ListXAttr(name string, context *fuse.Context) (attributes []string, code fuse.Status) {
	return nil, fuse.EIO
}

func (f *FileSystem) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	return fuse.EIO
}

func (f *FileSystem) SetXAttr(name string, attr string, data []byte, flags int, ctx *fuse.Context) fuse.Status {
	return fuse.EIO
}

func (f *FileSystem) Link(oldName string, newName string, ctx *fuse.Context) (code fuse.Status) {
	return fuse.EIO
}

func (f *FileSystem) Mknod(name string, mode uint32, dev uint32, ctx *fuse.Context) fuse.Status {
	return fuse.EIO
}

func (f *FileSystem) StatFs(name string) *fuse.StatfsOut {
	return nil
}

func (f *FileSystem) SetDebug(debug bool) {}

func inodeHash(o string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(o))
	return h.Sum64()
}

func generateObjectKey() string {
	return uuid.NewV4().String()
}

func keyGen(input []byte) string {
	return fmt.Sprintf("%x", murmur3.Sum64(input))
}

func rootKey() string {
	return keyGen([]byte("localstackmount"))
}
