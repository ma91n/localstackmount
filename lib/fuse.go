package lib

import (
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

func InodeHash(o string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(o))
	return h.Sum64()
}

func (f *FileSystem) GetAttr(name string, _ *fuse.Context) (*fuse.Attr, fuse.Status) {
	log.Printf("GetAttr call: name:%s", name)

	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return nil, fuse.ENOENT
	}

	node, err := f.Sess.NewNode(key)
	if err != nil {
		return nil, fuse.ENOENT
	}

	attr := &fuse.Attr{
		Ino:   InodeHash(key),
		Size:  uint64(node.Meta.Size),
		Mode:  node.Meta.Mode,
		Nlink: 1,
		Owner: fuse.Owner{
			Uid: node.Meta.UID,
			Gid: node.Meta.GID,
		},
	}
	attr.SetTimes(&node.Meta.Atime, &node.Meta.Mtime, &node.Meta.Ctime)

	return attr, fuse.OK
}

func (f *FileSystem) Open(name string, flags uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return nil, fuse.ENOENT
	}

	node, err := f.Sess.NewFile(key)
	if err != nil {
		return nil, fuse.ENOENT
	}

	return NewOpenedFile(node), fuse.OK
}

func (f *FileSystem) getParent(name string) (*Directory, fuse.Status) {
	parent := filepath.Dir(name)
	key, err := f.Sess.PathWalk(parent)
	if err != nil {
		return nil, fuse.ENOENT
	}

	dir, err := f.Sess.NewDirectory(key)
	if err != nil {
		return nil, fuse.EACCES
	}

	return dir, fuse.OK
}

func (f *FileSystem) Rename(oldName string, newName string, _ *fuse.Context) fuse.Status {
	return fuse.EIO
}

func (f *FileSystem) Mkdir(name string, mode uint32, ctx *fuse.Context) fuse.Status {
	dir, status := f.getParent(name)
	if status != fuse.OK {
		return status
	}

	// Set
	newKey := generateObjectKey()
	dir.FileMeta[filepath.Base(name)] = newKey

	newDir := f.Sess.CreateDirectory(newKey, dir.Key, mode, ctx)

	// Save
	if err := newDir.Save(); err != nil {
		return fuse.EIO
	}

	if err := dir.Save(); err != nil {
		return fuse.EIO
	}

	return fuse.OK
}

func (f *FileSystem) Symlink(value string, linkName string, ctx *fuse.Context) fuse.Status {
	return fuse.EIO
}

func (f *FileSystem) Create(name string, flags uint32, mode uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	// TODO: flags??
	dir, status := f.getParent(name)
	if status != fuse.OK {
		return nil, status
	}

	// Set
	newKey := generateObjectKey()
	dir.FileMeta[filepath.Base(name)] = newKey

	file := f.Sess.CreateFile(newKey, dir.Key, mode, ctx)

	if err := file.Save(); err != nil {
		return nil, fuse.EIO
	}

	if err := dir.Save(); err != nil {
		return nil, fuse.EIO
	}

	return NewOpenedFile(file), fuse.OK
}

func (f *FileSystem) OpenDir(name string, _ *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return nil, fuse.ENOENT
	}

	dir, err := f.Sess.NewDirectory(key)
	if err != nil {
		return nil, fuse.ENOENT
	}

	entries := make([]fuse.DirEntry, 0, len(dir.FileMeta))
	for name, key := range dir.FileMeta {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  InodeHash(key),
		})
	}

	// TODO
	keys, err := f.Sess.List(name)
	if err != nil {
		return nil, fuse.ENOENT
	}

	log.Println("keys", keys)

	for _, k := range keys {
		if name == "" {
			//TODO なければディレクトリを作成？
			entries = append(entries, fuse.DirEntry{
				Name: strings.Split(k, string(filepath.Separator))[0],
				Ino:  InodeHash(k),
			})
			continue
		}

		//TODO なければディレクトリを作成？

		base := path.Base(strings.Split(k, name)[1])
		entries = append(entries, fuse.DirEntry{
			Name: base,
			Ino:  InodeHash(base),
		})
	}

	return entries, fuse.OK
}

func (f *FileSystem) OnMount(_ *pathfs.PathNodeFs) {}

func (f *FileSystem) OnUnmount() {}

func (f *FileSystem) Chmod(name string, mode uint32, _ *fuse.Context) (code fuse.Status) {
	return fuse.EIO
}

func (f *FileSystem) Chown(name string, uid uint32, gid uint32, _ *fuse.Context) fuse.Status {
	return fuse.EIO
}

func (f *FileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time, _ *fuse.Context) fuse.Status {
	log.Printf("Utimens")

	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return fuse.ENOENT
	}

	node, err := f.Sess.NewTypedNode(key)
	if err != nil {
		return fuse.ENOENT
	}

	switch typed := node.(type) {
	case *Directory:
		typed.Meta.Atime = *Atime
		typed.Meta.Mtime = *Mtime
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	case *File:
		typed.Meta.Atime = *Atime
		typed.Meta.Mtime = *Mtime
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	case *SymLink:
		typed.Meta.Atime = *Atime
		typed.Meta.Mtime = *Mtime
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	}
	if err != nil {
		return fuse.EIO
	}

	return fuse.OK

}

func (f *FileSystem) Access(name string, mode uint32, _ *fuse.Context) (code fuse.Status) {
	log.Printf("Access")

	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return fuse.ENOENT
	}

	if f.Sess.Exists(key) {
		return fuse.OK
	}
	return fuse.ENOENT
}

func (f *FileSystem) Truncate(name string, size uint64, _ *fuse.Context) (code fuse.Status) {
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return fuse.ENOENT
	}

	node, err := f.Sess.NewFile(key)
	if err != nil {
		return fuse.ENOENT
	}

	node.Meta.Size = int64(size)
	if err = node.Save(); err != nil {
		return fuse.EIO
	}

	return fuse.OK
}

func (f *FileSystem) Readlink(_ string, _ *fuse.Context) (string, fuse.Status) {
	return "", fuse.ENOENT
}

func (f *FileSystem) Rmdir(name string, ctx *fuse.Context) (code fuse.Status) {
	return f.Unlink(name, ctx)
}

func (f *FileSystem) Unlink(name string, _ *fuse.Context) (code fuse.Status) {
	return fuse.EIO
}

func (f *FileSystem) String() string {
	return "localstackmount"
}

func generateObjectKey() string {
	return uuid.NewV4().String()
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
