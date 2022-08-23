package lib

import (
	"time"

	"github.com/hanwen/go-fuse/fuse"
)

// Meta is common struct for directory, file and symlink
type Meta struct {
	Size  int64     `json:"size"`
	Mode  uint32    `json:"mode"`
	UID   uint32    `json:"uid"`
	GID   uint32    `json:"gid"`
	Atime time.Time `json:"atime"`
	Ctime time.Time `json:"ctime"`
	Mtime time.Time `json:"mtime"`
}

// Node is common part of Directory, File, and SymLink
type Node struct {
	Key  string `json:"key"`
	Meta Meta   `json:"meta"`
}

type Directory struct {
	Key      string            `json:"key"`
	Meta     Meta              `json:"meta"`
	FileMeta map[string]string `json:"children"`
	sess     *S3Session
}

func (o *Directory) Save() error {
	//result, err := json.Marshal(o)
	//if err != nil {
	//	return err
	//}
	//
	//return o.sess.Upload(o.Key, bytes.NewReader(result))
	return nil
}

type File struct {
	Key        string            `json:"key"`
	Meta       Meta              `json:"meta"`
	ExtentSize int64             `json:"extent_size"`
	Extent     map[int64]*Extent `json:"extent"`
	sess       *S3Session
	dirty      bool
}

func (o *File) Save() error {
	//wg := sync.WaitGroup{}
	//errc := make(chan error)
	//done := make(chan struct{})
	//for _, e := range o.Extent {
	//	wg.Add(1)
	//	go func(e *Extent) {
	//		if !e.dirty {
	//			wg.Done()
	//			return
	//		}
	//		key := e.CurrentKey()
	//		if o.sess.Exists(key) {
	//			wg.Done()
	//			return
	//		}
	//		err := o.sess.Upload(key, bytes.NewReader(e.body))
	//		if err != nil {
	//			errc <- err
	//			return
	//		}
	//		e.dirty = false
	//		wg.Done()
	//	}(e)
	//}
	//go func() {
	//	wg.Wait()
	//	close(done)
	//}()
	//
	//select {
	//case err := <-errc:
	//	return err
	//case <-done:
	//	marshal, err := json.Marshal(o)
	//	if err != nil {
	//		return err
	//	}
	//
	//	return o.sess.Upload(o.Key, bytes.NewReader(marshal))
	//}

	return nil
}

type Extent struct {
	Key   string `json:"key"`
	body  []byte // call Fill() to use this
	dirty bool
	sess  *S3Session
}

func (e *Extent) CurrentKey() string {
	return keyGen(e.body)
}

func (e *Extent) Fill() error {
	//if e.dirty || len(e.body) != 0 {
	//	return nil
	//}
	//
	//body, err := e.sess.Get(e.Key)
	//if err != nil {
	//	return err
	//}
	//
	//e.body = body
	return nil
}

func NewMeta(mode uint32, ctx *fuse.Context) Meta {
	return Meta{
		Mode:  mode,
		Size:  0,
		UID:   ctx.Uid,
		GID:   ctx.Gid,
		Atime: time.Now(),
		Ctime: time.Now(),
		Mtime: time.Now(),
	}
}
