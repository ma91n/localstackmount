package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/ma91n/localstackmount/lib"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
)

type Input struct {
	Region string
	Dir    string
}

func main() {
	dir, _ := os.UserHomeDir()
	c := Input{
		Region: endpoints.ApNortheast1RegionID,
		Dir:    path.Join(dir, "mount", "test38"),
	}

	if err := mount(c); err != nil {
		panic(err)
	}
}

func mount(c Input) error {

	// create mount point dir
	_ = os.MkdirAll(c.Dir, 755)

	if err := doHealthCheck(); err != nil {
		return err
	}

	sess := lib.NewS3Session(c.Region)

	fs := lib.NewFileSystem(sess)

	s, _, err := nodefs.MountRoot(c.Dir, fs.Root(), nil)
	if err != nil {
		return err
	}
	defer s.Unmount()

	// ctrl + C
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		for {
			<-ch
			err := s.Unmount()
			if err == nil {
				log.Println("unmounted")
				break
			}
			log.Print("unmount failed: ", err)
		}
	}()

	fmt.Println("mount start")
	s.Serve()
	return nil
}

type Health struct {
	Services struct {
		S3 string `json:"s3"`
	} `json:"services"`
}

// LocalStackの起動チェック
func doHealthCheck() error {
	resp, err := http.Get("http://localhost:4566/health")
	if err != nil {
		return fmt.Errorf("localhost is not running? :%v", err)
	}
	defer resp.Body.Close()

	all, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("localhost health check read body? :%v", err)
	}

	var body Health
	if err := json.Unmarshal(all, &body); err != nil {
		return fmt.Errorf("localhost health check response is invalid :%v, %s", err, string(all))
	}

	if body.Services.S3 != "running" {
		return fmt.Errorf("localstack s3 service is not running. response is %s", string(all))
	}
	return nil
}
