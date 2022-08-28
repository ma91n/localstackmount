package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/ma91n/localstackmount/fs"
	"golang.org/x/exp/slices"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
)

const localStackEndpoint = "http://localhost:4566"

type Input struct {
	Region             string
	LocalStackEndpoint string
	Dir                string
	Debug              bool
}

func main() {
	dir, _ := os.UserHomeDir()

	c := Input{
		Region:             endpoints.ApNortheast1RegionID,
		LocalStackEndpoint: localStackEndpoint,
		Dir:                path.Join(dir, "mount", "localstack"),
		Debug:              false,
	}

	if os.Getenv("AWS_REGION") != "" {
		c.Region = os.Getenv("AWS_REGION")
	}

	if os.Getenv("LOCALSTACK_ENDPOINT") != "" {
		c.LocalStackEndpoint = os.Getenv("LOCALSTACK_ENDPOINT")
	}

	if err := mount(c); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}
}

func mount(c Input) error {

	// create mount point dir
	_ = os.MkdirAll(c.Dir, 0777)

	if err := doHealthCheck(c.LocalStackEndpoint); err != nil {
		return err
	}

	sess := fs.NewS3Session(c.Region, c.LocalStackEndpoint)

	fileSystem := fs.NewFileSystem(sess)

	opts := &nodefs.Options{
		Debug: c.Debug,
	}
	s, _, err := mountRoot(c.Dir, fileSystem.Root(), opts)
	if err != nil {
		return fmt.Errorf("nodefs mount root: %w", err)
	}
	defer s.Unmount()

	// ctrl + C
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		for {
			<-ch
			err := s.Unmount()
			if err == nil {
				log.Println("unmounted")
				break
			}
			fmt.Println("May be in use by another user")
			fmt.Print("unmount failed: ", err)
		}
	}()
	abs, _ := filepath.Abs(c.Dir)
	fmt.Println("mount start:", abs)

	s.Serve()
	return nil
}

func mountRoot(mountpoint string, root nodefs.Node, opts *nodefs.Options) (*fuse.Server, *nodefs.FileSystemConnector, error) {
	conn := nodefs.NewFileSystemConnector(root, opts)

	mountOpts := fuse.MountOptions{
		AllowOther: true, // TODO コマンドライン引数から取得
	}
	mountOpts.Options = append(mountOpts.Options, "nonempty") // TODO
	if opts != nil && opts.Debug {
		mountOpts.Debug = opts.Debug
	}
	s, err := fuse.NewServer(conn.RawFS(), mountpoint, &mountOpts)
	if err != nil {
		return nil, nil, err
	}
	return s, conn, nil
}

type Health struct {
	Services struct {
		S3 string `json:"s3"`
	} `json:"services"`
}

// LocalStackの起動チェック
func doHealthCheck(localStackEndpoint string) error {
	resp, err := http.Get(localStackEndpoint + "/health")
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
		return fmt.Errorf("localhost health check response is invalid :%v, status:%s body:%s", err, resp.Status, string(all))
	}

	if !slices.Contains([]string{"running", "available"}, body.Services.S3) {
		return fmt.Errorf("localstack s3 service is not running. response is %s", string(all))
	}
	return nil
}
