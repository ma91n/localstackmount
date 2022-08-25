package fs

import (
	"path"
	"path/filepath"
	"strings"
)

func MostParentPath(s string) string {
	split := strings.Split(s, string(filepath.Separator))

	for _, v := range split {
		if len(v) == 0 {
			continue
		}
		return v
	}

	return ""
}

func NextParentPath(s, prefix string) string {
	if !strings.HasPrefix(s, prefix) {
		return ""
	}

	child := strings.Split(s, prefix)[1]
	return MostParentPath(child)
}

type Position struct {
	IsMountRoot  bool
	IsBucketRoot bool
	Bucket       string
	Key          string
	OriginalPath string
}

func Parse(name string) Position {
	if name == "" || name == "." {
		return Position{
			IsMountRoot:  true,
			IsBucketRoot: false,
			Bucket:       "",
			Key:          "",
			OriginalPath: name,
		}
	}

	items := strings.Split(path.Clean(name), string(filepath.Separator))
	bucket, key := items[0], strings.Join(items[1:], string(filepath.Separator))

	return Position{
		IsMountRoot:  false,
		IsBucketRoot: key == "",
		Bucket:       bucket,
		Key:          key,
		OriginalPath: name,
	}
}
