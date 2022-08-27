package fs

import (
	"golang.org/x/exp/slices"
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

func DirCombination(keyPath string) []string {
	split := strings.Split(keyPath, string(filepath.Separator))

	resp := make([]string, 0, len(split)+1)
	resp = append(resp, "") // Bucketルートパスを示す

	for i := range split {
		resp = append(resp, path.Join(split[:i+1]...))
	}
	return resp
}

type Position struct {
	IsMountRoot  bool
	IsBucketRoot bool
	Bucket       string
	Key          string
	OriginalPath string
}

func Parse(name string) Position {
	if slices.Contains([]string{"", "."}, path.Clean(name)) {
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

func CanAccess(list []string, destPath string) bool {
	destSplit := strings.Split(destPath, string(filepath.Separator))
	for _, v := range list {
		if !strings.HasPrefix(v, destPath) {
			continue
		}

		split := strings.Split(v, string(filepath.Separator))

		if len(destSplit) > len(split) {
			continue
		}

		for i := range destSplit {
			if destSplit[i] != split[i] {
				break
			}
			if i == len(destSplit)-1 {
				return true // 完全一致した場合は移動可能
			}
		}
	}
	return false
}
