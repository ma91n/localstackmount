package lib

import (
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
