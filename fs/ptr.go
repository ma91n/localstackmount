package fs

import "time"

func timePtr(t time.Time) *time.Time {
	return &t
}
