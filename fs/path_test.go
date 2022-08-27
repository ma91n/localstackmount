package fs

import "testing"

func TestMostParentPath(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{
				s: "/a1/a2/a3/a4.txt",
			},
			want: "a1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MostParentPath(tt.args.s); got != tt.want {
				t.Errorf("MostParentPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNextParentPath(t *testing.T) {
	type args struct {
		s      string
		prefix string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{
				s:      "/a1/a2/a3/a4.txt",
				prefix: "/a1",
			},
			want: "a2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NextParentPath(tt.args.s, tt.args.prefix); got != tt.want {
				t.Errorf("NextParentPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCanAccess(t *testing.T) {
	type args struct {
		list     []string
		destPath string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "OK move root dir",
			args: args{
				list:     []string{"/aaa/111/ccc", "aaa/222/ccc"},
				destPath: "/aaa",
			},
			want: true,
		},
		{
			name: "OK second dir",
			args: args{
				list:     []string{"aaa/111/ccc", "aaa/222/ccc"},
				destPath: "aaa/111",
			},
			want: true,
		},
		{
			name: "OK full path",
			args: args{
				list:     []string{"aaa/111/ccc", "aaa/222/ccc"},
				destPath: "aaa/111/ccc",
			},
			want: true,
		},
		{
			name: "NG partial path",
			args: args{
				list:     []string{"aaa/111/ccc", "aaa/222/ccc"},
				destPath: "aa",
			},
			want: false,
		},
		{
			name: "NG 2nd layer partial path",
			args: args{
				list:     []string{"aaa/111/ccc", "aaa/222/ccc"},
				destPath: "aaa/11",
			},
			want: false,
		},
		{
			name: "NG not found path",
			args: args{
				list:     []string{"aaa/111/ccc", "aaa/222/ccc"},
				destPath: "aaa/111/ddd",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanAccess(tt.args.list, tt.args.destPath); got != tt.want {
				t.Errorf("CanAccess() = %v, want %v", got, tt.want)
			}
		})
	}
}
