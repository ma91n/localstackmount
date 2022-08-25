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
