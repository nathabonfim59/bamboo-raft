package objstore_test

import (
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
)

func TestValidKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"chunks/ab12", true},
		{"manifests/0190c3a4-0000-7000-8000-000000000000/3.json.zst", true},
		{"a", true},
		{"a.b/c..d/.e", true},
		{"", false},
		{"/a", false},
		{"a/", false},
		{"a//b", false},
		{"./a", false},
		{"a/..", false},
		{"a/../b", false},
		{"a\\b", false},
		{"a\x00b", false},
		{"a\nb", false},
		{"a\x7fb", false},
		{strings.Repeat("k", objstore.MaxKeyLen), true},
		{strings.Repeat("k", objstore.MaxKeyLen+1), false},
	}
	for _, c := range cases {
		if got := objstore.ValidKey(c.key); got != c.want {
			t.Errorf("ValidKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}
