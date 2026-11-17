package ids

import (
	"regexp"
	"testing"
)

var sandboxRE = regexp.MustCompile(`^bx_[23456789abcdefghjkmnpqrstuvwxyz]{8}$`)

func TestSandbox(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := Sandbox()
		if !sandboxRE.MatchString(id) || !ValidSandbox(id) {
			t.Fatalf("bad id %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestValidSandbox(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"bx_23456789", true},
		{"bx_2345678", false},
		{"bx_234567890", false},
		{"bx_2345678o", false},
		{"bx_2345678A", false},
		{"by_23456789", false},
		{"", false},
	} {
		if got := ValidSandbox(tc.in); got != tc.want {
			t.Errorf("ValidSandbox(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
