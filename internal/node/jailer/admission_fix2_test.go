package jailer

import "testing"

// A10 slice4 fix round 1: the admission mount proof follows mount
// id/parent topology, never listing order.
func TestAdmissionMountTopology(t *testing.T) {
	const target = "/state/admission/x.d/mnt"
	line := func(id, parent, point, opts, optional string) string {
		return id + " " + parent + " 0:52 / " + point + " " + opts + " " + optional + "- tmpfs tmpfs rw\n"
	}
	const good = "rw,nosuid,nodev,noexec"
	cases := map[string]struct {
		info string
		ok   bool
	}{
		"private top over shared, top first": {line("20", "10", target, good, "") + line("10", "1", target, good, "shared:3 "), true},
		"private top over shared, top last":  {line("10", "1", target, good, "shared:3 ") + line("20", "10", target, good, ""), true},
		"shared top, top first":              {line("20", "10", target, good, "shared:8 ") + line("10", "1", target, good, ""), false},
		"shared top, top last":               {line("10", "1", target, good, "") + line("20", "10", target, good, "shared:8 "), false},
		"slave top":                          {line("10", "1", target, good, "master:2 "), false},
		"top lacks nodev":                    {line("10", "1", target, good, "") + line("20", "10", target, "rw,nosuid,noexec", ""), false},
		"branch: two effective mounts":       {line("10", "1", target, good, "") + line("20", "1", target, good, ""), false},
		"cycle":                              {line("10", "20", target, good, "") + line("20", "10", target, good, ""), false},
		"duplicate id":                       {line("10", "1", target, good, "") + line("10", "1", "/other", good, ""), false},
		"absent":                             {line("10", "1", "/other", good, ""), false},
		"malformed id":                       {line("x", "1", target, good, ""), false},
		"no separator":                       {"10 1 0:52 / " + target + " " + good + "\n", false},
		"escaped mount point":                {line("10", "1", `/state/a\040b/mnt`, good, ""), true},
		"bad escape":                         {line("10", "1", `/state/a\04b/mnt`, good, ""), false},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			want := target
			if name == "escaped mount point" || name == "bad escape" {
				want = "/state/a b/mnt"
			}
			err := checkAdmissionMount([]byte(tt.info), want)
			if (err == nil) != tt.ok {
				t.Fatalf("checkAdmissionMount = %v, want ok %v", err, tt.ok)
			}
		})
	}
}
