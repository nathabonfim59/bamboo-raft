package main

import (
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node"
)

// A10 D-a: -node.jail-tool-binary (BBRAFT_NODE_JAIL_TOOL_BINARY) names the
// trusted qemu-img the jailer stages for image tools, with the same
// trusted-path rules as -node.jail-qemu-binary.

func TestJailToolBinaryDefaultMatchesNode(t *testing.T) {
	if defaultJailToolBinary != node.DefaultJailToolBinary {
		t.Fatalf("cmd default %q, node default %q", defaultJailToolBinary, node.DefaultJailToolBinary)
	}
}

func TestJailToolBinaryWiring(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"default", nil, defaultJailToolBinary},
		{"flag", []string{"-node.jail-tool-binary=/opt/qemu-10.0.13/bin/qemu-img"}, "/opt/qemu-10.0.13/bin/qemu-img"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newJailFixture(t)
			f.args = append(f.args, tc.args...)
			if err := f.run(t); err != nil {
				t.Fatal(err)
			}
			if got := f.s.nodeCfg.JailToolBinary; got != tc.want {
				t.Fatalf("node.Config.JailToolBinary = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestJailToolBinaryEnvAndRefusals(t *testing.T) {
	hostname := func() (string, error) { return "h", nil }
	base := append(tapNetworkArgs(), jailArgs(t.TempDir(), t.TempDir())...)
	t.Run("env", func(t *testing.T) {
		o, err := parseFlags(base, io.Discard, testEnv(map[string]string{"BBRAFT_NODE_JAIL_TOOL_BINARY": "/opt/q/qemu-img"}), hostname)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := o.jail()
		if err != nil || plan.ToolBinary != "/opt/q/qemu-img" {
			t.Fatalf("plan %+v, %v; want the env tool binary", plan, err)
		}
	})
	for name, value := range map[string]string{"relative": "qemu-img", "unclean": "/usr/bin/../bin/qemu-img", "empty": ""} {
		t.Run(name, func(t *testing.T) {
			o, err := parseFlags(append(slices.Clone(base), "-node.jail-tool-binary="+value), io.Discard, testEnv(nil), hostname)
			if err == nil {
				_, err = o.jail()
			}
			if err == nil || !strings.Contains(err.Error(), "jail-tool-binary") {
				t.Fatalf("accepted -node.jail-tool-binary=%q: %v", value, err)
			}
		})
	}
	for name, tc := range map[string]struct {
		args []string
		env  map[string]string
	}{
		"user mode flag": {[]string{"-node.network-mode=user", "-node.jail-tool-binary=" + defaultJailToolBinary}, nil},
		"user mode env":  {[]string{"-node.network-mode=user"}, map[string]string{"BBRAFT_NODE_JAIL_TOOL_BINARY": defaultJailToolBinary}},
	} {
		t.Run(name, func(t *testing.T) {
			o, err := parseFlags(tc.args, io.Discard, testEnv(tc.env), hostname)
			if err == nil {
				_, err = o.jail()
			}
			if err == nil || !strings.Contains(err.Error(), "jail-tool-binary") {
				t.Fatalf("user mode accepted a supplied tool binary: %v", err)
			}
		})
	}
}
