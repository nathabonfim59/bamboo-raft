package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A10 slice4 fix round 1: explicit user mode refuses every supplied jail
// setting, the host proofs require a root supervisor and a root-owned,
// non-writable delegated hierarchy, all before any mutation.

func TestUserModeRejectsSuppliedJailSettings(t *testing.T) {
	hostname := func() (string, error) { return "h", nil }
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"flag default binary", []string{"-node.jail-qemu-binary=" + defaultJailQEMUBinary}, nil},
		{"flag zero rate", []string{"-node.jail-io-write-iops=0"}, nil},
		{"flag empty firmware", []string{"-node.jail-firmware="}, nil},
		{"env default binary", nil, map[string]string{"BBRAFT_NODE_JAIL_QEMU_BINARY": defaultJailQEMUBinary}},
		{"env zero rate", nil, map[string]string{"BBRAFT_NODE_JAIL_IO_READ_BPS": "0"}},
		{"env empty cgroup", nil, map[string]string{"BBRAFT_NODE_JAIL_CGROUP": ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"BBRAFT_NODE_NETWORK_MODE": networkModeUser}
			for k, v := range tc.env {
				env[k] = v
			}
			o, err := parseFlags(tc.args, io.Discard, testEnv(env), hostname)
			if err != nil {
				t.Fatal(err)
			}
			if plan, err := o.jail(); err == nil {
				t.Fatalf("user mode accepted a supplied jail setting (plan %+v)", plan)
			}
		})
	}
}

func TestCheckRootSupervisor(t *testing.T) {
	if err := checkRootSupervisor(0, 0, 0, 0); err != nil {
		t.Fatalf("root supervisor refused: %v", err)
	}
	for name, ids := range map[string][4]int{
		"uid":  {1000, 0, 0, 0},
		"euid": {0, 1000, 0, 0},
		"gid":  {0, 0, 1000, 0},
		"egid": {0, 0, 0, 1000},
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkRootSupervisor(ids[0], ids[1], ids[2], ids[3]); err == nil {
				t.Fatal("a non-root supervisor was accepted")
			}
		})
	}
}

// TestRealJailHostRefusesNonRootFirst: as a non-root user the real host
// proof fails on the supervisor identity before reading anything else.
func TestRealJailHostRefusesNonRootFirst(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root")
	}
	_, err := realJailHost(t.Context(), &jailPlan{}, t.TempDir(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "root supervisor") {
		t.Fatalf("realJailHost = %v, want the root-supervisor refusal", err)
	}
}

// jailDelegationOwnerEnv is a good delegation env owned by the test user.
func jailDelegationOwnerEnv(root, service string) delegationEnv {
	return delegationEnv{
		CgroupRoot: root, FSType: cgroup2SuperMagic,
		SelfCgroup: []byte("0::/system.slice/bbraft-node.service/supervisor\n"),
		SelfNS:     4026531835, InitNS: 4026531835,
		Xattr: func(path, name string) (string, error) {
			if path == service && name == "trusted.delegate" {
				return "1", nil
			}
			return "", errors.New("no xattr")
		},
		JailCgroup: service + "/vms",
		Owner:      uint32(os.Getuid()),
	}
}

func TestCheckDelegationProvesHierarchyOwnership(t *testing.T) {
	root, service := jailCgroupTree(t)
	if _, err := checkDelegation(jailDelegationOwnerEnv(root, service)); err != nil {
		t.Fatalf("own hierarchy refused: %v", err)
	}
	for name, edit := range map[string]func(t *testing.T, e *delegationEnv, service string){
		"service owned by another uid": func(_ *testing.T, e *delegationEnv, _ string) { e.Owner++ },
		"service group-writable": func(t *testing.T, _ *delegationEnv, service string) {
			if err := os.Chmod(service, 0o775); err != nil {
				t.Fatal(err)
			}
		},
		"existing vms world-writable": func(t *testing.T, _ *delegationEnv, service string) {
			if err := os.Mkdir(filepath.Join(service, "vms"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(service, "vms"), 0o777); err != nil {
				t.Fatal(err)
			}
		},
		"service is a symlink": func(t *testing.T, e *delegationEnv, service string) {
			real := service + ".real"
			if err := os.Rename(service, real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, service); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			root, service := jailCgroupTree(t)
			env := jailDelegationOwnerEnv(root, service)
			edit(t, &env, service)
			if _, err := checkDelegation(env); err == nil {
				t.Fatalf("checkDelegation accepted %s", name)
			}
		})
	}
}

func TestPrepareJailCgroupReprovesOwnership(t *testing.T) {
	owner := uint32(os.Getuid())
	for name, edit := range map[string]func(t *testing.T, service string) uint32{
		"service owned by another uid": func(*testing.T, string) uint32 { return owner + 1 },
		"service group-writable": func(t *testing.T, service string) uint32 {
			if err := os.Chmod(service, 0o775); err != nil {
				t.Fatal(err)
			}
			return owner
		},
		"service replaced by a symlink": func(t *testing.T, service string) uint32 {
			if err := os.Rename(service, service+".real"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(service+".real", service); err != nil {
				t.Fatal(err)
			}
			return owner
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, service := jailCgroupTree(t)
			want := edit(t, service)
			if err := prepareJailCgroupAs(t.Context(), service, service+"/vms", want); err == nil {
				t.Fatalf("prepareJailCgroup accepted %s", name)
			}
			for _, dir := range []string{service, service + ".real"} {
				raw, err := os.ReadFile(filepath.Join(dir, "cgroup.subtree_control"))
				if err == nil && len(raw) != 0 {
					t.Fatal("controllers were enabled on an unproven hierarchy")
				}
				if _, err := os.Lstat(filepath.Join(dir, "vms")); err == nil {
					t.Fatal("vms was created in an unproven hierarchy")
				}
			}
		})
	}
}
