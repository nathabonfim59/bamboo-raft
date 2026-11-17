package jailer

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// jailerVarTempDir is a temp dir outside /tmp, which the jail reserves for
// its own private tmpfs and therefore refuses as a runtime path.
func jailerVarTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/var/tmp", "bbraft-jailer-test-")
	if err != nil {
		t.Skipf("no /var/tmp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func TestResolveRuntime(t *testing.T) {
	jailerNeedBinary(t)
	cfg := Config{}.withDefaults()
	files, err := resolveRuntime(t.Context(), jailerTestBinary, cfg.LibraryDirs, []string{"/etc/passwd"})
	if err != nil {
		t.Fatal(err)
	}
	if files[0].target != strings.TrimPrefix(jailerTestBinary, "/") {
		t.Fatalf("binary staged at /%s, want its declared path", files[0].target)
	}
	interpreter, _, err := elfDependencies(files[0].source)
	if err != nil {
		t.Fatal(err)
	}
	targets := make([]string, 0, len(files))
	for _, f := range files {
		targets = append(targets, f.target)
	}
	if interpreter != "" && !slices.Contains(targets, strings.TrimPrefix(interpreter, "/")) {
		t.Fatalf("interpreter %s not staged at its PT_INTERP path: %v", interpreter, targets)
	}
	if !slices.ContainsFunc(targets, func(s string) bool { return strings.Contains(s, "libc.so") }) {
		t.Fatalf("libc not resolved: %v", targets)
	}
	if targets[len(targets)-1] != "etc/passwd" {
		t.Fatalf("firmware not staged last at its declared path: %v", targets)
	}
	if len(slices.Compact(slices.Sorted(slices.Values(targets)))) != len(targets) {
		t.Fatalf("duplicate targets: %v", targets)
	}
}

func TestResolveRuntimeRejects(t *testing.T) {
	jailerNeedBinary(t)
	cfg := Config{}.withDefaults()
	notELF := filepath.Join(jailerVarTempDir(t), "qemu")
	if err := os.WriteFile(notELF, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		binary   string
		libs     []string
		firmware []string
		reason   string
	}{
		{name: "missing binary", binary: "/nonexistent/qemu", libs: cfg.LibraryDirs, reason: "resolve runtime"},
		{name: "not an ELF", binary: notELF, libs: cfg.LibraryDirs, reason: "parse ELF"},
		{name: "dependency outside trusted dirs", binary: jailerTestBinary, libs: []string{t.TempDir()}, reason: "not found in trusted library dirs"},
		{name: "directory firmware", binary: jailerTestBinary, libs: cfg.LibraryDirs, firmware: []string{"/etc"}, reason: "not a regular file"},
		{name: "missing firmware", binary: jailerTestBinary, libs: cfg.LibraryDirs, firmware: []string{"/nonexistent/bios.bin"}, reason: "firmware"},
		{name: "firmware over jail mount", binary: jailerTestBinary, libs: cfg.LibraryDirs, firmware: []string{"/dev/null"}, reason: "collides"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolveRuntime(t.Context(), tt.binary, tt.libs, tt.firmware)
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("resolveRuntime = %v, want %q", err, tt.reason)
			}
		})
	}
}

func TestConfigValidateFirmware(t *testing.T) {
	many := make([]string, maxFirmwareFiles+1)
	for i := range many {
		many[i] = fmt.Sprintf("/usr/share/qemu/f%d.bin", i)
	}
	tests := []struct {
		name     string
		firmware []string
		reason   string
	}{
		{name: "explicit list", firmware: []string{"/usr/share/qemu/bios-256k.bin", "/usr/share/qemu/efi-virtio.rom"}},
		{name: "relative", firmware: []string{"bios.bin"}, reason: "absolute clean"},
		{name: "unclean", firmware: []string{"/usr/share/qemu/../../etc/shadow"}, reason: "absolute clean"},
		{name: "duplicate", firmware: []string{"/usr/share/qemu/a.bin", "/usr/share/qemu/a.bin"}, reason: "duplicate"},
		{name: "reserved jail path", firmware: []string{"/control/qmp.sock"}, reason: "collides"},
		{name: "too many", firmware: many, reason: "exceed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{StateDir: "/var/lib/bbraft/jailer", CgroupParent: "/sys/fs/cgroup/bbraft", UIDs: UIDRange{Min: 1, Max: 2}, FirmwareFiles: tt.firmware, Reclaim: jailerNoReclaim}
			err := cfg.Validate()
			if tt.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("Validate = %v, want %q", err, tt.reason)
			}
		})
	}
}

// TestStartStagesFirmware copies a declared trusted file read-only at the
// same path in the chroot, and refuses an untrusted (non-root-owned) one
// before any process exists.
func TestStartStagesFirmware(t *testing.T) {
	jailerNeedBinary(t)
	t.Run("trusted", func(t *testing.T) {
		env := newJailerEnv(t)
		env.cfg.FirmwareFiles = []string{"/etc/passwd"}
		m := env.open(t)
		defer m.Close()
		child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
		if err != nil {
			t.Fatal(err)
		}
		staged := filepath.Join(env.cfg.StateDir, jailsDir, jailerLaunchID, "root", "etc", "passwd")
		got, err := os.ReadFile(staged)
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadFile("/etc/passwd")
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(staged)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) || info.Mode().Perm() != 0o555 {
			t.Fatalf("firmware copy mode %v, content equal %v", info.Mode().Perm(), bytes.Equal(got, want))
		}
		if err := child.Kill(); err != nil {
			t.Fatal(err)
		}
		owner.waitKilled(t)
		if err := child.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		jailerAssertReleased(t, env, m, jailerLaunchID)
	})
	t.Run("untrusted", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("a root-created temp file is root-owned")
		}
		env := newJailerEnv(t)
		untrusted := filepath.Join(jailerVarTempDir(t), "bios.bin")
		if err := os.WriteFile(untrusted, []byte("bios"), 0o444); err != nil {
			t.Fatal(err)
		}
		env.cfg.FirmwareFiles = []string{untrusted}
		m := env.open(t)
		defer m.Close()
		_, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
		if err == nil || !strings.Contains(err.Error(), "root-owned") {
			t.Fatalf("Start = %v, want an untrusted-source refusal", err)
		}
		if got := env.stages.last(); got != stageJailPopulate {
			t.Fatalf("last stage %q, want %q", got, stageJailPopulate)
		}
		jailerAssertReleased(t, env, m, jailerLaunchID)
	})
}

// TestHelperConfigSealed: the helper config is an anonymous, sealed memfd.
func TestHelperConfigSealed(t *testing.T) {
	f, err := helperConfigFile(helperConfig{Root: "/r", Binary: "/usr/bin/qemu", UID: 1, ControlFD: 9, LogFD: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("x")); err == nil {
		t.Fatal("sealed helper config accepted a write")
	}
	if link, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", f.Fd())); err != nil || !strings.HasPrefix(link, "/memfd:") {
		t.Fatalf("helper config is %q (%v), want an anonymous memfd", link, err)
	}
}
