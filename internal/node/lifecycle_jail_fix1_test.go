package node

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Slice3 fix round 1: the complete recovered argv grammar, durable
// caller-resource pins before adoption, unknown legacy executables, and the
// durable reason of a failed adoption's jail release.

const jailStrictID = "bx_ssssssss"

// jailStrictArgv is an independent literal production jailed argv for
// jailStrictID (not rendered by the code under test).
func jailStrictArgv() []string {
	return []string{
		"/usr/bin/qemu-system-x86_64",
		"-name", "guest=bx_ssssssss,process=bx_ssssssss",
		"-machine", "q35,accel=kvm",
		"-cpu", "Haswell-noTSX,enforce",
		"-nodefaults", "-no-user-config", "-no-reboot",
		"-m", "2048M", "-smp", "2",
		"-kernel", "/proc/self/fd/5",
		"-initrd", "/proc/self/fd/6",
		"-append", "root=/dev/vda rw console=ttyS0,115200n8 panic=-1 ip=10.42.0.2::10.42.0.1:255.255.0.0:::off:1.1.1.1",
		"-blockdev", "driver=qcow2,node-name=base,read-only=on,file.driver=file,file.node-name=basefile,file.filename=/proc/self/fd/8,file.cache.direct=off,file.cache.no-flush=off",
		"-blockdev", "driver=qcow2,node-name=rootdisk,lazy-refcounts=off,backing=base,file.driver=file,file.node-name=rootfile,file.filename=/proc/self/fd/9,file.cache.direct=off,file.cache.no-flush=off",
		"-device", "virtio-blk-pci,drive=rootdisk,write-cache=on,werror=stop,rerror=stop",
		"-netdev", "tap,id=net0,fd=3,vhost=on,vhostfd=4",
		"-device", "virtio-net-pci,netdev=net0,mac=52:54:00:00:00:02,romfile=",
		"-device", "vhost-vsock-pci,guest-cid=9,vhostfd=7",
		"-object", "rng-random,filename=/dev/urandom,id=rng0",
		"-device", "virtio-rng-pci,rng=rng0",
		"-device", "virtio-balloon-pci,free-page-reporting=on",
		"-device", "vmgenid",
		"-chardev", "file,id=serial0,path=/serial.log,append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:/control/qmp.sock,server=on,wait=off",
		"-display", "none",
		"-sandbox", "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny",
	}
}

func TestJailedArgvIdentityIsTheCompleteGrammar(t *testing.T) {
	meta := storage.VMMeta{SandboxID: jailStrictID}
	const binary = "/usr/bin/qemu-system-x86_64"
	if err := jailedArgvIdentity(jailStrictArgv(), meta, binary); err != nil {
		t.Fatalf("valid literal jailed argv refused: %v", err)
	}
	replace := func(old, new string) func([]string) []string {
		return func(a []string) []string {
			for i := range a {
				a[i] = strings.Replace(a[i], old, new, 1)
			}
			return a
		}
	}
	appendArgs := func(extra ...string) func([]string) []string {
		return func(a []string) []string { return append(a, extra...) }
	}
	for name, mutate := range map[string]func([]string) []string{
		"base opens a host path":    replace("file.filename=/proc/self/fd/8", "file.filename=/etc/shadow"),
		"overlay opens a host path": replace("file.filename=/proc/self/fd/9", "file.filename=/srv/disk.qcow2"),
		"base is writable":          replace("node-name=base,read-only=on", "node-name=base,read-only=off"),
		"overlay follows stored":    replace(",backing=base,", ","),
		"network ROM enabled":       replace("mac=52:54:00:00:00:02,romfile=", "mac=52:54:00:00:00:02"),
		"network ROM from a file":   replace(",romfile=", ",romfile=/srv/efi.rom"),
		"extra readconfig":          appendArgs("-readconfig", "/etc/shadow"),
		"extra drive":               appendArgs("-drive", "file=/etc/shadow"),
		"second netdev":             appendArgs("-netdev", "user,id=net1"),
		"duplicate qmp":             appendArgs("-qmp", "unix:/control/qmp.sock,server=on,wait=off"),
		"alternate qmp spelling":    replace("-qmp", "--qmp"),
		"json blockdev":             replace("driver=qcow2,node-name=base,", `{"driver":"qcow2","node-name":"base"}`),
		"protocol kernel":           replace("/proc/self/fd/5", "http://203.0.113.1/k"),
		"other sandbox name":        replace("guest=bx_ssssssss,process=bx_ssssssss", "guest=bx_tttttttt,process=bx_tttttttt"),
		"relaxed seccomp":           replace("spawn=deny,", ""),
		"other binary":              replace("/usr/bin/qemu-system-x86_64", "/tmp/qemu"),
		"qmp on a host path":        replace("unix:/control/qmp.sock", "unix:/var/lib/bbraft/qmp.sock"),
		"positional argument":       appendArgs("/etc/shadow"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := jailedArgvIdentity(mutate(jailStrictArgv()), meta, binary); err == nil {
				t.Fatal("unsafe or incomplete jailed argv accepted")
			}
		})
	}
}

// TestJailedAdoptionRequiresDurableCallerPins: a claimed live attempt is
// adopted only when the manifest's pins equal the record and the current
// control, console and disk are those inodes; otherwise the process, record,
// network, reservation and every byte stay untouched.
func TestJailedAdoptionRequiresDurableCallerPins(t *testing.T) {
	for _, mode := range []string{"none", "mismatch", "replaced-console", "symlink-disk"} {
		t.Run(mode, func(t *testing.T) {
			f := newJailLiveFixture(t, false)
			f.pins = mode
			j := f.harness(t)
			if !j.unresolved(f.id) || j.vmRef(f.id) != (jailer.Ref{SandboxID: f.id, Attempt: f.attempt}) {
				t.Fatalf("state %s, unresolved %v: adopted without proven caller pins", j.state(t, f.id).GetState(), j.unresolved(f.id))
			}
			if !jailAlive(f.helper) || j.jail.attempts != 0 || !j.netAllocated(f.id) || j.reserved().vcpus == 0 {
				t.Fatal("an unproven adoption killed, released or freed something")
			}
			dir := j.mg.store.VMDir(f.id)
			after := jailTreeWithContent(t, dir, jailTree(t, dir))
			before := maps.Clone(f.before)
			for _, tree := range []map[string]string{before, after} {
				maps.DeleteFunc(tree, func(p string, _ string) bool { return filepath.Base(p) == "vm.json" })
			}
			if !maps.Equal(before, after) {
				t.Fatalf("verification changed the VM directory\nbefore %v\nafter  %v", before, after)
			}
		})
	}
}

// TestJailedLegacyRecoveryRefusesUnknownExecutable: without a trusted QEMU
// executable the legacy scanner cannot prove a live legacy QEMU absent, so
// startup fails closed instead of treating "not found" as death.
func TestJailedLegacyRecoveryRefusesUnknownExecutable(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	id := ids.Sandbox()
	var helperAlive func() bool
	jailNewFails(t, newFakeJail(t), id, func(t *testing.T, dataDir, imageDir string) {
		jailPrepare(t, dataDir, imageDir, id, func(s *storage.Store) {
			if err := s.WriteLaunchIntent(id); err != nil {
				t.Fatal(err)
			}
			host := qemu.HostCPU{Vendor: "GenuineIntel"}
			cfg := qemu.Config{SandboxID: id, WorkDir: s.VMDir(id), ImageDir: filepath.Join(imageDir, testImageID), DiskPath: s.DiskPath(id), VCPUs: 2, MemoryMiB: 2048, VsockCID: 9, Nested: qemu.NestedOff, HostCPU: &host}
			args, err := cfg.Args()
			if err != nil {
				t.Fatal(err)
			}
			helper := jailLiveHelper(t, append([]string{os.Args[0]}, args[1:]...))
			helperAlive = func() bool { return jailAlive(helper) }
		})
	}, "cannot identify")
	if !helperAlive() {
		t.Fatal("the live legacy process was touched")
	}
}

// TestJailedLegacyRecoveryKnownDead is the positive control: with a trusted
// executable and no live legacy process the record recovers FAILED.
func TestJailedLegacyRecoveryKnownDead(t *testing.T) {
	id := ids.Sandbox()
	j := newJailedHarness(t, jailedOptions{
		prepare: func(t *testing.T, dataDir, imageDir string) {
			jailPrepare(t, dataDir, imageDir, id, func(s *storage.Store) {
				if err := s.WriteLaunchIntent(id); err != nil {
					t.Fatal(err)
				}
			})
		},
		configure: func(cfg *Config) { cfg.qemuExe = "/usr/bin/qemu-system-x86_64" },
	})
	if vm := j.state(t, id); vm.GetState() != nodev1.VMState_VM_STATE_FAILED || j.unresolved(id) {
		t.Fatalf("state %s unresolved %v", vm.GetState(), j.unresolved(id))
	}
}

// TestJailedAdoptedFailurePersistsJailReleaseError: a failed adoption whose
// jail release fails keeps everything and records both causes durably; a
// retry by identity then settles.
func TestJailedAdoptedFailurePersistsJailReleaseError(t *testing.T) {
	f := newJailFixture(t)
	attempt := uuid.NewV7().String()
	if err := f.m.store.WriteJailedLaunchIntent(t.Context(), jailTestID, attempt, strings.Repeat("ab", 32)); err != nil {
		t.Fatal(err)
	}
	meta, err := f.m.store.ReadVM(jailTestID)
	if err != nil {
		t.Fatal(err)
	}
	ref := jailer.Ref{SandboxID: jailTestID, Attempt: attempt}
	f.v.jail = ref
	f.jail.records[jailTestID] = jailer.Record{SandboxID: jailTestID, Attempt: attempt}
	injected := errors.New("sentinel jail release barrier failed")
	f.jail.releaseErr = injected
	proc := newFakeProcess()
	proc.exit(nil)
	f.v.process = proc
	f.m.mu.Lock()
	f.m.reserveForceLocked(f.v)
	full := f.m.reserved
	f.m.mu.Unlock()
	f.m.settleAdoptedFailure(f.v, meta, proc, nil, errors.New("guest ping failed"))
	after, err := f.m.store.ReadVM(jailTestID)
	if err != nil {
		t.Fatal(err)
	}
	for _, cause := range []string{injected.Error(), "guest ping failed"} {
		if !strings.Contains(after.LastError, cause) {
			t.Fatalf("durable reason %q lacks %q", after.LastError, cause)
		}
	}
	f.v.mu.Lock()
	visible := f.v.errMsg
	f.v.mu.Unlock()
	if !strings.Contains(visible, injected.Error()) {
		t.Fatalf("visible reason %q hides the jail release error", visible)
	}
	f.m.mu.Lock()
	reserved := f.m.reserved
	f.m.mu.Unlock()
	if reserved != full || after.QEMU == nil || f.ref() != ref || f.jail.releases != 0 {
		t.Fatal("a failed adopted settlement released accounting, the launch record or the attempt")
	}
	if _, err := os.Stat(f.m.store.VMDir(jailTestID)); err != nil {
		t.Fatal("the VM directory was removed")
	}
	f.jail.releaseErr = nil
	if err := f.m.settleJail(t.Context(), f.v, nil); err != nil || f.ref() != (jailer.Ref{}) {
		t.Fatalf("identity retry = %v, ref %+v", err, f.ref())
	}
}
