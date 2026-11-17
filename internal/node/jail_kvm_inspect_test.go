package node

import (
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func TestJailKVMCheckFDs(t *testing.T) {
	const chr, fifo = 0o020000, 0o010000
	overlay, kernel := jailKVMIdentity{Dev: 9, Ino: 90}, jailKVMIdentity{Dev: 9, Ino: 50}
	dataRoot, control := jailKVMIdentity{Dev: 9, Ino: 2}, jailKVMIdentity{Dev: 9, Ino: 70}
	null := 1<<8 | uint64(3)
	policy := jailKVMFDPolicy{
		Rdev:         map[int]uint64{3: 10<<8 | 200, 4: 10<<8 | 238, 7: 10<<8 | 241},
		ID:           map[int]jailKVMIdentity{5: kernel, 9: overlay},
		ForbiddenIDs: map[jailKVMIdentity]string{dataRoot: "data root", control: "control dir"},
		ForbiddenDev: 33,
		NullRdev:     null,
		MaxFDs:       64,
	}
	ok := []jailKVMFD{
		{Num: 0, Link: "/dev/null", ID: jailKVMIdentity{Dev: 5, Ino: 4}, Mode: chr, Rdev: null},
		{Num: 1, Link: "/dev/null", ID: jailKVMIdentity{Dev: 5, Ino: 4}, Mode: chr, Rdev: null},
		{Num: 2, Link: "pipe:[55]", ID: jailKVMIdentity{Dev: 12, Ino: 55}, Mode: fifo | 0o600},
		{Num: 3, Link: "/dev/net/tun", ID: jailKVMIdentity{Dev: 5, Ino: 10}, Mode: chr, Rdev: 10<<8 | 200},
		{Num: 4, Link: "/dev/vhost-net", ID: jailKVMIdentity{Dev: 5, Ino: 11}, Mode: chr, Rdev: 10<<8 | 238},
		{Num: 5, Link: "/srv/kernel", ID: kernel, Mode: 0o100444},
		{Num: 7, Link: "/dev/vhost-vsock", ID: jailKVMIdentity{Dev: 5, Ino: 12}, Mode: chr, Rdev: 10<<8 | 241},
		{Num: 9, Link: "/data/vms/bx/disk.qcow2", ID: overlay, Mode: 0o100600},
		// The intended serial chardev (the pinned console log) is allowed.
		{Num: 10, Link: "/serial.log", ID: jailKVMIdentity{Dev: 9, Ino: 71}, Mode: 0o100600},
		{Num: 12, Link: "socket:[777]", ID: jailKVMIdentity{Dev: 8, Ino: 777}, Mode: 0o140777},
		{Num: 13, Link: "anon_inode:kvm-vm", ID: jailKVMIdentity{Dev: 14, Ino: 1}, Mode: 0o600},
	}
	if err := jailKVMCheckFDs(ok, policy); err != nil {
		t.Fatalf("exact map refused: %v", err)
	}
	// set replaces (or, with drop, removes) the fd numbered n; add appends.
	set := func(n int, f *jailKVMFD) []jailKVMFD {
		var c []jailKVMFD
		for _, fd := range ok {
			if fd.Num != n {
				c = append(c, fd)
			} else if f != nil {
				c = append(c, *f)
			}
		}
		return c
	}
	add := func(f jailKVMFD) []jailKVMFD { return append(slices.Clone(ok), f) }
	at := func(n int) jailKVMFD { return ok[slices.IndexFunc(ok, func(f jailKVMFD) bool { return f.Num == n })] }
	with := func(n int, change func(*jailKVMFD)) []jailKVMFD { f := at(n); change(&f); return set(n, &f) }
	many := slices.Clone(ok)
	for i := range 64 {
		many = append(many, jailKVMFD{Num: 100 + i, Link: "anon_inode:[eventfd]", ID: jailKVMIdentity{Dev: 14, Ino: uint64(10 + i)}, Mode: 0o600})
	}
	for name, fds := range map[string][]jailKVMFD{
		"tap missing":        set(3, nil),
		"tap wrong device":   with(3, func(f *jailKVMFD) { f.Rdev = null }),
		"tap not a char":     with(3, func(f *jailKVMFD) { f.Mode = 0o100600 }),
		"vsock swapped":      with(7, func(f *jailKVMFD) { f.Rdev = 10<<8 | 238 }),
		"kernel replaced":    with(5, func(f *jailKVMFD) { f.ID = jailKVMIdentity{Dev: 9, Ino: 51} }),
		"overlay missing":    set(9, nil),
		"data root fd":       add(jailKVMFD{Num: 20, Link: "/data", ID: dataRoot, Mode: 0o040700}),
		"control dir fd":     add(jailKVMFD{Num: 23, Link: "/control", ID: control, Mode: 0o040700}),
		"cgroup fd":          add(jailKVMFD{Num: 21, Link: "/sys/fs/cgroup/x", ID: jailKVMIdentity{Dev: 33, Ino: 5}, Mode: 0o040755}),
		"setup config memfd": add(jailKVMFD{Num: 22, Link: "/memfd:bbraft-jailer-config (deleted)", ID: jailKVMIdentity{Dev: 1, Ino: 9}, Mode: 0o100600}),
		// The release and status pipes are the only pipes the setup ever
		// held: no pipe may survive beyond the inherited stdio.
		"setup pipe leaked":  add(jailKVMFD{Num: 24, Link: "pipe:[99]", ID: jailKVMIdentity{Dev: 12, Ino: 99}, Mode: fifo | 0o600}),
		"stdin a pipe":       set(0, &jailKVMFD{Num: 0, Link: "pipe:[98]", ID: jailKVMIdentity{Dev: 12, Ino: 98}, Mode: fifo | 0o600}),
		"stdout a data file": set(1, &jailKVMFD{Num: 1, Link: "/x", ID: jailKVMIdentity{Dev: 9, Ino: 3}, Mode: 0o100600}),
		"stdout other char":  with(1, func(f *jailKVMFD) { f.Rdev = 1<<8 | 5 }),
		"stderr missing":     set(2, nil),
		"too many fds":       many,
	} {
		t.Run(name, func(t *testing.T) {
			if err := jailKVMCheckFDs(fds, policy); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestJailKVMCrossCheckQEMU(t *testing.T) {
	const svc, id = "bbraft-kt-a1", "bx_aaaaaaaa"
	rec := jailer.Record{SandboxID: id, Attempt: "0190aaaa-0000-7000-8000-000000000001", Kind: jailer.KindVM, UID: 900003, PID: 4242,
		BootID: "boot-1", StartTicks: 777, ControlDev: 9, ControlIno: 70}
	meta := storage.VMMeta{SandboxID: id, QEMU: &storage.QEMURecord{State: storage.QEMURunning, PID: 4242, BootID: "boot-1", StartTicks: 777, JailAttempt: rec.Attempt},
		Control: &storage.Identity{Dev: 9, Ino: 70}}
	cg := "0::/system.slice/" + svc + ".service/vms/qemu-" + id + "\n"
	check := func(rec jailer.Record, meta storage.VMMeta, boot, start, cg, procs string) error {
		return jailKVMCrossCheckQEMU(rec, meta, boot, start, cg, procs, svc, id)
	}
	if err := check(rec, meta, "boot-1", "777", cg, "4242\n"); err != nil {
		t.Fatalf("consistent record refused: %v", err)
	}
	q := *meta.QEMU
	for name, err := range map[string]error{
		"other boot":         check(rec, meta, "boot-2", "777", cg, "4242\n"),
		"pid reused":         check(rec, meta, "boot-1", "778", cg, "4242\n"),
		"not a member":       check(rec, meta, "boot-1", "777", cg, "4243\n"),
		"other cgroup":       check(rec, meta, "boot-1", "777", "0::/system.slice/"+svc+".service/supervisor\n", "4242\n"),
		"probe record":       check(func() jailer.Record { r := rec; r.Kind = jailer.KindProbe; return r }(), meta, "boot-1", "777", cg, "4242\n"),
		"other sandbox":      check(func() jailer.Record { r := rec; r.SandboxID = "bx_bbbbbbbb"; return r }(), meta, "boot-1", "777", cg, "4242\n"),
		"root uid":           check(func() jailer.Record { r := rec; r.UID = 0; return r }(), meta, "boot-1", "777", cg, "4242\n"),
		"manifest other pid": check(rec, func() storage.VMMeta { m := meta; c := q; c.PID = 4243; m.QEMU = &c; return m }(), "boot-1", "777", cg, "4242\n"),
		"manifest attempt":   check(rec, func() storage.VMMeta { m := meta; c := q; c.JailAttempt = "other"; m.QEMU = &c; return m }(), "boot-1", "777", cg, "4242\n"),
		"manifest launching": check(rec, func() storage.VMMeta { m := meta; c := q; c.State = storage.QEMULaunching; m.QEMU = &c; return m }(), "boot-1", "777", cg, "4242\n"),
		"manifest no qemu":   check(rec, func() storage.VMMeta { m := meta; m.QEMU = nil; return m }(), "boot-1", "777", cg, "4242\n"),
		"control replaced":   check(rec, func() storage.VMMeta { m := meta; m.Control = &storage.Identity{Dev: 9, Ino: 71}; return m }(), "boot-1", "777", cg, "4242\n"),
		"manifest start":     check(rec, func() storage.VMMeta { m := meta; c := q; c.StartTicks = 778; m.QEMU = &c; return m }(), "boot-1", "777", cg, "4242\n"),
	} {
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestJailKVMCheckQEMUStatus(t *testing.T) {
	status := func(uid string, seccomp string) string {
		return "Uid:\t" + uid + "\t" + uid + "\t" + uid + "\t" + uid + "\nGid:\t" + uid + "\t" + uid + "\t" + uid + "\t" + uid +
			"\nGroups:\t\nNoNewPrivs:\t1\nSeccomp:\t" + seccomp + "\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapEff:\t0000000000000000\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\n"
	}
	if err := jailKVMCheckQEMUStatus(status("900003", "2"), 900003, 900000, 900999); err != nil {
		t.Fatalf("confined status refused: %v", err)
	}
	for name, err := range map[string]error{
		"no seccomp filter": jailKVMCheckQEMUStatus(status("900003", "0"), 900003, 900000, 900999),
		"strict seccomp":    jailKVMCheckQEMUStatus(status("900003", "1"), 900003, 900000, 900999),
		"other uid":         jailKVMCheckQEMUStatus(status("900004", "2"), 900003, 900000, 900999),
		"outside the range": jailKVMCheckQEMUStatus(status("901000", "2"), 901000, 900000, 900999),
		"capabilities":      jailKVMCheckQEMUStatus(strings.Replace(status("900003", "2"), "CapBnd:\t0000000000000000", "CapBnd:\t0000000000000001", 1), 900003, 900000, 900999),
		"groups":            jailKVMCheckQEMUStatus(strings.Replace(status("900003", "2"), "Groups:\t", "Groups:\t27", 1), 900003, 900000, 900999),
		"no NNP":            jailKVMCheckQEMUStatus(strings.Replace(status("900003", "2"), "NoNewPrivs:\t1", "NoNewPrivs:\t0", 1), 900003, 900000, 900999),
	} {
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestJailKVMRootMountReadOnly(t *testing.T) {
	ro := "500 499 253:1 /jail/root / ro,nosuid,relatime shared:1 - ext4 /dev/vda1 rw\n501 500 0:22 / /proc rw,nosuid,nodev,noexec - proc proc rw\n"
	if err := jailKVMRootMountReadOnly(ro); err != nil {
		t.Fatalf("read-only root refused: %v", err)
	}
	// The effective root is the top of the stack of "/" mounts by
	// mount-id/parent topology, whatever the line order.
	for name, mi := range map[string]string{
		"stacked, top last":  "499 1 253:1 /jail/root / rw,relatime - ext4 /dev/vda1 rw\n" + ro,
		"stacked, top first": ro + "499 1 253:1 /jail/root / rw,relatime - ext4 /dev/vda1 rw\n",
	} {
		if err := jailKVMRootMountReadOnly(mi); err != nil {
			t.Fatalf("%s: the top root mount is read-only; refused: %v", name, err)
		}
	}
	for name, mi := range map[string]string{
		"writable root":           "500 499 253:1 /jail/root / rw,nosuid - ext4 /dev/vda1 rw\n",
		"writable on top":         ro + "502 500 253:1 /jail/root / rw,relatime - ext4 /dev/vda1 rw\n",
		"writable top unordered":  "502 500 253:1 /jail/root / rw,relatime - ext4 /dev/vda1 rw\n" + ro,
		"no root mount":           "501 500 0:22 / /proc rw - proc proc rw\n",
		"malformed":               "500 499 253:1\n",
		"ro only in super":        "500 499 253:1 /jail/root / rw,nosuid - ext4 /dev/vda1 ro\n",
		"branch":                  ro + "502 500 253:1 /a / ro - ext4 /dev/vda1 rw\n503 500 253:1 /b / ro - ext4 /dev/vda1 rw\n",
		"two unrelated roots":     "500 499 253:1 /a / ro - ext4 /dev/vda1 rw\n600 599 253:1 /b / ro - ext4 /dev/vda1 rw\n",
		"cycle":                   "500 600 253:1 /a / ro - ext4 /dev/vda1 rw\n600 500 253:1 /b / ro - ext4 /dev/vda1 rw\n",
		"self parent":             "500 500 253:1 /a / ro - ext4 /dev/vda1 rw\n",
		"duplicate id":            "500 499 253:1 /a / ro - ext4 /dev/vda1 rw\n500 499 253:1 /b /proc rw - proc proc rw\n",
		"no separator":            "500 499 253:1 /jail/root / ro,nosuid shared:1 ext4 /dev/vda1 rw x\n",
		"nothing after separator": "500 499 253:1 /jail/root / ro - ext4\n",
		"non-numeric id":          "x5 499 253:1 /jail/root / ro - ext4 /dev/vda1 rw\n",
		"empty":                   "",
	} {
		if err := jailKVMRootMountReadOnly(mi); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestJailKVMCheckLimits(t *testing.T) {
	want := jailer.Limits{CPUQuotaPercent: 200, MemoryBytes: (2048 + 256) << 20, Pids: 260,
		IO: jailer.IOLimits{Device: jailer.BlockDevice{Major: 8, Minor: 0}, WriteBPS: 104857600}}
	files := map[string]string{
		"cpu.max":    "200000 100000\n",
		"memory.max": strconv.FormatUint(want.MemoryBytes, 10) + "\n",
		"pids.max":   "260\n",
		"io.max":     "8:0 rbps=max wbps=104857600 riops=max wiops=max\n",
	}
	if err := jailKVMCheckLimits(files, want); err != nil {
		t.Fatalf("exact limits refused: %v", err)
	}
	for name, edit := range map[string][2]string{
		"cpu wider":       {"cpu.max", "max 100000\n"},
		"memory unbound":  {"memory.max", "max\n"},
		"memory other":    {"memory.max", "1\n"},
		"pids other":      {"pids.max", "261\n"},
		"io unbound":      {"io.max", ""},
		"io other device": {"io.max", "8:16 rbps=max wbps=104857600 riops=max wiops=max\n"},
		"io extra limit":  {"io.max", "8:0 rbps=1 wbps=104857600 riops=max wiops=max\n"},
		"io other rate":   {"io.max", "8:0 rbps=max wbps=1 riops=max wiops=max\n"},
		"io two devices":  {"io.max", "8:0 rbps=max wbps=104857600 riops=max wiops=max\n8:16 rbps=max wbps=1 riops=max wiops=max\n"},
		"missing file":    {"pids.max", "\x00drop"},
	} {
		t.Run(name, func(t *testing.T) {
			f := maps.Clone(files)
			if edit[1] == "\x00drop" {
				delete(f, edit[0])
			} else {
				f[edit[0]] = edit[1]
			}
			if err := jailKVMCheckLimits(f, want); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestJailKVMCheckNamespaces(t *testing.T) {
	main := map[string]string{"mnt": "mnt:[1]", "pid": "pid:[2]", "ipc": "ipc:[3]"}
	if err := jailKVMCheckNamespaces(map[string]string{"mnt": "mnt:[11]", "pid": "pid:[12]", "ipc": "ipc:[13]"}, main); err != nil {
		t.Fatalf("private namespaces refused: %v", err)
	}
	for _, shared := range []string{"mnt", "pid", "ipc"} {
		q := map[string]string{"mnt": "mnt:[11]", "pid": "pid:[12]", "ipc": "ipc:[13]"}
		q[shared] = main[shared]
		if err := jailKVMCheckNamespaces(q, main); err == nil {
			t.Errorf("shared %s accepted", shared)
		}
	}
	if err := jailKVMCheckNamespaces(map[string]string{"mnt": "mnt:[11]", "pid": "pid:[12]"}, main); err == nil {
		t.Error("an unread namespace accepted")
	}
}

func TestJailKVMParseProbeOutput(t *testing.T) {
	out := "=== RUN   TestKVMJailProbeHelper\n" + jailKVMProbeMarker + `{"status":"Uid:\t1\n","result":"EACCES"}` + "\n--- PASS: TestKVMJailProbeHelper\nPASS\n"
	got, err := jailKVMParseProbeOutput(out)
	if err != nil || got.Result != "EACCES" || got.Status != "Uid:\t1\n" {
		t.Fatalf("parse = %+v, %v", got, err)
	}
	for name, bad := range map[string]string{
		"no report":     "PASS\n",
		"two reports":   jailKVMProbeMarker + `{"status":"","result":"ok"}` + "\n" + jailKVMProbeMarker + `{"status":"","result":"ok"}` + "\n",
		"invalid json":  jailKVMProbeMarker + "{\n",
		"unknown field": jailKVMProbeMarker + `{"status":"","result":"ok","x":1}` + "\n",
		"odd result":    jailKVMProbeMarker + `{"status":"","result":"EIO"}` + "\n",
	} {
		if _, err := jailKVMParseProbeOutput(bad); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestJailKVMCheckRotatedHostKey(t *testing.T) {
	keyA := "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
	keyB := "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAa7+7rFQQ2bJm7nN0rD3gS3hK0r5pAFzZRyUTE9tKbL"
	access := func(key string) *nodev1.SSHAccess {
		return &nodev1.SSHAccess{User: "user", Host: "203.0.113.5", Port: 30001, HostKey: key}
	}
	if err := jailKVMCheckRotatedHostKey(access(keyA), access(keyB)); err != nil {
		t.Fatalf("a rotated key refused: %v", err)
	}
	for name, pair := range map[string][2]string{
		"same key":               {keyA, keyA},
		"same key other comment": {keyA + " root@a", keyA + " root@b"},
		"same key extra spacing": {keyA, strings.Replace(keyA, " ", "  ", 1)},
		"malformed old":          {"ssh-ed25519 !!!", keyB},
		"malformed new":          {keyA, "not a key"},
		"empty new":              {keyA, ""},
		"not ed25519":            {keyA, "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC7"},
	} {
		if err := jailKVMCheckRotatedHostKey(access(pair[0]), access(pair[1])); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
