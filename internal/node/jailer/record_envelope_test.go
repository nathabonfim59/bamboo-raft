package jailer

import (
	"bytes"
	"os"
	"path"
	"testing"
)

// jailerEnvelopeGolden is the node harness's copy of one durable record
// file (internal/node/testdata), which the jailed acceptance decodes.
const jailerEnvelopeGolden = "../testdata/jail-record-envelope.json"

// jailerEnvelopeFixture is a deterministic, valid running vm record and its
// ownership: literal ids, inodes and times, no host details.
func jailerEnvelopeFixture() (Record, resourceOwnership) {
	return Record{
		SchemaVersion: recordSchema, SandboxID: "bx_aaaaaaaa", Attempt: "0190aaaa-0000-7000-8000-000000000001", Kind: KindVM,
		UID: 900003, PID: 4242, BootID: "00000000-0000-4000-8000-0000000000b0", StartTicks: 777,
		ControlDev: 9, ControlIno: 70, LogDev: 9, LogIno: 71, TargetDev: 9, TargetIno: 90,
		TargetRoot: TargetRootData, TargetPath: "vms/bx_aaaaaaaa/disk.qcow2", TargetAccess: AccessOwnerRW,
		CreatedUnixMs: 1700000000000,
	}, resourceOwnership{
		Jail: inodeIdentity{Dev: 9, Ino: 80}, Cgroup: inodeIdentity{Dev: 33, Ino: 5},
		Entries: []ownedEntry{{Path: "root", Dev: 9, Ino: 81, Dir: true}, {Path: "root/usr/bin/qemu-system-x86_64", Dev: 9, Ino: 82}},
		Exec:    "root/usr/bin/qemu-system-x86_64",
	}
}

// TestPersistedRecordEnvelopeGolden pins the node harness's golden record
// file to the REAL writer: fileStates.Write publishes the fixture and the
// published bytes must equal the golden exactly. A change of the persisted
// shape fails here first; BBRAFT_UPDATE_JAIL_RECORD_GOLDEN=1 rewrites the
// golden from the writer.
func TestPersistedRecordEnvelopeGolden(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.Mkdir(recordsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	record, own := jailerEnvelopeFixture()
	if _, err := (fileStates{}).Write(root, record, own, inodeIdentity{}); err != nil {
		t.Fatal(err)
	}
	raw, err := root.ReadFile(path.Join(recordsDir, record.SandboxID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("BBRAFT_UPDATE_JAIL_RECORD_GOLDEN") == "1" {
		if err := os.WriteFile(jailerEnvelopeGolden, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	golden, err := os.ReadFile(jailerEnvelopeGolden)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, golden) {
		t.Fatalf("the real writer's record file differs from %s:\nwriter: %s\ngolden: %s", jailerEnvelopeGolden, raw, golden)
	}
}
