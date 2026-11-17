package node

import (
	"os"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
)

// TestJailKVMDecodeJailRecord: the acceptance reads the jailer's durable
// record file exactly as the real writer publishes it (the golden is pinned
// to fileStates.Write by the jailer package's TestPersistedRecordEnvelopeGolden):
// the {record, ownership} envelope, decoded strictly, with a complete
// running vm identity required before any /proc read.
func TestJailKVMDecodeJailRecord(t *testing.T) {
	golden, err := os.ReadFile("testdata/jail-record-envelope.json")
	if err != nil {
		t.Fatal(err)
	}
	rec, err := jailKVMDecodeJailRecord(golden, "bx_aaaaaaaa")
	if err != nil {
		t.Fatalf("the real envelope was refused: %v", err)
	}
	if rec.PID != 4242 || rec.StartTicks != 777 || rec.Attempt != "0190aaaa-0000-7000-8000-000000000001" || rec.UID != 900003 ||
		rec.Kind != jailer.KindVM || rec.SchemaVersion != 2 || rec.ControlDev != 9 || rec.ControlIno != 70 || rec.BootID != "00000000-0000-4000-8000-0000000000b0" {
		t.Fatalf("decoded %+v: want the fixture's running identity", rec)
	}
	g := string(golden)
	edit := func(old, new string) []byte {
		if strings.Count(g, old) != 1 {
			t.Fatalf("golden lacks %q", old)
		}
		return []byte(strings.Replace(g, old, new, 1))
	}
	bare := g[strings.Index(g, `{"schemaVersion"`):strings.Index(g, `,"ownership"`)]
	for name, raw := range map[string][]byte{
		"bare record":       []byte(bare),
		"unknown top-level": edit(`{"record":`, `{"extra":1,"record":`),
		"unknown in record": edit(`"pid":4242`, `"pid":4242,"x":1`),
		"no ownership":      []byte(g[:strings.Index(g, `,"ownership"`)] + "}"),
		"empty ownership":   []byte(g[:strings.Index(g, `,"ownership"`)] + `,"ownership":{}}`),
		"blank ownership":   []byte(g[:strings.Index(g, `,"ownership"`)] + `,"ownership":{ }}`),
		"null ownership":    []byte(g[:strings.Index(g, `,"ownership"`)] + `,"ownership":null}`),
		"no log dev":        edit(`"logDev":9`, `"logDev":0`),
		"no target dev":     edit(`"targetDev":9`, `"targetDev":0`),
		"schema 1":          edit(`"schemaVersion":2`, `"schemaVersion":1`),
		"probe kind":        edit(`"kind":"vm"`, `"kind":"probe"`),
		"no pid":            edit(`"pid":4242,`, ``),
		"no start ticks":    edit(`"startTicks":777,`, ``),
		"no boot id":        edit(`"bootId":"00000000-0000-4000-8000-0000000000b0"`, `"bootId":""`),
		"no attempt":        edit(`"attempt":"0190aaaa-0000-7000-8000-000000000001"`, `"attempt":""`),
		"root uid":          edit(`"uid":900003`, `"uid":0`),
		"no control":        edit(`"controlIno":70`, `"controlIno":0`),
		"trailing data":     append([]byte(g), []byte("{}")...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := jailKVMDecodeJailRecord(raw, "bx_aaaaaaaa"); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	if _, err := jailKVMDecodeJailRecord(golden, "bx_bbbbbbbb"); err == nil {
		t.Error("another sandbox's record accepted")
	}
}
