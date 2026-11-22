package qemu

import (
	"encoding/json/v2"
	"os"
	"slices"
	"strings"
	"testing"
)

// The fixture testdata/qmp-schema-8.2-phase2.json is a hand-trimmed excerpt
// of a query-qmp-schema reply in QEMU's introspection format (masked type
// names, flattened union variants, enum values and members), following QEMU
// 8.2's qapi schema for every command Phase 2 uses. The spike log only kept
// the first bytes of the real reply; the KVM test TestKVMPhase2Schema checks
// the parser against a real QEMU.

type schemaFixture []map[string]any

func schemaLoad(t *testing.T) schemaFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/qmp-schema-8.2-phase2.json")
	if err != nil {
		t.Fatal(err)
	}
	var f schemaFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f schemaFixture) entry(t *testing.T, name string) map[string]any {
	t.Helper()
	for _, e := range f {
		if e["name"] == name {
			return e
		}
	}
	t.Fatalf("fixture has no entry %q", name)
	return nil
}

// args returns a command's argument object.
func (f schemaFixture) args(t *testing.T, command string) map[string]any {
	t.Helper()
	return f.entry(t, f.entry(t, command)["arg-type"].(string))
}

// memberType returns the type of obj's member.
func (f schemaFixture) memberType(t *testing.T, obj map[string]any, member string) string {
	t.Helper()
	for _, m := range obj["members"].([]any) {
		if m.(map[string]any)["name"] == member {
			return m.(map[string]any)["type"].(string)
		}
	}
	t.Fatalf("object %v has no member %q", obj["name"], member)
	return ""
}

func (f schemaFixture) dropMember(t *testing.T, obj map[string]any, member string) {
	t.Helper()
	ms := obj["members"].([]any)
	obj["members"] = slices.DeleteFunc(ms, func(m any) bool { return m.(map[string]any)["name"] == member })
	if len(obj["members"].([]any)) == len(ms) {
		t.Fatalf("object %v has no member %q", obj["name"], member)
	}
}

func (f schemaFixture) dropEnumValue(t *testing.T, enum map[string]any, value string) {
	t.Helper()
	enum["values"] = slices.DeleteFunc(enum["values"].([]any), func(v any) bool { return v == value })
	f.dropMember(t, enum, value)
}

func (f schemaFixture) dropVariant(t *testing.T, union map[string]any, tagCase string) {
	t.Helper()
	vs := union["variants"].([]any)
	union["variants"] = slices.DeleteFunc(vs, func(v any) bool { return v.(map[string]any)["case"] == tagCase })
	if len(union["variants"].([]any)) == len(vs) {
		t.Fatalf("union %v has no variant %q", union["name"], tagCase)
	}
}

func (f schemaFixture) variant(t *testing.T, union map[string]any, tagCase string) map[string]any {
	t.Helper()
	for _, v := range union["variants"].([]any) {
		if v.(map[string]any)["case"] == tagCase {
			return f.entry(t, v.(map[string]any)["type"].(string))
		}
	}
	t.Fatalf("union %v has no variant %q", union["name"], tagCase)
	return nil
}

func (f schemaFixture) dropCommand(t *testing.T, name string) schemaFixture {
	t.Helper()
	out := slices.DeleteFunc(f, func(e map[string]any) bool { return e["name"] == name })
	if len(out) == len(f) {
		t.Fatalf("fixture has no command %q", name)
	}
	return out
}

func (f schemaFixture) marshal(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPhase2SchemaSupport(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, f schemaFixture) schemaFixture
		want   []string
	}{
		{"QEMU 8.2 excerpt", nil, nil},
		{"missing command", func(t *testing.T, f schemaFixture) schemaFixture {
			return f.dropCommand(t, "block-dirty-bitmap-enable")
		}, []string{"command block-dirty-bitmap-enable"}},
		{"missing command with arguments reports only the command", func(t *testing.T, f schemaFixture) schemaFixture {
			return f.dropCommand(t, "block-export-add")
		}, []string{"command block-export-add"}},
		{"missing argument", func(t *testing.T, f schemaFixture) schemaFixture {
			f.dropMember(t, f.args(t, "blockdev-backup"), "filter-node-name")
			return f
		}, []string{"blockdev-backup argument filter-node-name"}},
		{"missing variant argument", func(t *testing.T, f schemaFixture) schemaFixture {
			f.dropMember(t, f.variant(t, f.args(t, "block-export-add"), "nbd"), "allocation-depth")
			return f
		}, []string{"block-export-add type nbd argument allocation-depth"}},
		{"missing export type", func(t *testing.T, f schemaFixture) schemaFixture {
			f.dropVariant(t, f.args(t, "block-export-add"), "nbd")
			return f
		}, []string{"block-export-add type nbd argument name", "block-export-add type nbd argument bitmaps", "block-export-add type nbd argument allocation-depth"}},
		{"missing enum value", func(t *testing.T, f schemaFixture) schemaFixture {
			args := f.args(t, "blockdev-backup")
			f.dropEnumValue(t, f.entry(t, f.memberType(t, args, "sync")), "none")
			return f
		}, []string{"blockdev-backup sync value none"}},
		{"missing reboot action", func(t *testing.T, f schemaFixture) schemaFixture {
			args := f.args(t, "set-action")
			f.dropEnumValue(t, f.entry(t, f.memberType(t, args, "reboot")), "reset")
			return f
		}, []string{"set-action reboot value reset"}},
		{"missing transaction action", func(t *testing.T, f schemaFixture) schemaFixture {
			array := f.entry(t, f.memberType(t, f.args(t, "transaction"), "actions"))
			f.dropVariant(t, f.entry(t, array["element-type"].(string)), "blockdev-backup")
			return f
		}, []string{"transaction action blockdev-backup"}},
		{"enum with values only (older QEMU)", func(t *testing.T, f schemaFixture) schemaFixture {
			args := f.args(t, "blockdev-backup")
			delete(f.entry(t, f.memberType(t, args, "sync")), "members")
			return f
		}, nil},
		{"enum with members only (newer QEMU)", func(t *testing.T, f schemaFixture) schemaFixture {
			args := f.args(t, "blockdev-backup")
			delete(f.entry(t, f.memberType(t, args, "sync")), "values")
			return f
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := schemaLoad(t)
			if tt.mutate != nil {
				f = tt.mutate(t, f)
			}
			got, err := Phase2SchemaSupport(f.marshal(t))
			if err != nil {
				t.Fatalf("Phase2SchemaSupport: %v", err)
			}
			if !slices.Equal(got.Missing, tt.want) {
				t.Fatalf("Missing = %q, want %q", got.Missing, tt.want)
			}
			if got.Available() != (len(tt.want) == 0) {
				t.Fatalf("Available = %t with Missing %q", got.Available(), got.Missing)
			}
		})
	}
}

func TestPhase2SupportReason(t *testing.T) {
	if r := (Phase2Support{QEMUVersion: "10.0.0"}).Reason(); r != "" {
		t.Fatalf("available Reason = %q", r)
	}
	s := Phase2Support{QEMUVersion: "7.2.0", Missing: []string{"command set-action", "blockdev-backup sync value none"}}
	want := "Phase 2 unavailable: QEMU 7.2.0 lacks command set-action, blockdev-backup sync value none"
	if r := s.Reason(); r != want {
		t.Fatalf("Reason = %q, want %q", r, want)
	}
}

func TestPhase2SchemaSupportBadReply(t *testing.T) {
	for _, raw := range []string{``, `{}`, `[{"name":1}]`} {
		if _, err := Phase2SchemaSupport([]byte(raw)); err == nil {
			t.Errorf("Phase2SchemaSupport(%q) succeeded", raw)
		}
	}
	// An empty schema decodes and lacks everything.
	s, err := Phase2SchemaSupport([]byte(`[]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Missing) != len(phase2Commands) || !strings.HasPrefix(s.Missing[0], "command ") {
		t.Fatalf("empty schema Missing = %q", s.Missing)
	}
}
