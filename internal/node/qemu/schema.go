package qemu

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
)

// Phase2Support is the result of checking a QEMU's QMP schema
// (query-qmp-schema) for every command, argument and value Phase 2 uses.
// The zero value means "available".
type Phase2Support struct {
	// QEMUVersion is the version the schema came from, for diagnostics.
	QEMUVersion string
	// Missing lists what the schema lacks, one human-readable item each,
	// in a fixed order; empty when Phase 2 is available.
	Missing []string
}

// Available reports whether every Phase 2 requirement is present.
func (s Phase2Support) Available() bool { return len(s.Missing) == 0 }

// Reason is the operator-facing text for an unavailable Phase 2, the
// FAILED_PRECONDITION message of SnapshotVM and CreateVM from_manifest.
// It is empty when Phase 2 is available.
func (s Phase2Support) Reason() string {
	if s.Available() {
		return ""
	}
	version := ""
	if s.QEMUVersion != "" {
		version = " " + s.QEMUVersion
	}
	return "Phase 2 unavailable: QEMU" + version + " lacks " + strings.Join(s.Missing, ", ")
}

// CheckPhase2Schema queries client's QMP schema and checks it with
// Phase2SchemaSupport. An error means the schema could not be read, not
// that something is missing.
func CheckPhase2Schema(ctx context.Context, client *qmp.Client) (Phase2Support, error) {
	if client == nil {
		return Phase2Support{}, fmt.Errorf("qemu: query-qmp-schema: no QMP client")
	}
	var raw jsontext.Value
	if err := client.Execute(ctx, "query-qmp-schema", nil, &raw); err != nil {
		return Phase2Support{}, fmt.Errorf("qemu: query-qmp-schema: %w", err)
	}
	s, err := Phase2SchemaSupport(raw)
	if err != nil {
		return Phase2Support{}, err
	}
	s.QEMUVersion = client.Greeting().Version.String()
	return s, nil
}

// schemaInfo is one query-qmp-schema entry (QAPI SchemaInfo). Type names
// other than commands and events are masked by QEMU ("0", "1", ...), so
// every lookup follows the references.
type schemaInfo struct {
	Name        string          `json:"name"`
	MetaType    string          `json:"meta-type"`
	ArgType     string          `json:"arg-type"`
	Members     []schemaMember  `json:"members"`
	Tag         string          `json:"tag"`
	Variants    []schemaVariant `json:"variants"`
	Values      []string        `json:"values"`
	ElementType string          `json:"element-type"`
}

// schemaMember serves object members (name, type), enum members (name) and
// alternate members (type).
type schemaMember struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type schemaVariant struct {
	Case string `json:"case"`
	Type string `json:"type"`
}

type qmpSchema map[string]schemaInfo

// command returns a command's argument object, if the command exists.
// Commands without arguments have no arg-type entry with members.
func (s qmpSchema) command(name string) (schemaInfo, bool) {
	c, ok := s[name]
	if !ok || c.MetaType != "command" {
		return schemaInfo{}, false
	}
	return s[c.ArgType], true
}

// member returns the type of obj's member name, or of the member of
// obj's variant for tagCase when tagCase is not empty.
func (s qmpSchema) member(obj schemaInfo, tagCase, name string) (string, bool) {
	if tagCase != "" {
		v, ok := s.variant(obj, tagCase)
		if !ok {
			return "", false
		}
		obj = v
	}
	for _, m := range obj.Members {
		if m.Name == name {
			return m.Type, true
		}
	}
	return "", false
}

// variant returns the object of a union's variant for tagCase.
func (s qmpSchema) variant(obj schemaInfo, tagCase string) (schemaInfo, bool) {
	for _, v := range obj.Variants {
		if v.Case == tagCase {
			t, ok := s[v.Type]
			return t, ok
		}
	}
	return schemaInfo{}, false
}

// enumHas reports whether the enum typ (directly or as a branch of an
// alternate) accepts value. Newer QEMUs list enum members, older ones
// values; both are read.
func (s qmpSchema) enumHas(typ, value string) bool {
	t, ok := s[typ]
	if !ok {
		return false
	}
	switch t.MetaType {
	case "enum":
		if slices.Contains(t.Values, value) {
			return true
		}
		return slices.ContainsFunc(t.Members, func(m schemaMember) bool { return m.Name == value })
	case "alternate":
		return slices.ContainsFunc(t.Members, func(m schemaMember) bool { return s.enumHas(m.Type, value) })
	}
	return false
}

// arrayElement returns an array type's element type, or the type itself
// when it is not an array.
func (s qmpSchema) arrayElement(typ string) schemaInfo {
	t := s[typ]
	if t.MetaType == "array" {
		return s[t.ElementType]
	}
	return t
}

// phase2Commands lists every command Phase 2 issues. The cut and its
// teardown, the launch bitmap step, the maintenance and restore sessions
// and ResetVM all use only these.
var phase2Commands = []string{
	"query-named-block-nodes",
	"transaction",
	"blockdev-backup",
	"block-dirty-bitmap-add",
	"block-dirty-bitmap-merge",
	"block-dirty-bitmap-clear",
	"block-dirty-bitmap-enable",
	"block-dirty-bitmap-disable",
	"block-dirty-bitmap-remove",
	"add-fd",
	"remove-fd",
	"blockdev-add",
	"blockdev-del",
	"nbd-server-start",
	"nbd-server-stop",
	"block-export-add",
	"block-export-del",
	"block-job-cancel",
	"query-jobs",
	"set-action",
	"system_reset",
}

// phase2Arguments lists the command arguments Phase 2 passes. tagCase
// names the union variant the argument belongs to ("" for the base).
var phase2Arguments = []struct{ command, tagCase, name string }{
	{"query-named-block-nodes", "", "flat"},
	{"transaction", "", "actions"},
	{"blockdev-backup", "", "job-id"},
	{"blockdev-backup", "", "device"},
	{"blockdev-backup", "", "target"},
	{"blockdev-backup", "", "sync"},
	{"blockdev-backup", "", "filter-node-name"},
	{"block-dirty-bitmap-add", "", "node"},
	{"block-dirty-bitmap-add", "", "name"},
	{"block-dirty-bitmap-add", "", "persistent"},
	{"block-dirty-bitmap-add", "", "granularity"},
	{"block-dirty-bitmap-add", "", "disabled"},
	{"block-dirty-bitmap-merge", "", "node"},
	{"block-dirty-bitmap-merge", "", "target"},
	{"block-dirty-bitmap-merge", "", "bitmaps"},
	{"add-fd", "", "opaque"},
	{"remove-fd", "", "fdset-id"},
	{"nbd-server-start", "", "addr"},
	{"block-export-add", "", "type"},
	{"block-export-add", "", "id"},
	{"block-export-add", "", "node-name"},
	{"block-export-add", "", "writable"},
	{"block-export-add", "nbd", "name"},
	{"block-export-add", "nbd", "bitmaps"},
	{"block-export-add", "nbd", "allocation-depth"},
	{"block-export-del", "", "id"},
	{"block-job-cancel", "", "device"},
	{"set-action", "", "reboot"},
}

// phase2TransactionActions are the transaction action types of the cut.
var phase2TransactionActions = []string{
	"block-dirty-bitmap-add",
	"block-dirty-bitmap-merge",
	"block-dirty-bitmap-clear",
	"blockdev-backup",
}

// Phase2SchemaSupport checks a raw query-qmp-schema reply for every
// command, argument, transaction action and enum value Phase 2 uses and
// lists what is missing. It errors only on a reply it cannot decode.
func Phase2SchemaSupport(raw []byte) (Phase2Support, error) {
	var entries []schemaInfo
	if err := json.Unmarshal(raw, &entries); err != nil {
		return Phase2Support{}, fmt.Errorf("qemu: decode query-qmp-schema: %w", err)
	}
	s := make(qmpSchema, len(entries))
	for _, e := range entries {
		s[e.Name] = e
	}
	var missing []string
	for _, name := range phase2Commands {
		if _, ok := s.command(name); !ok {
			missing = append(missing, "command "+name)
		}
	}
	for _, a := range phase2Arguments {
		args, ok := s.command(a.command)
		if !ok {
			continue // already reported as a missing command
		}
		if _, ok := s.member(args, a.tagCase, a.name); !ok {
			where := a.command
			if a.tagCase != "" {
				where += " type " + a.tagCase
			}
			missing = append(missing, where+" argument "+a.name)
		}
	}
	if args, ok := s.command("blockdev-backup"); ok {
		if typ, ok := s.member(args, "", "sync"); ok && !s.enumHas(typ, "none") {
			missing = append(missing, "blockdev-backup sync value none")
		}
	}
	if args, ok := s.command("set-action"); ok {
		if typ, ok := s.member(args, "", "reboot"); ok && !s.enumHas(typ, "reset") {
			missing = append(missing, "set-action reboot value reset")
		}
	}
	if args, ok := s.command("block-export-add"); ok {
		if typ, ok := s.member(args, "", "type"); ok && !s.enumHas(typ, "nbd") {
			missing = append(missing, "block-export-add type value nbd")
		}
	}
	if args, ok := s.command("transaction"); ok {
		if typ, ok := s.member(args, "", "actions"); ok {
			action := s.arrayElement(typ)
			for _, name := range phase2TransactionActions {
				if _, ok := s.variant(action, name); !ok {
					missing = append(missing, "transaction action "+name)
				}
			}
		}
	}
	return Phase2Support{Missing: missing}, nil
}
