package api

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"

	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// TTLSeconds is the ttlSeconds request field. It distinguishes an absent
// member (apply the default) from an explicit null or the legacy string
// "infinite" (disable auto-stop) and from a number of seconds.
type TTLSeconds struct {
	Present bool
	Value   *int
}

// Seconds returns the requested number of seconds. ok is false when the field
// is absent or disables auto-stop.
func (t TTLSeconds) Seconds() (int, bool) {
	if !t.Present || t.Value == nil {
		return 0, false
	}
	return *t.Value, true
}

// Set reports whether the request wrote ttlSeconds at all.
func (t TTLSeconds) Set() bool { return t.Present }

// Disabled reports an explicit null or the legacy "infinite".
func (t TTLSeconds) Disabled() bool { return t.Present && t.Value == nil }

// MarshalJSONTo writes the number, null, or omits the member when absent.
func (t TTLSeconds) MarshalJSONTo(enc *jsontext.Encoder) error {
	if t.Value == nil {
		return enc.WriteToken(jsontext.Null)
	}
	return json.MarshalEncode(enc, *t.Value)
}

// UnmarshalJSONFrom accepts an integer, null or "infinite".
func (t *TTLSeconds) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	t.Present = true
	switch dec.PeekKind() {
	case jsontext.KindNull:
		t.Value = nil
		return dec.SkipValue()
	case jsontext.KindString:
		var value string
		if err := json.UnmarshalDecode(dec, &value); err != nil {
			return err
		}
		if value != "infinite" {
			return fmt.Errorf(`ttlSeconds must be an integer, null or "infinite"`)
		}
		t.Value = nil
		return nil
	case jsontext.KindNumber:
		var value int
		if err := json.UnmarshalDecode(dec, &value); err != nil {
			return err
		}
		t.Value = &value
		return nil
	default:
		return fmt.Errorf(`ttlSeconds must be an integer, null or "infinite"`)
	}
}

// DefaultTTLSeconds is the auto-stop length applied when create omits
// ttlSeconds.
const DefaultTTLSeconds = 3600

// CreateSandboxRequest is the full pinned request body of POST
// /api/v1/sandboxes. It carries fields outside the Phase 0.5 subset so a
// client's request parses, but handlers must reject any feature they do not
// implement instead of ignoring it.
type CreateSandboxRequest struct {
	Type       *string           `json:"type,omitzero"`
	TTLSeconds TTLSeconds        `json:"ttlSeconds,omitzero"`
	Labels     map[string]string `json:"labels,omitzero"`
	Env        map[string]string `json:"env,omitzero"`

	Environment *string             `json:"environment,omitzero"`
	NoEnv       *bool               `json:"noEnv,omitzero"`
	Snapshots   *bool               `json:"snapshots,omitzero"`
	Monitoring  *bool               `json:"monitoring,omitzero"`
	FailFast    *bool               `json:"failFast,omitzero"`
	Count       *int                `json:"count,omitzero"`
	SetupScript *string             `json:"setupScript,omitzero"`
	Name        *string             `json:"name,omitzero"`
	Visibility  *string             `json:"visibility,omitzero"`
	HostedPorts []HostedPortRequest `json:"hostedPorts,omitzero"`
	Files       []CreateSandboxFile `json:"files,omitzero"`
	Org         *string             `json:"org,omitzero"`
	TeamID      *string             `json:"teamId,omitzero"`
	From        *string             `json:"from,omitzero"`
}

// MachineType returns the requested type, or the default when omitted.
func (r CreateSandboxRequest) MachineType() string {
	if r.Type == nil {
		return machinetype.Default
	}
	return *r.Type
}

// TTL returns the effective auto-stop length. An omitted field applies
// DefaultTTLSeconds; an explicit null or "infinite" disables auto-stop.
func (r CreateSandboxRequest) TTL() *int {
	if !r.TTLSeconds.Present {
		value := DefaultTTLSeconds
		return &value
	}
	return r.TTLSeconds.Value
}

// HostedPortRequest is one entry of the create hostedPorts field. Port
// hosting is outside the Phase 0.5 subset.
type HostedPortRequest struct {
	Port      int     `json:"port"`
	Public    *bool   `json:"public,omitzero"`
	Localhost *bool   `json:"localhost,omitzero"`
	Title     *string `json:"title,omitzero"`
}

// CreateSandboxFile is one entry of the create files field. Inline files are
// outside the Phase 0.5 subset; the CLI uploads through an upload URL.
type CreateSandboxFile struct {
	Path     string `json:"path"`
	Content  string `json:"content"`
	Encoding string `json:"encoding,omitzero"`
}
