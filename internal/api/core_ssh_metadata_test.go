package api

import (
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// TestSandboxSSHEndpointConversion proves the durable SSH pair is exposed as
// the pinned nullable sshEndpoint only while a machine is running. The host key
// is never part of the public sandbox object, and a malformed row that keeps a
// pair in a settled state must not advertise a dead endpoint.
func TestSandboxSSHEndpointConversion(t *testing.T) {
	const (
		ipv4Endpoint = "203.0.113.10:22001"
		ipv6Endpoint = "[2001:db8::10]:22001"
	)
	tests := []struct {
		name     string
		state    db.SandboxState
		ssh      *db.SSHAccess
		want     *string
		wantJSON any
	}{
		{
			name:     "ready without metadata",
			state:    db.StateReady,
			wantJSON: nil,
		},
		{
			name:     "ready with IPv4 metadata",
			state:    db.StateReady,
			ssh:      &db.SSHAccess{Endpoint: ipv4Endpoint, HostKey: "ssh-ed25519 AAAA test"},
			want:     new(ipv4Endpoint),
			wantJSON: ipv4Endpoint,
		},
		{
			name:     "idle with IPv6 metadata",
			state:    db.StateIdle,
			ssh:      &db.SSHAccess{Endpoint: ipv6Endpoint, HostKey: "ssh-ed25519 AAAA test"},
			want:     new(ipv6Endpoint),
			wantJSON: ipv6Endpoint,
		},
		{
			name:     "running with metadata",
			state:    db.StateRunning,
			ssh:      &db.SSHAccess{Endpoint: ipv4Endpoint, HostKey: "ssh-ed25519 AAAA test"},
			want:     new(ipv4Endpoint),
			wantJSON: ipv4Endpoint,
		},
		{
			name:     "archived stale metadata is hidden",
			state:    db.StateArchived,
			ssh:      &db.SSHAccess{Endpoint: ipv4Endpoint, HostKey: "ssh-ed25519 AAAA test"},
			wantJSON: nil,
		},
		{
			name:     "provisioning stale metadata is hidden",
			state:    db.StateProvisioning,
			ssh:      &db.SSHAccess{Endpoint: ipv4Endpoint, HostKey: "ssh-ed25519 AAAA test"},
			wantJSON: nil,
		},
		{
			name:     "error stale metadata is hidden",
			state:    db.StateError,
			ssh:      &db.SSHAccess{Endpoint: ipv4Endpoint, HostKey: "ssh-ed25519 AAAA test"},
			wantJSON: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := db.Sandbox{ID: "bx_23456789", State: test.state, SSH: test.ssh}
			converted := sandboxFromRow(row)
			switch {
			case test.want == nil && converted.SSHEndpoint != nil:
				t.Fatalf("sshEndpoint = %q, want absent", *converted.SSHEndpoint)
			case test.want != nil && (converted.SSHEndpoint == nil || *converted.SSHEndpoint != *test.want):
				t.Fatalf("sshEndpoint = %v, want %q", converted.SSHEndpoint, *test.want)
			}

			shape := jsonMap(t, converted)
			value, present := shape["sshEndpoint"]
			if !present {
				t.Fatal("sshEndpoint is not serialized as the pinned nullable field")
			}
			if value != test.wantJSON {
				t.Fatalf("sshEndpoint JSON = %v, want %v", value, test.wantJSON)
			}
			if _, leaked := shape["hostKey"]; leaked {
				t.Fatal("sandbox object exposed a host key")
			}

			info := jsonMap(t, NewSandboxInfoResponse(converted))
			infoSandbox, ok := info["sandbox"].(map[string]any)
			if !ok {
				t.Fatal("sandbox info envelope has no sandbox object")
			}
			if infoSandbox["sshEndpoint"] != test.wantJSON {
				t.Fatalf("sandbox.info sshEndpoint = %v, want %v", infoSandbox["sshEndpoint"], test.wantJSON)
			}
			list := jsonMap(t, NewSandboxListResponse([]Sandbox{converted}, NewPageInfo(nil, 10)))
			sandboxes, ok := list["sandboxes"].([]any)
			if !ok || len(sandboxes) != 1 {
				t.Fatal("sandbox list envelope has no sandboxes")
			}
			listed, ok := sandboxes[0].(map[string]any)
			if !ok {
				t.Fatal("sandbox list entry is not an object")
			}
			if listed["sshEndpoint"] != test.wantJSON {
				t.Fatalf("sandbox.list sshEndpoint = %v, want %v", listed["sshEndpoint"], test.wantJSON)
			}
		})
	}
}
