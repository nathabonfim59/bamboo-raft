package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// phase2Harness builds a manager with or without an object store, the only
// Phase 2 configuration the Part A entry points consult.
func phase2Harness(t *testing.T, withStore bool) *harness {
	t.Helper()
	return newHarness(t, harnessOptions{configure: func(cfg *Config) {
		if withStore {
			cfg.Store = memstore.New()
		}
	}})
}

// TestSnapshotVMValidation covers the frozen contract's field order: no
// object store is FAILED_PRECONDITION before a bad chain id, which is
// INVALID_ARGUMENT before an unknown sandbox, which is NOT_FOUND.
func TestSnapshotVMValidation(t *testing.T) {
	chain := uuid.NewV7().String()
	tests := []struct {
		name    string
		store   bool
		chainID string
		create  bool
		want    codes.Code
		wantMsg string
	}{
		{
			name:    "no store",
			chainID: chain,
			want:    codes.FailedPrecondition,
			wantMsg: "this node has no object store",
		},
		{
			// A node without a store reports that before the request's own
			// chain id: the store is the first check, as in nodefake.
			name:    "no store before bad chain",
			chainID: "not-a-uuid",
			want:    codes.FailedPrecondition,
			wantMsg: "this node has no object store",
		},
		{
			name:    "bad chain",
			store:   true,
			chainID: "",
			want:    codes.InvalidArgument,
			wantMsg: "bad chain_id",
		},
		{
			name:    "unknown sandbox",
			store:   true,
			chainID: chain,
			want:    codes.NotFound,
			wantMsg: "not found",
		},
		{
			// A valid request reaches the VM: this harness's fake QEMU has
			// no QMP connection, which a real VM always has.
			name:    "valid request reaches the vm",
			store:   true,
			chainID: chain,
			create:  true,
			want:    codes.Internal,
			wantMsg: "no QMP connection",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := phase2Harness(t, tt.store)
			sandbox := ids.Sandbox()
			if tt.create {
				h.create(sandbox, nil)
			}
			_, err := h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{
				SandboxId: sandbox,
				ChainId:   tt.chainID,
			})
			if got := status.Code(err); got != tt.want {
				t.Fatalf("SnapshotVM code = %s (%v), want %s", got, err, tt.want)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tt.wantMsg) {
				t.Fatalf("SnapshotVM message = %q, want it to contain %q", msg, tt.wantMsg)
			}
		})
	}
}

// TestResetVMValidation covers the entry point's order: an unknown or
// malformed sandbox id is NOT_FOUND before any state check, and a valid one
// reaches the VM. ResetVM does not depend on the object store. This harness's
// fake QEMU has no QMP connection, which a real running VM always has.
func TestResetVMValidation(t *testing.T) {
	tests := []struct {
		name    string
		sandbox string
		create  bool
		want    codes.Code
		wantMsg string
	}{
		{name: "unknown sandbox", sandbox: ids.Sandbox(), want: codes.NotFound, wantMsg: "not found"},
		{name: "invalid sandbox id", sandbox: "not-a-sandbox", want: codes.NotFound, wantMsg: "not found"},
		{name: "valid request reaches the vm", sandbox: ids.Sandbox(), create: true, want: codes.FailedPrecondition, wantMsg: "no QMP connection"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := phase2Harness(t, false)
			if tt.create {
				h.create(tt.sandbox, nil)
			}
			_, err := h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: tt.sandbox})
			if got := status.Code(err); got != tt.want {
				t.Fatalf("ResetVM code = %s (%v), want %s", got, err, tt.want)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tt.wantMsg) {
				t.Fatalf("ResetVM message = %q, want it to contain %q", msg, tt.wantMsg)
			}
		})
	}
}

// TestCreateVMFromManifestValidation covers the from_manifest checks at the
// CreateVM entry. Every request leaves resources unset on purpose: the
// from_manifest checks run before resource validation and reservation, so a
// refusal that mentions resources would prove the wrong order. Each refused
// call must also leave no VM record behind.
func TestCreateVMFromManifestValidation(t *testing.T) {
	validKey := snapformat.ManifestKey(uuid.NewV7().String(), 0)
	tests := []struct {
		name         string
		store        bool
		fromSnapshot string
		key          string
		want         codes.Code
		wantMsg      string
	}{
		{
			name:         "both sources",
			store:        true,
			fromSnapshot: "snap-x",
			key:          validKey,
			want:         codes.InvalidArgument,
			wantMsg:      "from_snapshot and from_manifest are exclusive",
		},
		{
			name:    "no store",
			key:     validKey,
			want:    codes.FailedPrecondition,
			wantMsg: "this node has no object store",
		},
		{
			// The store check precedes the key parse, as in nodefake.
			name:    "no store before bad key",
			key:     "chunks/abc",
			want:    codes.FailedPrecondition,
			wantMsg: "this node has no object store",
		},
		{
			name:    "bad key",
			store:   true,
			key:     "chunks/abc",
			want:    codes.InvalidArgument,
			wantMsg: "not a manifest key",
		},
		{
			// A valid request reaches the store (phase2_restore_test.go
			// covers the store-backed checks).
			name:    "missing manifest",
			store:   true,
			key:     validKey,
			want:    codes.NotFound,
			wantMsg: "not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := phase2Harness(t, tt.store)
			sandbox := ids.Sandbox()
			_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
				SandboxId:    sandbox,
				FromManifest: tt.key,
				FromSnapshot: tt.fromSnapshot,
			})
			if got := status.Code(err); got != tt.want {
				t.Fatalf("CreateVM code = %s (%v), want %s", got, err, tt.want)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tt.wantMsg) {
				t.Fatalf("CreateVM message = %q, want it to contain %q", msg, tt.wantMsg)
			}
			if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: sandbox}); status.Code(err) != codes.NotFound {
				t.Fatalf("GetVM after refused CreateVM = %v (code %s), want NOT_FOUND and no record", err, status.Code(err))
			}
		})
	}
}

// TestCreateVMExistingIDIgnoresBadFromManifest matches the existing-id
// precedence the from_snapshot path already has: a retry for an id this node
// already owns joins that record and returns it without inspecting any other
// request field, so an invalid from_manifest is not this caller's business
// and never reaches a reservation path.
func TestCreateVMExistingIDIgnoresBadFromManifest(t *testing.T) {
	h := phase2Harness(t, true)
	id := h.create(ids.Sandbox(), nil).GetSandboxId()
	started := h.launcher.count()
	tests := []struct {
		name string
		req  *nodev1.CreateVMRequest
	}{
		{
			name: "bad key",
			req:  &nodev1.CreateVMRequest{SandboxId: id, FromManifest: "chunks/abc"},
		},
		{
			name: "both sources",
			req: &nodev1.CreateVMRequest{
				SandboxId:    id,
				FromManifest: snapformat.ManifestKey(uuid.NewV7().String(), 0),
				FromSnapshot: "snap-x",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			vm, err := h.mg.CreateVM(ctx, tt.req)
			if err != nil {
				t.Fatalf("CreateVM existing id = %v, want the joined VM", err)
			}
			if vm.GetSandboxId() != id || vm.GetState() != nodev1.VMState_VM_STATE_READY {
				t.Fatalf("CreateVM existing id = %+v, want READY %s", vm, id)
			}
		})
	}
	// A join must not launch anything or replace the record.
	if got := h.launcher.count(); got != started {
		t.Fatalf("launcher starts = %d, want %d", got, started)
	}
}

// TestManagerKeepsPhase2Config proves the node command's Phase 2 settings
// reach the manager unchanged; M2 reads them from there.
func TestManagerKeepsPhase2Config(t *testing.T) {
	store := memstore.New()
	const cacheBytes = int64(8) << 30
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.Store = store
		cfg.ChunkCacheBytes = cacheBytes
	}})
	if h.mg.cfg.Store != store {
		t.Fatalf("manager object store = %v, want the configured store", h.mg.cfg.Store)
	}
	if h.mg.cfg.ChunkCacheBytes != cacheBytes {
		t.Fatalf("manager chunk cache bytes = %d, want %d", h.mg.cfg.ChunkCacheBytes, cacheBytes)
	}
}
