package server

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

// s3TestStoreConfig is one validated object-store configuration.
func s3TestStoreConfig(secret string) *s3.Config {
	return &s3.Config{
		Endpoint:        "https://s3.example.test:3900",
		Region:          "garage",
		Bucket:          "bbraft",
		PathStyle:       true,
		AccessKeyID:     "GKEXAMPLE",
		SecretAccessKey: secret,
		LocationHint:    "wnam",
		CreateBucket:    true,
	}
}

func TestOpenObjectStore(t *testing.T) {
	mem := memstore.New()
	tests := []struct {
		name     string
		cfg      *config.Config
		injected objstore.Store
		want     objstore.Store
	}{
		{name: "phase 1 mode", cfg: &config.Config{}},
		{name: "nil config"},
		{name: "injected store", cfg: &config.Config{}, injected: mem, want: mem},
		{name: "injected wins over configured bucket", cfg: &config.Config{ObjectStore: s3TestStoreConfig("secret")}, injected: mem, want: mem},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := openObjectStore(t.Context(), tc.cfg, tc.injected)
			if err != nil {
				t.Fatalf("openObjectStore: %v", err)
			}
			if got != tc.want {
				t.Fatalf("store = %v, want %v", got, tc.want)
			}
		})
	}
}

// s3RejectedConfig is a configuration s3.New refuses before any request: the
// bucket name is not a valid S3 bucket name.
func s3RejectedConfig(secret string) *s3.Config {
	cfg := s3TestStoreConfig(secret)
	cfg.Bucket = "Not_A_Bucket"
	return cfg
}

// TestOpenObjectStoreConfiguredFailure pins the wire-up: a configured bucket
// reaches s3.New, and a configuration it refuses fails start-up with a clear
// error and no credential.
func TestOpenObjectStoreConfiguredFailure(t *testing.T) {
	const sentinel = "SENTINEL-s3-secret"
	_, err := openObjectStore(t.Context(), &config.Config{ObjectStore: s3RejectedConfig(sentinel)}, nil)
	if err == nil {
		t.Fatal("an invalid bucket name was accepted")
	}
	if !strings.HasPrefix(err.Error(), "server: object store:") {
		t.Fatalf("error = %q, want the server: object store: prefix", err)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatal("error leaked the S3 secret")
	}
}

// TestRunFailsStartupBeforeOpeningRoles pins that a broken object-store
// configuration fails Run before any role store is opened.
func TestRunFailsStartupBeforeOpeningRoles(t *testing.T) {
	var opened atomic.Bool
	cfg := &config.Config{
		Roles:       config.Roles{config.RoleAPI: true},
		ObjectStore: s3RejectedConfig("SENTINEL-s3-secret"),
	}
	err := Run(t.Context(), cfg, Options{
		StoreOpener: func(context.Context, config.Role) (*db.Store, error) {
			opened.Store(true)
			return nil, errors.New("store opened")
		},
	})
	if err == nil || !strings.HasPrefix(err.Error(), "server: object store:") {
		t.Fatalf("Run error = %v, want the object-store start-up failure", err)
	}
	if opened.Load() {
		t.Fatal("Run opened a role store despite the object-store failure")
	}
}
