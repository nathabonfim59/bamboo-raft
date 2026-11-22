package cli

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

var (
	cliTestShareID   = "shr_" + strings.Repeat("a", 32)
	cliTestShareID2  = "shr_" + strings.Repeat("b", 32)
	cliTestShareCode = "bsh_" + strings.Repeat("a", 52)
)

func cliTestShare(id, prefix string, status api.SnapshotShareStatus, uses int64, lastUsed *time.Time) api.SnapshotShare {
	share := api.SnapshotShare{
		ID:         id,
		CodePrefix: prefix,
		Status:     status,
		UseCount:   uses,
		LastUsedAt: lastUsed,
		CreatedAt:  cliTestTime,
	}
	if status == api.SnapshotShareRevoked {
		share.RevokedAt = new(cliTestTime.Add(time.Hour))
	}
	return share
}

func TestSnapshotSharePrintsCodeOnce(t *testing.T) {
	created := api.SnapshotShareCreatedResponse{
		SuccessBase: api.SuccessBase{OK: true, Type: api.TypeSnapshotShareCreated},
		Name:        cliTestSnapshotName,
		ShareCode:   cliTestShareCode,
		Share:       cliTestShare(cliTestShareID, "bsh_aaaaaaaa", api.SnapshotShareActive, 0, nil),
	}

	t.Run("human prints code once with notice", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/api/v1/saved-snapshots/"+cliTestSnapshotName+"/shares" {
				t.Errorf("request = %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			cliTestWriteJSON(t, w, http.StatusCreated, created)
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "snapshot", "share", cliTestSnapshotName); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		combined := env.stdout.String() + env.stderr.String()
		if count := strings.Count(combined, cliTestShareCode); count != 1 {
			t.Fatalf("share code count = %d, want 1:\n%s", count, combined)
		}
		lower := strings.ToLower(env.stdout.String())
		if !strings.Contains(lower, "only once") {
			t.Fatalf("output missing one-time notice:\n%s", env.stdout)
		}
	})

	t.Run("json matches v1 API schema and prints code once", func(t *testing.T) {
		validator := compat.Load(t)
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusCreated, created)
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "snapshot", "share", cliTestSnapshotName, "--json"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		raw := strings.TrimSpace(env.stdout.String())
		lifecycleTestValidate(t, validator, "createSnapshotShare", http.StatusCreated, []byte(raw))
		if count := strings.Count(raw, cliTestShareCode); count != 1 {
			t.Fatalf("share code count = %d, want 1", count)
		}
	})

	t.Run("missing code is invalid_response", func(t *testing.T) {
		broken := created
		broken.ShareCode = ""
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusCreated, broken)
		}, nil)
		if status := env.run(t, "snapshot", "share", cliTestSnapshotName); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "invalid_response" {
			t.Fatalf("lines = %v, want invalid_response", lines)
		}
	})
}

func TestSnapshotSharesTableAndJSON(t *testing.T) {
	usedAt := cliTestTime.Add(15 * time.Minute)
	listResp := api.SnapshotShareListResponse{
		SuccessBase: api.SuccessBase{OK: true, Type: api.TypeSnapshotShareList},
		Name:        cliTestSnapshotName,
		Shares: []api.SnapshotShare{
			cliTestShare(cliTestShareID, "bsh_aaaaaaaa", api.SnapshotShareActive, 3, &usedAt),
			cliTestShare(cliTestShareID2, "bsh_bbbbbbbb", api.SnapshotShareRevoked, 0, nil),
		},
	}

	t.Run("table columns and values", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/saved-snapshots/"+cliTestSnapshotName+"/shares" {
				t.Errorf("request = %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			cliTestWriteJSON(t, w, http.StatusOK, listResp)
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "snapshot", "shares", cliTestSnapshotName); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		out := env.stdout.String()
		for _, want := range []string{
			"ID", "PREFIX", "STATUS", "USES", "LAST USED", "CREATED",
			cliTestShareID, "bsh_aaaaaaaa", "active", "3", usedAt.UTC().Format(time.RFC3339),
			cliTestShareID2, "bsh_bbbbbbbb", "revoked", "-", cliTestTime.UTC().Format(time.RFC3339),
		} {
			if !strings.Contains(out, want) {
				t.Errorf("table output missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("empty human message", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, api.SnapshotShareListResponse{
				SuccessBase: api.SuccessBase{OK: true, Type: api.TypeSnapshotShareList},
				Name:        cliTestSnapshotName,
				Shares:      []api.SnapshotShare{},
			})
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "snapshot", "shares", cliTestSnapshotName); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		if got := strings.TrimSpace(env.stdout.String()); !strings.Contains(got, "no shares") {
			t.Fatalf("stdout = %q, want no shares", got)
		}
	})

	t.Run("json matches v1 API schema", func(t *testing.T) {
		validator := compat.Load(t)
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, listResp)
		}, nil)
		if status := env.run(t, "snapshot", "shares", cliTestSnapshotName); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		raw := strings.TrimSpace(env.stdout.String())
		lifecycleTestValidate(t, validator, "listSnapshotShares", http.StatusOK, []byte(raw))
	})
}

func TestSnapshotUnshareHumanAndJSON(t *testing.T) {
	revoked := api.SnapshotShareRevokedResponse{
		SuccessBase: api.SuccessBase{OK: true, Type: api.TypeSnapshotShareRevoked},
		Name:        cliTestSnapshotName,
		Share:       cliTestShare(cliTestShareID, "bsh_aaaaaaaa", api.SnapshotShareRevoked, 1, nil),
	}

	t.Run("human confirms revocation", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			wantPath := "/api/v1/saved-snapshots/" + cliTestSnapshotName + "/shares/" + cliTestShareID
			if r.Method != http.MethodDelete || r.URL.Path != wantPath {
				t.Errorf("request = %s %s, want DELETE %s", r.Method, r.URL.Path, wantPath)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			cliTestWriteJSON(t, w, http.StatusOK, revoked)
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "snapshot", "unshare", cliTestSnapshotName, cliTestShareID); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		out := env.stdout.String()
		if !strings.Contains(out, cliTestShareID) || !strings.Contains(out, cliTestSnapshotName) {
			t.Fatalf("stdout = %q, want share id and snapshot name", out)
		}
	})

	t.Run("json matches v1 API schema", func(t *testing.T) {
		validator := compat.Load(t)
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, revoked)
		}, nil)
		if status := env.run(t, "snapshot", "unshare", cliTestSnapshotName, cliTestShareID); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		raw := strings.TrimSpace(env.stdout.String())
		lifecycleTestValidate(t, validator, "revokeSnapshotShare", http.StatusOK, []byte(raw))
	})

	t.Run("404 exits 1", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(http.StatusNotFound, "not_found", "share not found"))
		}, nil)
		if status := env.run(t, "snapshot", "unshare", cliTestSnapshotName, cliTestShareID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "not_found" {
			t.Fatalf("lines = %v", lines)
		}
	})

	t.Run("mismatched share id is invalid_response", func(t *testing.T) {
		wrong := revoked
		wrong.Share.ID = cliTestShareID2
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, wrong)
		}, nil)
		if status := env.run(t, "snapshot", "unshare", cliTestSnapshotName, cliTestShareID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "invalid_response" {
			t.Fatalf("lines = %v, want invalid_response", lines)
		}
	})
}

func TestSnapshotShareUsageErrorsMakeNoRequest(t *testing.T) {
	cases := [][]string{
		{"snapshot", "share"},
		{"snapshot", "share", cliTestSnapshotName, "extra"},
		{"snapshot", "shares"},
		{"snapshot", "shares", cliTestSnapshotName, "extra"},
		{"snapshot", "unshare"},
		{"snapshot", "unshare", cliTestSnapshotName},
		{"snapshot", "unshare", cliTestSnapshotName, cliTestShareID, "extra"},
	}
	for _, args := range cases {
		var hits atomic.Int64
		env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
		if status := env.run(t, args...); status != 2 {
			t.Fatalf("args %v: exit status = %d, want 2", args, status)
		}
		if hits.Load() != 0 {
			t.Fatalf("args %v: server received %d requests, want 0", args, hits.Load())
		}
	}
}

func TestSnapshotShareSubcommandsEscapePathSegments(t *testing.T) {
	const name = "my snap/é"
	const shareID = "shr/weird id"
	cases := []struct {
		name     string
		args     []string
		wantMeth string
		wantPath string
	}{
		{
			name:     "share",
			args:     []string{"snapshot", "share", name},
			wantMeth: http.MethodPost,
			wantPath: "/api/v1/saved-snapshots/" + name + "/shares",
		},
		{
			name:     "shares",
			args:     []string{"snapshot", "shares", name},
			wantMeth: http.MethodGet,
			wantPath: "/api/v1/saved-snapshots/" + name + "/shares",
		},
		{
			name:     "unshare",
			args:     []string{"snapshot", "unshare", name, shareID},
			wantMeth: http.MethodDelete,
			wantPath: "/api/v1/saved-snapshots/" + name + "/shares/" + shareID,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tc.wantMeth || r.URL.Path != tc.wantPath {
					t.Errorf("request = %s %s, want %s %s", r.Method, r.URL.Path, tc.wantMeth, tc.wantPath)
				}
				cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(http.StatusNotFound, "not_found", "not found"))
			}, nil)
			if status := env.run(t, tc.args...); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
		})
	}
}
