package guest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// helperFixtureEnvName selects a misbehaving child from the test binary's
// TestMain. It is test-only and must match helper_fixture_test.go.
const helperFixtureEnvName = "BBRAFT_GUEST_FILE_HELPER_FIXTURE"

// helperPIDFileEnvName matches helper_fixture_test.go's env var.
const helperPIDFileEnvName = "BBRAFT_GUEST_FILE_HELPER_PIDFILE"

// newFixtureHelper builds a helper client whose child is a protocol fixture.
func newFixtureHelper(t *testing.T, fixture string) *helperFiles {
	return newFixtureHelperEnv(t, fixture)
}

// newFixtureHelperEnv adds fixture-specific environment entries.
func newFixtureHelperEnv(t *testing.T, fixture string, extra ...string) *helperFiles {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	homeRoot, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tmpRoot, err := os.OpenRoot(t.TempDir())
	if err != nil {
		homeRoot.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		homeRoot.Close()
		tmpRoot.Close()
	})
	uid, gid := os.Getuid(), os.Getgid()
	return &helperFiles{
		exe:      exe,
		homeRoot: homeRoot,
		tmpRoot:  tmpRoot,
		uid:      uid,
		gid:      gid,
		credential: func() (*syscall.Credential, error) {
			return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid), NoSetGroups: true}, nil
		},
		extraEnv: append([]string{helperFixtureEnvName + "=" + fixture}, extra...),
	}
}

// readFixturePID waits briefly for the fixture's pid file.
func readFixturePID(t *testing.T, p string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		b, err := os.ReadFile(p)
		if err == nil {
			pid, cerr := strconv.Atoi(strings.TrimSpace(string(b)))
			if cerr != nil {
				t.Fatalf("pid file %q: %v", p, cerr)
			}
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture did not write its pid: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitFixtureReaped proves the helper process is gone (and not a zombie
// lingering in /proc) after the caller supposedly reaped it.
func waitFixtureReaped(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid)))
		if errors.Is(err, fs.ErrNotExist) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper pid %d still exists after the handler returned", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestReadFileSendErrorReapsHelper makes the gRPC send callback fail on the
// info and on the data frame: the handler must reap the child before
// returning the client error, never leaving a zombie.
func TestReadFileSendErrorReapsHelper(t *testing.T) {
	for _, tc := range []struct {
		fixture string
		failAt  int
	}{
		{"read-info-send-error", 0},
		{"read-data-send-error", 1},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			pidFile := filepath.Join(t.TempDir(), "helper.pid")
			h := newFixtureHelperEnv(t, tc.fixture, helperPIDFileEnvName+"="+pidFile)
			sentinel := errors.New("grpc send failed")
			frames := 0
			err := h.readFile(t.Context(), "x", 0, func(*nodev1.ReadFileResponse) error {
				if frames == tc.failAt {
					frames++
					return sentinel
				}
				frames++
				return nil
			})
			if !errors.Is(err, sentinel) {
				t.Fatalf("readFile send error = %v, want the client error", err)
			}
			waitFixtureReaped(t, readFixturePID(t, pidFile))
		})
	}
}

// TestHelperUploadReadyRejection proves the child's header decision arrives
// before any upload data: a rejected header surfaces the child's status, not
// an EPIPE from a dead helper.
func TestHelperUploadReadyRejection(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "helper.pid")
	h := newFixtureHelperEnv(t, "put-header-reject", helperPIDFileEnvName+"="+pidFile)
	up, err := h.newUpload(t.Context(), "x", 0)
	if up != nil {
		t.Fatal("newUpload returned an upload after the child rejected the header")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("rejected header = %v (%v), want InvalidArgument", err, got)
	}
	if !strings.Contains(status.Convert(err).Message(), "header rejected by child") {
		t.Fatalf("rejected header message = %q", status.Convert(err).Message())
	}
	waitFixtureReaped(t, readFixturePID(t, pidFile))
}

// TestHelperUploadReadyCancel cancels while the upload is waiting for the
// child's header decision: newUpload returns Canceled and the child is reaped
// within the readiness abort bound.
func TestHelperUploadReadyCancel(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "helper.pid")
	h := newFixtureHelperEnv(t, "put-ready-hang", helperPIDFileEnvName+"="+pidFile)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := h.newUpload(ctx, "x", 0)
		done <- err
	}()
	// Wait for the child to prove it started and reached the fixture before
	// canceling, so the readiness-wait branch is really exercised even on a
	// loaded machine.
	pid := readFixturePID(t, pidFile)
	cancel()
	err := <-done
	if got := status.Code(err); got != codes.Canceled {
		t.Fatalf("canceled readiness wait = %v (%v), want Canceled", err, got)
	}
	if elapsed := time.Since(start); elapsed > 2*helperReadyAbortTimeout {
		t.Fatalf("canceled readiness wait took %s", elapsed)
	}
	waitFixtureReaped(t, pid)
}

// TestHelperProtocolFailures proves each malformed child exchange is a
// protocol/helper failure, never a silent success or a trusted operation
// error.
func TestHelperProtocolFailures(t *testing.T) {
	for _, tc := range []struct {
		fixture string
		want    string
	}{
		{"error-ok", "invalid code"},
		{"error-then-crash", "failed after reporting"},
		{"result-then-trailing", "trailing frame"},
		{"hang-after-result", "did not close"},
		{"oversized-frame", "frame length"},
		{"exit-now", "file helper"},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			h := newFixtureHelper(t, tc.fixture)
			var out fileInfoDTO
			err := h.simple(t.Context(), helperRequest{Op: helperOpStat, Path: "x"}, &out)
			if err == nil {
				t.Fatal("malformed helper exchange succeeded")
			}
			if got := status.Code(err); got != codes.Internal {
				t.Fatalf("malformed helper exchange = %v (%v), want Internal", err, got)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tc.want) {
				t.Fatalf("error %q does not mention %q", msg, tc.want)
			}
		})
	}
}

// TestHelperUploadTransportFailure makes the upload handshake and commit
// paths fail cleanly when the child disappears immediately.
func TestHelperUploadTransportFailure(t *testing.T) {
	h := newFixtureHelper(t, "exit-now")
	up, err := h.newUpload(t.Context(), "upload", 0)
	if err == nil {
		err = up.write([]byte("data"))
		if err == nil {
			_, err = up.commit(t.Context())
		}
		if abortErr := up.abort(t.Context()); abortErr != nil {
			t.Logf("abort after transport failure: %v", abortErr)
		}
	}
	if err == nil {
		t.Fatal("upload against a vanished helper succeeded")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("upload transport failure = %v (%v), want Internal", err, got)
	}
}

// TestValidateHelperEOFRejectsUnbounded pins the mandatory trailing-frame
// check: a stream the guest cannot bound is a protocol error, never skipped.
func TestValidateHelperEOFRejectsUnbounded(t *testing.T) {
	err := validateHelperEOF(
		io.NopCloser(strings.NewReader("")),
		bufio.NewReader(strings.NewReader("")),
	)
	if !errors.Is(err, errHelperProtocol) {
		t.Fatalf("validateHelperEOF on an unbounded stream = %v, want errHelperProtocol", err)
	}
}

// TestHelperStartupErrorIsTyped proves a root-open failure reaches every file
// operation as the child's typed INVALID_ARGUMENT, never INTERNAL or a hang.
func TestHelperStartupErrorIsTyped(t *testing.T) {
	const want = "permission denied on the logical root /home/user"
	h := newFixtureHelper(t, "root-denied")
	if _, err := h.statFile(t.Context(), "x"); status.Code(err) != codes.InvalidArgument || !strings.Contains(status.Convert(err).Message(), want) {
		t.Fatalf("stat startup error = %v, want InvalidArgument with %q", err, want)
	}
	if err := h.readFile(t.Context(), "x", 0, func(*nodev1.ReadFileResponse) error { return nil }); status.Code(err) != codes.InvalidArgument || !strings.Contains(status.Convert(err).Message(), want) {
		t.Fatalf("read startup error = %v, want InvalidArgument with %q", err, want)
	}
	if _, err := h.listFiles(t.Context(), "x", 0); status.Code(err) != codes.InvalidArgument || !strings.Contains(status.Convert(err).Message(), want) {
		t.Fatalf("list startup error = %v, want InvalidArgument with %q", err, want)
	}
	if _, err := h.newUpload(t.Context(), "x", 0); status.Code(err) != codes.InvalidArgument || !strings.Contains(status.Convert(err).Message(), want) {
		t.Fatalf("upload startup error = %v, want InvalidArgument with %q", err, want)
	}
}

// TestHelperListProtocolFailures proves malformed streamed list results are
// protocol errors: count, order, path relation, limit and batch bounds.
func TestHelperListProtocolFailures(t *testing.T) {
	for _, tc := range []struct {
		fixture string
		want    string
		limit   uint32
	}{
		{"list-bad-count", "exceed the declared count", 10},
		{"list-out-of-order", "not strictly ordered", 10},
		{"list-wrong-path", "not under", 10},
		{"list-over-limit", "exceeds the", 1},
		{"list-bad-end", "count mismatch", 10},
		{"list-oversized-batch", "batch of", 10},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			pidFile := filepath.Join(t.TempDir(), "helper.pid")
			h := newFixtureHelperEnv(t, tc.fixture, helperPIDFileEnvName+"="+pidFile)
			_, err := h.listFiles(t.Context(), "/tmp", tc.limit)
			if err == nil {
				t.Fatal("malformed list result succeeded")
			}
			if got := status.Code(err); got != codes.Internal {
				t.Fatalf("malformed list result = %v (%v), want Internal", err, got)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tc.want) {
				t.Fatalf("error %q does not mention %q", msg, tc.want)
			}
			if tc.fixture == "list-oversized-batch" {
				// decodeListBatch fails after the frame was read; the child
				// must be killed and reaped before listFiles returns.
				waitFixtureReaped(t, readFixturePID(t, pidFile))
			}
		})
	}
}

// TestDecodeListBatchBounds checks the per-frame batch bounds before any
// entry is accepted into the parent's response.
func TestDecodeListBatchBounds(t *testing.T) {
	many := make([]fileInfoDTO, maxListBatchEntries+1)
	for i := range many {
		many[i] = fileInfoDTO{Path: "/tmp/a", Name: "a", Type: 1, Mode: 0o644}
	}
	tooMany, err := json.Marshal(many)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeListBatch(tooMany); !errors.Is(err, errHelperProtocol) {
		t.Fatalf("batch of %d entries = %v, want errHelperProtocol", len(many), err)
	}
	if _, err := decodeListBatch(bytes.Repeat([]byte("x"), listBatchBytes+1)); !errors.Is(err, errHelperProtocol) {
		t.Fatalf("oversized batch = %v, want errHelperProtocol", err)
	}
	if _, err := decodeListBatch(nil); !errors.Is(err, errHelperProtocol) {
		t.Fatalf("empty batch = %v, want errHelperProtocol", err)
	}
	one, err := json.Marshal([]fileInfoDTO{{Path: "/tmp/a", Name: "a", Type: 1, Mode: 0o644}})
	if err != nil {
		t.Fatal(err)
	}
	if batch, err := decodeListBatch(one); err != nil || len(batch) != 1 {
		t.Fatalf("valid batch = %v, %v", batch, err)
	}
}

// TestFileInfoDTOFieldBounds keeps decoded helper fields within filesystem
// path and name limits and rejects invalid enums/modes.
func TestFileInfoDTOFieldBounds(t *testing.T) {
	valid := fileInfoDTO{Path: "/tmp/a", Name: "a", Type: 1, Mode: 0o644}
	if err := validateFileInfoDTO(valid); err != nil {
		t.Fatalf("valid DTO: %v", err)
	}
	longName := strings.Repeat("n", maxNameLen+1)
	for _, tc := range []struct {
		name string
		dto  fileInfoDTO
	}{
		{"long name", fileInfoDTO{Path: "/tmp/" + longName, Name: longName, Type: 1}},
		{"relative path", fileInfoDTO{Path: "relative", Name: "relative", Type: 1}},
		{"unclean path", fileInfoDTO{Path: "/tmp/a/../a", Name: "a", Type: 1}},
		{"name mismatch", fileInfoDTO{Path: "/tmp/a", Name: "b", Type: 1}},
		{"slash in name", fileInfoDTO{Path: "/tmp/a", Name: "a/b", Type: 1}},
		{"mode too large", fileInfoDTO{Path: "/tmp/a", Name: "a", Type: 1, Mode: 0o10000}},
		{"invalid type", fileInfoDTO{Path: "/tmp/a", Name: "a", Type: 99}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateFileInfoDTO(tc.dto); !errors.Is(err, errHelperProtocol) {
				t.Fatalf("validateFileInfoDTO = %v, want errHelperProtocol", err)
			}
		})
	}
}

// TestHelperInvalidFileInfoIsInternal proves a validated-after-success
// FileInfo failure maps to INTERNAL (not UNKNOWN) through stat, mkdir and
// chmod; the child has already been reaped by finishOK.
func TestHelperInvalidFileInfoIsInternal(t *testing.T) {
	h := newFixtureHelper(t, "result-invalid-info")
	check := func(name string, err error) {
		t.Helper()
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf("%s invalid FileInfo = %v (%v), want Internal", name, err, got)
		}
		if !strings.Contains(status.Convert(err).Message(), "file helper protocol") {
			t.Fatalf("%s invalid FileInfo message = %q", name, status.Convert(err).Message())
		}
	}
	_, err := h.statFile(t.Context(), "x")
	check("stat", err)
	_, err = h.makeDir(t.Context(), "x", 0o755)
	check("mkdir", err)
	_, err = h.chmodFile(t.Context(), "x", 0o644)
	check("chmod", err)
}

// TestHelperStartupCode keeps permission-denied root startup failures as
// client errors and everything else internal.
func TestHelperStartupCode(t *testing.T) {
	perm := logicalRootError(homeSpec, fs.ErrPermission)
	if helperStartupCode(perm) != codes.InvalidArgument {
		t.Fatalf("permission root error code = %v, want InvalidArgument", helperStartupCode(perm))
	}
	if !strings.Contains(perm.Error(), "permission denied on the logical root /home/user") {
		t.Fatalf("permission root error = %q", perm.Error())
	}
	ioErr := logicalRootError(homeSpec, syscall.EMFILE)
	if helperStartupCode(ioErr) != codes.Internal {
		t.Fatalf("I/O root error code = %v, want Internal", helperStartupCode(ioErr))
	}
	if !strings.Contains(ioErr.Error(), "cannot open the logical root /home/user") {
		t.Fatalf("I/O root error = %q", ioErr.Error())
	}
}

// TestHelperInternalStartupError proves an internal root failure survives the
// protocol as INTERNAL rather than being flattened to INVALID_ARGUMENT.
func TestHelperInternalStartupError(t *testing.T) {
	h := newFixtureHelper(t, "root-internal-error")
	_, err := h.statFile(t.Context(), "x")
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("internal startup error = %v (%v), want Internal", err, got)
	}
	if !strings.Contains(status.Convert(err).Message(), "bad file descriptor") {
		t.Fatalf("internal startup message = %q", status.Convert(err).Message())
	}
}
