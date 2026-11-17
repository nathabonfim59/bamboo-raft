package cli

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// pushTestUploadToken is a fake signed-URL secret. Tests assert it never
// appears in command output.
const pushTestUploadToken = "push-test-secret-token"

// pushTestCapture records what the mock API and upload host saw. Every field
// is mutex-protected because HTTP handlers run on server goroutines.
type pushTestCapture struct {
	mu          sync.Mutex
	urlRequests int
	uploads     int
	commands    []string
	entries     map[string]archiveTarEntry
}

// pushTestAddURLCall counts one upload-url request.
func (c *pushTestCapture) pushTestAddURLCall() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.urlRequests++
}

// pushTestURLCalls returns the number of upload-url requests.
func (c *pushTestCapture) pushTestURLCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.urlRequests
}

// pushTestAddUpload counts one archive PUT.
func (c *pushTestCapture) pushTestAddUpload() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uploads++
}

// pushTestUploads returns the number of archive PUTs.
func (c *pushTestCapture) pushTestUploads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uploads
}

// pushTestAddCommand records one remote command.
func (c *pushTestCapture) pushTestAddCommand(command string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commands = append(c.commands, command)
}

// pushTestCommands returns a copy of the recorded commands.
func (c *pushTestCapture) pushTestCommands() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.commands)
}

// pushTestSetEntries stores the tar entries read from the upload body.
func (c *pushTestCapture) pushTestSetEntries(entries map[string]archiveTarEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = entries
}

// pushTestEntries returns the tar entries read from the upload body.
func (c *pushTestCapture) pushTestEntries() map[string]archiveTarEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries
}

// pushTestSource creates a source directory with the given files.
func pushTestSource(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		archiveWriteFile(t, filepath.Join(dir, name), body)
	}
	return dir
}

// pushTestWriteTerminal writes a complete synchronous command envelope.
func pushTestWriteTerminal(t *testing.T, w http.ResponseWriter, exitCode int) {
	t.Helper()
	cliTestWriteJSON(t, w, http.StatusOK, api.NewCommandResponse(exitCode == 0, &exitCode, "", "", false))
}

// pushTestAPIHandler serves the upload-url and command endpoints. uploadBase is
// the signed host origin. commandExit is the exit status reported by every
// command unless the handler blocked.
func pushTestAPIHandler(t *testing.T, capture *pushTestCapture, uploadBase string, commandExit int) http.HandlerFunc {
	t.Helper()
	uploadPath := "/api/v1/sandboxes/" + cliTestSandboxID + "/files/upload-url"
	commandPath := "/api/v1/sandboxes/" + cliTestSandboxID + "/commands"
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("unexpected method %s", r.Method)
			http.NotFound(w, r)
			return
		}
		if !cliTestSecretEqual(r.Header.Get("Authorization"), "Bearer test-key") {
			t.Error("API request does not carry the fixture bearer key")
		}
		switch r.URL.Path {
		case uploadPath:
			capture.pushTestAddURLCall()
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("upload-url Content-Type = %q", got)
			}
			var request api.FileURLRequest
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read upload-url body: %v", err)
				http.Error(w, "read failed", http.StatusInternalServerError)
				return
			}
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Errorf("decode upload-url request: %v", err)
				http.Error(w, "decode failed", http.StatusBadRequest)
				return
			}
			if !strings.HasPrefix(request.Path, pushArchivePathPrefix) || !strings.HasSuffix(request.Path, ".tar") {
				t.Errorf("upload-url path is not a safe temporary tar: %q", request.Path)
			}
			if request.TTLSeconds == nil || *request.TTLSeconds != pushUploadTTLSeconds {
				t.Errorf("upload-url ttlSeconds = %v", request.TTLSeconds)
			}
			response := api.NewFileUploadURLResponse(
				uploadBase+"/upload?token="+pushTestUploadToken,
				request.Path,
				time.Now().Add(time.Hour),
			)
			cliTestWriteJSON(t, w, http.StatusOK, response)
		case commandPath:
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read command body: %v", err)
				http.Error(w, "read failed", http.StatusInternalServerError)
				return
			}
			var request api.CommandRequest
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Errorf("decode command request: %v", err)
				http.Error(w, "decode failed", http.StatusBadRequest)
				return
			}
			capture.pushTestAddCommand(request.Command)
			if strings.Contains(request.Command, "tar -xf") {
				if request.TimeoutSeconds == nil || *request.TimeoutSeconds != pushExtractTimeoutSeconds {
					t.Errorf("extraction timeout = %v", request.TimeoutSeconds)
				}
				if request.Stream != nil && *request.Stream {
					t.Error("extraction is streamed; a v1-only server requires synchronous extraction")
				}
			}
			pushTestWriteTerminal(t, w, commandExit)
		default:
			t.Errorf("unexpected API path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}
}

// pushTestTarHandler reads the streamed tar and records its entries. It asserts
// the signed upload never carries platform credentials.
func pushTestTarHandler(t *testing.T, capture *pushTestCapture, status int) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("upload method = %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Error("upload request carries an Authorization header")
		}
		if got := r.Header.Get("X-Api-Key"); got != "" {
			t.Error("upload request carries an API key header")
		}
		capture.pushTestAddUpload()
		entries := make(map[string]archiveTarEntry)
		reader := tar.NewReader(r.Body)
		for {
			header, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Errorf("read uploaded tar: %v", err)
				break
			}
			body, err := io.ReadAll(reader)
			if err != nil {
				t.Errorf("read uploaded tar entry: %v", err)
				break
			}
			entries[header.Name] = archiveTarEntry{
				Type:     header.Typeflag,
				Mode:     header.Mode & 0o777,
				Linkname: header.Linkname,
				Body:     body,
			}
		}
		capture.pushTestSetEntries(entries)
		if status != http.StatusNoContent {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"success":true,"note":"foreign signed-host body"}`)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// pushTestClient builds the authenticated API client used by direct helper
// tests.
func pushTestClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

// TestPushResolveDestination covers relative, absolute, root, traversal and
// NUL destination handling.
func TestPushResolveDestination(t *testing.T) {
	cases := []struct {
		name        string
		destination string
		base        string
		want        string
		wantErr     bool
	}{
		{name: "default", base: "api", want: "/home/user/api"},
		{name: "relative", destination: "work/api", base: "ignored", want: "/home/user/work/api"},
		{name: "absolute home", destination: "/home/user/api", base: "ignored", want: "/home/user/api"},
		{name: "absolute tmp", destination: "/tmp/api", base: "ignored", want: "/tmp/api"},
		{name: "absolute outside home", destination: "/srv/api", base: "ignored", want: "/srv/api"},
		{name: "absolute etc", destination: "/etc/passwd", base: "ignored", want: "/etc/passwd"},
		{name: "cleans dots", destination: "work/./api/", base: "ignored", want: "/home/user/work/api"},
		{name: "root", destination: "/", base: "ignored", wantErr: true},
		{name: "relative traversal", destination: "../../etc", base: "ignored", wantErr: true},
		{name: "embedded traversal", destination: "api/../../etc", base: "ignored", wantErr: true},
		{name: "nul", destination: "api\x00x", base: "ignored", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pushResolveDestination(tc.destination, tc.base)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("pushResolveDestination(%q) succeeded with %q", tc.destination, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("pushResolveDestination(%q): %v", tc.destination, err)
			}
			if got != tc.want {
				t.Fatalf("pushResolveDestination(%q) = %q, want %q", tc.destination, got, tc.want)
			}
		})
	}
}

// TestPushResolveSource covers directory checks and safe basenames, including
// "." and a symlinked workspace.
func TestPushResolveSource(t *testing.T) {
	base := t.TempDir()
	tracked := filepath.Join(base, "my project")
	if err := os.Mkdir(tracked, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, "plain.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	link := filepath.Join(base, "linked")
	if err := os.Symlink(tracked, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	cases := []struct {
		name     string
		source   string
		wantBase string
		wantErr  bool
	}{
		{name: "directory", source: tracked, wantBase: "my project"},
		{name: "trailing slash", source: tracked + "/", wantBase: "my project"},
		{name: "symlink name", source: link, wantBase: "linked"},
		{name: "file", source: filepath.Join(base, "plain.txt"), wantErr: true},
		{name: "missing", source: filepath.Join(base, "missing"), wantErr: true},
		{name: "empty", source: "", wantErr: true},
		{name: "nul", source: "a\x00b", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			abs, gotBase, err := pushResolveSource(tc.source)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("pushResolveSource(%q) succeeded with %q", tc.source, abs)
				}
				return
			}
			if err != nil {
				t.Fatalf("pushResolveSource(%q): %v", tc.source, err)
			}
			if gotBase != tc.wantBase {
				t.Fatalf("base = %q, want %q", gotBase, tc.wantBase)
			}
		})
	}
}

// TestPushUsageBeforeHTTP pins the usage failures and proves they run no
// request.
func TestPushUsageBeforeHTTP(t *testing.T) {
	capture := new(pushTestCapture)
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		capture.pushTestAddURLCall()
		http.NotFound(w, r)
	}, nil)

	cases := []struct {
		name string
		args []string
		want int
	}{
		{name: "no args", args: []string{"push"}, want: 2},
		{name: "one arg", args: []string{"push", cliTestSandboxID}, want: 2},
		{name: "three args", args: []string{"push", cliTestSandboxID, ".", "extra"}, want: 2},
		{name: "unknown flag", args: []string{"push", cliTestSandboxID, ".", "--bogus"}, want: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if status := env.run(t, tc.args...); status != tc.want {
				t.Fatalf("exit status = %d, want %d", status, tc.want)
			}
		})
	}
	if got := capture.pushTestURLCalls(); got != 0 {
		t.Fatalf("usage failures made %d HTTP requests", got)
	}

	t.Run("source is a file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file.txt")
		if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
		if status := env.run(t, "push", cliTestSandboxID, file); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if got := capture.pushTestURLCalls(); got != 0 {
			t.Fatalf("invalid source made %d HTTP requests", got)
		}
	})
}

// TestPushStreamsAndExtracts exercises the success path end to end: request
// shape, streamed tar contents, default destination, counts and human output.
func TestPushStreamsAndExtracts(t *testing.T) {
	capture := new(pushTestCapture)
	upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusNoContent))
	t.Cleanup(upload.Close)
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, upload.URL, 0))
	t.Cleanup(apiServer.Close)

	source := pushTestSource(t, map[string]string{
		"cmd/run.sh": "#!/bin/sh\necho hi\n",
		"README.md":  "hello\n",
	})
	if err := os.Chmod(filepath.Join(source, "cmd", "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := os.Symlink("README.md", filepath.Join(source, "readme-link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	var progress bytes.Buffer
	result, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{Progress: &progress})
	if err != nil {
		t.Fatalf("PushDirectory: %v", err)
	}

	base := filepath.Base(source)
	if result.Directory != "/home/user/"+base {
		t.Fatalf("directory = %q, want /home/user/%s", result.Directory, base)
	}
	if result.Files != 3 {
		t.Fatalf("files = %d, want 3", result.Files)
	}
	if result.Bytes <= 0 {
		t.Fatalf("bytes = %d, want a positive streamed size", result.Bytes)
	}
	got := progress.String()
	if !strings.Contains(got, "pushed 3 files to /home/user/"+base) {
		t.Fatalf("progress = %q", got)
	}
	if strings.Contains(got, pushTestUploadToken) || strings.Contains(got, upload.URL) {
		t.Fatal("progress leaked the upload URL")
	}

	entries := capture.pushTestEntries()
	run, ok := entries["cmd/run.sh"]
	if !ok {
		t.Fatalf("archive entries = %v, want cmd/run.sh", slices.Sorted(func(yield func(string) bool) {
			for name := range entries {
				if !yield(name) {
					return
				}
			}
		}))
	}
	if run.Mode&0o111 == 0 {
		t.Fatalf("cmd/run.sh mode = %o, want an executable bit", run.Mode)
	}
	link, ok := entries["readme-link"]
	if !ok || link.Type != tar.TypeSymlink || link.Linkname != "README.md" {
		t.Fatalf("readme-link entry = %+v", link)
	}

	commands := capture.pushTestCommands()
	if len(commands) != 1 {
		t.Fatalf("commands = %d, want one extraction", len(commands))
	}
	command := commands[0]
	for _, want := range []string{"mkdir -p -- \"$dst\"", "tar -xf \"$tmp\" -C \"$dst\"", "trap cleanup EXIT"} {
		if !strings.Contains(command, want) {
			t.Fatalf("extraction command is missing %q", want)
		}
	}
	if strings.Contains(command, "rm -rf") {
		t.Fatal("extraction must not delete existing remote files")
	}
}

// TestPushLargeStreamNoBuffer uploads more than 5 MiB through a cross-origin
// signed URL with a foreign success body and no Authorization header.
func TestPushLargeStreamNoBuffer(t *testing.T) {
	capture := new(pushTestCapture)
	upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusOK))
	t.Cleanup(upload.Close)
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, upload.URL, 0))
	t.Cleanup(apiServer.Close)

	big := bytes.Repeat([]byte("0123456789abcdef"), (6<<20)/16)
	source := pushTestSource(t, map[string]string{"big.bin": string(big)})
	result, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
		SandboxID:   cliTestSandboxID,
		Source:      source,
		Destination: "/tmp/push-big",
	}, PushOutput{})
	if err != nil {
		t.Fatalf("PushDirectory: %v", err)
	}
	if result.Bytes <= 5<<20 {
		t.Fatalf("bytes = %d, want more than 5 MiB", result.Bytes)
	}
	entries := capture.pushTestEntries()
	entry, ok := entries["big.bin"]
	if !ok {
		t.Fatal("big.bin missing from streamed tar")
	}
	if len(entry.Body) != len(big) {
		t.Fatalf("big.bin size = %d, want %d", len(entry.Body), len(big))
	}
	if capture.pushTestUploads() != 1 {
		t.Fatalf("uploads = %d, want 1", capture.pushTestUploads())
	}
}

// TestPushUploadURLValidation pins the accepted envelope and its negative
// cases. It never prints the URL.
func TestPushUploadURLValidation(t *testing.T) {
	archivePath := "/tmp/bbraft-push-0123456789abcdef.tar"
	valid := func() api.FileURLResponse {
		return api.NewFileUploadURLResponse(
			"https://upload.example.test/put?token="+pushTestUploadToken,
			archivePath,
			time.Now().Add(time.Hour),
		)
	}
	cases := []struct {
		name   string
		mutate func(*api.FileURLResponse)
	}{
		{name: "valid"},
		{name: "not ok", mutate: func(r *api.FileURLResponse) { r.OK = false }},
		{name: "wrong type", mutate: func(r *api.FileURLResponse) { r.Type = api.TypeSandboxInfo }},
		{name: "wrong method", mutate: func(r *api.FileURLResponse) { r.Method = api.FileMethodGet }},
		{name: "wrong path", mutate: func(r *api.FileURLResponse) { r.Path = "/tmp/other.tar" }},
		{name: "expired", mutate: func(r *api.FileURLResponse) { r.ExpiresAt = time.Now().Add(-time.Second) }},
		{name: "zero expiry", mutate: func(r *api.FileURLResponse) { r.ExpiresAt = time.Time{} }},
		{name: "relative url", mutate: func(r *api.FileURLResponse) { r.URL = "/upload" }},
		{name: "ftp url", mutate: func(r *api.FileURLResponse) { r.URL = "ftp://upload.example.test/put" }},
		{name: "empty host", mutate: func(r *api.FileURLResponse) { r.URL = "https:///put" }},
		{name: "userinfo", mutate: func(r *api.FileURLResponse) { r.URL = "https://user:pass@upload.example.test/put" }},
		{name: "fragment", mutate: func(r *api.FileURLResponse) { r.URL = "https://upload.example.test/put#fragment" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := valid()
			if tc.mutate != nil {
				tc.mutate(&response)
			}
			err := pushValidateUploadURL(response, archivePath)
			if tc.name == "valid" {
				if err != nil {
					t.Fatalf("valid envelope rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid upload URL envelope was accepted")
			}
			if strings.Contains(err.Error(), pushTestUploadToken) {
				t.Fatal("validation error leaked the upload token")
			}
		})
	}
}

// TestPushUploadURLHTTPFailures covers issuance status, media type, malformed
// and oversized responses; none may stream an archive.
func TestPushUploadURLHTTPFailures(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		contentType string
		body        string
		wantAPI     int
	}{
		{
			name:        "status",
			status:      http.StatusUnauthorized,
			contentType: "application/json",
			body:        `{"ok":false,"type":"sandbox.error","status":401,"code":"unauthorized","message":"no"}`,
			wantAPI:     http.StatusUnauthorized,
		},
		{
			name:        "media type",
			status:      http.StatusOK,
			contentType: "text/plain",
			body:        "not json",
		},
		{
			name:        "malformed",
			status:      http.StatusOK,
			contentType: "application/json",
			body:        `{"ok":`,
		},
		{
			name:        "oversized",
			status:      http.StatusOK,
			contentType: "application/json",
			body:        strings.Repeat("x", pushResponseMaxBytes+1),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := new(pushTestCapture)
			apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capture.pushTestAddURLCall()
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(apiServer.Close)
			doer := pushTestDoerFunc(func(*http.Request) (*http.Response, error) {
				t.Error("archive upload started after a failed issuance")
				return nil, errors.New("unexpected upload")
			})
			source := pushTestSource(t, map[string]string{"a.txt": "a"})
			_, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), doer, PushRequest{
				SandboxID: cliTestSandboxID,
				Source:    source,
			}, PushOutput{})
			if err == nil {
				t.Fatal("failed issuance was reported as success")
			}
			if tc.wantAPI != 0 {
				apiErr, ok := errors.AsType[*APIError](err)
				if !ok || apiErr.Status != tc.wantAPI {
					t.Fatal("want the API error status")
				}
				return
			}
			if _, ok := errors.AsType[*CLIError](err); !ok {
				t.Fatal("want a safe invalid-response error")
			}
		})
	}
}

// TestPushUploadErrorClassification pins cancellation and timeout mapping.
func TestPushUploadErrorClassification(t *testing.T) {
	if err := pushUploadError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled transport error lost its classification")
	}
	if err := pushUploadError(context.DeadlineExceeded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("timed-out transport error lost its classification")
	}
	err := pushUploadError(errors.New("token-bearing detail"))
	if _, ok := errors.AsType[*CLIError](err); !ok {
		t.Fatal("generic transport failure is not a safe CLI error")
	}
}

// TestPushMalformedIssuanceStopsBeforeUpload proves a bad issuance response
// never streams an archive or runs a command.
func TestPushMalformedIssuanceStopsBeforeUpload(t *testing.T) {
	capture := new(pushTestCapture)
	upload := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		capture.pushTestAddUpload()
	}))
	t.Cleanup(upload.Close)
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.pushTestAddURLCall()
		cliTestWriteJSON(t, w, http.StatusOK, map[string]any{
			"ok":        true,
			"type":      api.TypeSandboxInfo,
			"url":       upload.URL + "/upload",
			"method":    "PUT",
			"path":      "/tmp/bbraft-push-0123456789abcdef.tar",
			"expiresAt": time.Now().Add(time.Hour),
		})
	}))
	t.Cleanup(apiServer.Close)

	source := pushTestSource(t, map[string]string{"a.txt": "a"})
	_, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{})
	if err == nil {
		t.Fatal("malformed upload URL envelope was accepted")
	}
	if capture.pushTestUploads() != 0 {
		t.Fatal("archive streamed after a malformed issuance")
	}
	if len(capture.pushTestCommands()) != 0 {
		t.Fatal("a command ran after a malformed issuance")
	}
}

// TestPushUploadRejectedStopsExtraction proves a non-2xx signed-host response
// fails before extraction and only runs bounded temp cleanup.
func TestPushUploadRejectedStopsExtraction(t *testing.T) {
	capture := new(pushTestCapture)
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("upload method = %s", r.Method)
		}
		capture.pushTestAddUpload()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"ok":false,"code":"expired","message":"secret `+pushTestUploadToken+`"}`)
	}))
	t.Cleanup(upload.Close)
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, upload.URL, 0))
	t.Cleanup(apiServer.Close)

	source := pushTestSource(t, map[string]string{"a.txt": "a"})
	_, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{})
	if err == nil {
		t.Fatal("rejected upload was reported as success")
	}
	pushTestAssertNoSecret(t, err, pushTestUploadToken)
	apiErr, ok := errors.AsType[*APIError](err)
	if !ok || apiErr.Status != http.StatusForbidden || apiErr.Code != "http_403" {
		t.Fatal("want a status-only 403 API error")
	}
	commands := capture.pushTestCommands()
	if len(commands) != 1 || !strings.HasPrefix(commands[0], "rm -f -- ") {
		t.Fatalf("commands = %v, want only temp cleanup", commands)
	}
	if strings.Contains(commands[0], "tar -xf") {
		t.Fatal("extraction ran after a rejected upload")
	}
}

// TestPushEarlyUploadResponseJoinsWriter proves an immediate response before
// the body is read unblocks and joins the archive writer instead of hanging.
// An early 2xx is a truncated upload and must be rejected, never accepted.
func TestPushEarlyUploadResponseJoinsWriter(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{name: "early 500", status: http.StatusInternalServerError},
		{name: "early 200", status: http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := new(pushTestCapture)
			upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capture.pushTestAddUpload()
				// Do not read the body: the writer is still streaming a
				// large file when the response arrives.
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(upload.Close)
			apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, upload.URL, 0))
			t.Cleanup(apiServer.Close)

			big := bytes.Repeat([]byte("x"), 8<<20)
			source := pushTestSource(t, map[string]string{"big.bin": string(big)})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			start := time.Now()
			_, err := PushDirectory(ctx, pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
				SandboxID: cliTestSandboxID,
				Source:    source,
			}, PushOutput{})
			if err == nil {
				t.Fatal("early upload response was reported as success")
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("early upload response hung until the test deadline")
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Fatalf("early upload response took %s", elapsed)
			}
			commands := capture.pushTestCommands()
			if len(commands) == 0 || !strings.HasPrefix(commands[0], "rm -f -- ") {
				t.Fatalf("commands = %v, want bounded temp cleanup", commands)
			}
		})
	}
}

// TestPushExtractionFailureNoRetry proves one ambiguous extraction command is
// never repeated and the script carries the EXIT cleanup trap.
func TestPushExtractionFailureNoRetry(t *testing.T) {
	capture := new(pushTestCapture)
	upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusNoContent))
	t.Cleanup(upload.Close)
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, upload.URL, 1))
	t.Cleanup(apiServer.Close)

	source := pushTestSource(t, map[string]string{"a.txt": "a"})
	_, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{})
	if err == nil {
		t.Fatal("failed extraction was reported as success")
	}
	exit, ok := errors.AsType[*ExitError](err)
	if !ok || exit.Code != 1 || exit.Err == nil {
		t.Fatal("want a rendered exit status 1")
	}
	commands := capture.pushTestCommands()
	extractions := 0
	var cleanup string
	for _, command := range commands {
		if strings.Contains(command, "tar -xf") {
			extractions++
			if !strings.Contains(command, "trap cleanup EXIT") {
				t.Fatal("extraction command has no temp cleanup trap")
			}
		} else if strings.HasPrefix(command, "rm -f -- ") {
			cleanup = command
		}
	}
	if extractions != 1 {
		t.Fatalf("extractions = %d, want exactly one and no retry", extractions)
	}
	if cleanup == "" {
		t.Fatal("failed extraction did not trigger bounded temp cleanup")
	}
}

// TestPushCancellationCleanup proves a cancelled upload returns promptly and
// attempts bounded remote temp cleanup.
func TestPushCancellationCleanup(t *testing.T) {
	capture := new(pushTestCapture)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.pushTestAddUpload()
		once.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); upload.Close() })
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, upload.URL, 0))
	t.Cleanup(apiServer.Close)

	big := bytes.Repeat([]byte("x"), 4<<20)
	source := pushTestSource(t, map[string]string{"big.bin": string(big)})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := PushDirectory(ctx, pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
			SandboxID: cliTestSandboxID,
			Source:    source,
		}, PushOutput{})
		done <- err
	}()
	t.Cleanup(func() {
		// Every fatal path below leaves the worker running; stop it and join
		// it before the test returns.
		cancel()
		select {
		case <-finished:
		case <-time.After(2 * pushCleanupTimeout):
			t.Error("cancelled push worker did not stop")
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("want context.Canceled")
		}
		if elapsed := time.Since(start); elapsed > 2*pushCleanupTimeout {
			t.Fatalf("cancellation took %s", elapsed)
		}
	case <-time.After(2 * pushCleanupTimeout):
		t.Fatal("cancelled push did not return")
	}
	commands := capture.pushTestCommands()
	if len(commands) == 0 || !strings.HasPrefix(commands[0], "rm -f -- ") {
		t.Fatalf("commands = %v, want best-effort temp cleanup", commands)
	}
}

// TestPushUploadURLCallerDeadline keeps the caller's cancellation
// classification when the issuance request is cut short.
func TestPushUploadURLCallerDeadline(t *testing.T) {
	capture := new(pushTestCapture)
	release := make(chan struct{})
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.pushTestAddURLCall()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); apiServer.Close() })
	source := pushTestSource(t, map[string]string{"a.txt": "a"})
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusNoContent))
	t.Cleanup(upload.Close)
	_, err := PushDirectory(ctx, pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("want context.DeadlineExceeded")
	}
	if capture.pushTestUploads() != 0 {
		t.Fatal("archive streamed after the issuance deadline")
	}
}

// TestPushCommandLiteralPaths proves every path is POSIX-quoted and that
// extraction keeps remote files instead of deleting the destination.
func TestPushCommandLiteralPaths(t *testing.T) {
	archivePath := "/tmp/bbraft-push-0123456789abcdef.tar"
	destination := "/home/user/a b'c$(touch pwned)-d"
	command := pushExtractCommand(archivePath, destination)
	if !strings.Contains(command, "dst="+execShellJoin([]string{destination})) {
		t.Fatalf("destination is not shell-quoted in the extraction command")
	}
	if !strings.Contains(command, "tmp="+execShellJoin([]string{archivePath})) {
		t.Fatalf("archive path is not shell-quoted in the extraction command")
	}
	if !strings.Contains(command, "mkdir -p -- \"$dst\"") || !strings.Contains(command, "tar -xf \"$tmp\" -C \"$dst\"") {
		t.Fatal("extraction command does not create the destination and extract into it")
	}
	if strings.Contains(command, "rm -rf") {
		t.Fatal("extraction command deletes remote files")
	}

	// Round-trip the same quoting through a real shell.
	script := "dst=" + execShellJoin([]string{destination}) + "\nprintf '%s' \"$dst\""
	out, err := exec.CommandContext(t.Context(), "bash", "-c", script).Output()
	if err != nil {
		t.Fatalf("bash roundtrip: %v", err)
	}
	if string(out) != destination {
		t.Fatalf("bash roundtrip = %q, want %q", out, destination)
	}
}

// TestPushExtractCommandActualShell runs the constructed script in a temp
// directory: executable bits and symlinks survive, a remote-only file stays,
// the tar is removed, a bad tar fails and is removed, and a malicious
// destination stays literal.
func TestPushExtractCommandActualShell(t *testing.T) {
	source := t.TempDir()
	archiveWriteFile(t, filepath.Join(source, "bin", "run.sh"), "#!/bin/sh\necho hi\n")
	if err := os.Chmod(filepath.Join(source, "bin", "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := os.Symlink("bin/run.sh", filepath.Join(source, "run link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	archiveWriteFile(t, filepath.Join(source, "odd", "a b'c.txt"), "literal\n")

	tarPath := filepath.Join(t.TempDir(), "push archive.tar")
	file, err := os.Create(tarPath)
	if err != nil {
		t.Fatalf("create tar: %v", err)
	}
	if _, err := WriteArchive(t.Context(), source, file); err != nil {
		file.Close()
		t.Fatalf("WriteArchive: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	workDir := t.TempDir()
	destination := filepath.Join(t.TempDir(), "dest with space'quote$(touch pwned)")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "remote.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("write remote file: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", "-c", pushExtractCommand(tarPath, destination))
	cmd.Dir = workDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("extract script failed: %v: %s", err, out)
	}

	executable, err := os.Stat(filepath.Join(destination, "bin", "run.sh"))
	if err != nil {
		t.Fatalf("stat extracted executable: %v", err)
	}
	if executable.Mode().Perm()&0o111 == 0 {
		t.Fatal("extracted file lost its executable bit")
	}
	link, err := os.Lstat(filepath.Join(destination, "run link"))
	if err != nil {
		t.Fatalf("lstat extracted symlink: %v", err)
	}
	if link.Mode()&fs.ModeSymlink == 0 {
		t.Fatal("extracted symlink is not a symlink")
	}
	if _, err := os.Stat(filepath.Join(destination, "odd", "a b'c.txt")); err != nil {
		t.Fatalf("literal archive name was not extracted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, "remote.txt")); err != nil {
		t.Fatalf("remote-only file was removed: %v", err)
	}
	if _, err := os.Stat(tarPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("temporary tar was not removed")
	}
	if _, err := os.Stat(filepath.Join(workDir, "pwned")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("malicious destination text was executed")
	}

	badTar := filepath.Join(t.TempDir(), "bad.tar")
	if err := os.WriteFile(badTar, []byte("not a tar"), 0o644); err != nil {
		t.Fatalf("write bad tar: %v", err)
	}
	badCmd := exec.CommandContext(t.Context(), "bash", "-c", pushExtractCommand(badTar, filepath.Join(t.TempDir(), "bad-dest")))
	if err := badCmd.Run(); err == nil {
		t.Fatal("bad tar extraction succeeded")
	}
	if _, err := os.Stat(badTar); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("temporary tar was not removed after a bad extraction")
	}
}

// TestPushCommandJSONOutput pins the minimal JSON event and checks no secret
// reaches stdout or stderr.
func TestPushCommandJSONOutput(t *testing.T) {
	capture := new(pushTestCapture)
	upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusNoContent))
	t.Cleanup(upload.Close)
	env := cliTestNew(t, pushTestAPIHandler(t, capture, upload.URL, 0), nil)
	source := pushTestSource(t, map[string]string{"a.txt": "a"})

	status := env.run(t, "push", cliTestSandboxID, source, "--to", "/tmp/json-push")
	pushTestAssertOutputNoSecret(t, env)
	if status != 0 {
		t.Fatalf("exit status = %d, want 0", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 1 {
		t.Fatalf("JSON lines = %d, want 1", len(lines))
	}
	event := lines[0]
	if event["event"] != "pushed" || event["sandboxId"] != cliTestSandboxID ||
		event["directory"] != "/tmp/json-push" || event["files"] != float64(1) {
		t.Fatalf("completion event = %v", event)
	}
	if bytes, ok := event["bytes"].(float64); !ok || bytes <= 0 {
		t.Fatalf("completion bytes = %v", event["bytes"])
	}
}

// pushTestAssertOutputNoSecret checks command streams before any assertion
// that might print them.
func pushTestAssertOutputNoSecret(t *testing.T, env *cliTestEnv) {
	t.Helper()
	combined := env.stdout.String() + env.stderr.String()
	if strings.Contains(combined, pushTestUploadToken) {
		t.Fatal("command output leaked the upload token")
	}
}

// TestPushCommandHumanOutput puts the completion notice on stderr, leaving
// stdout for machine or child output.
func TestPushCommandHumanOutput(t *testing.T) {
	capture := new(pushTestCapture)
	upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusNoContent))
	t.Cleanup(upload.Close)
	env := cliTestNew(t, pushTestAPIHandler(t, capture, upload.URL, 0), func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	source := pushTestSource(t, map[string]string{"a.txt": "a"})

	status := env.run(t, "push", cliTestSandboxID, source)
	pushTestAssertOutputNoSecret(t, env)
	if status != 0 {
		t.Fatalf("exit status = %d, want 0", status)
	}
	if env.stdout.Len() != 0 {
		t.Fatal("stdout is not empty in human mode")
	}
	if !strings.Contains(env.stderr.String(), "pushed 1 files to /home/user/"+filepath.Base(source)) {
		t.Fatal("human completion notice is missing")
	}
}

// TestPushArchiveSelectionDelegated proves push sends the same git-filtered
// file set as WriteArchive.
func TestPushArchiveSelectionDelegated(t *testing.T) {
	archiveRequireGit(t)
	capture := new(pushTestCapture)
	upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusNoContent))
	t.Cleanup(upload.Close)
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, upload.URL, 0))
	t.Cleanup(apiServer.Close)

	source := t.TempDir()
	pushTestRun(t, source, "git", "init", "-q")
	if err := os.WriteFile(filepath.Join(source, ".gitignore"), []byte("ignored.txt\n"), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "tracked.txt"), []byte("tracked"), 0o644); err != nil {
		t.Fatalf("write tracked: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "ignored.txt"), []byte("ignored"), 0o644); err != nil {
		t.Fatalf("write ignored: %v", err)
	}
	pushTestRun(t, source, "git", "add", ".gitignore", "tracked.txt")

	if _, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{}); err != nil {
		t.Fatalf("PushDirectory: %v", err)
	}
	entries := capture.pushTestEntries()
	if _, ok := entries["tracked.txt"]; !ok {
		t.Fatal("tracked file missing from pushed archive")
	}
	if _, ok := entries[".gitignore"]; !ok {
		t.Fatal(".gitignore missing from pushed archive")
	}
	if _, ok := entries["ignored.txt"]; ok {
		t.Fatal("ignored file was pushed")
	}
}

// pushTestRun runs a setup command in dir.
func pushTestRun(t *testing.T, dir, name string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), name, args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v: %s", name, args, err, out)
	}
}

// TestPushUploadSuccessBodies covers the accepted signed-host success bodies:
// an empty 204 and a foreign 200 JSON object that is not a bbraft DTO.
func TestPushUploadSuccessBodies(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{name: "204", status: http.StatusNoContent},
		{name: "200 foreign body", status: http.StatusOK},
		{name: "201 created", status: http.StatusCreated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := new(pushTestCapture)
			upload := httptest.NewServer(pushTestTarHandler(t, capture, tc.status))
			t.Cleanup(upload.Close)
			apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, upload.URL, 0))
			t.Cleanup(apiServer.Close)
			source := pushTestSource(t, map[string]string{"a.txt": "a"})
			if _, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), NewUploadClient(upload.Client()), PushRequest{
				SandboxID: cliTestSandboxID,
				Source:    source,
			}, PushOutput{}); err != nil {
				t.Fatalf("PushDirectory: %v", err)
			}
			if capture.pushTestUploads() != 1 {
				t.Fatalf("uploads = %d, want 1", capture.pushTestUploads())
			}
		})
	}
}

// pushTestProbeBody counts reads and closes of an opaque success body.
type pushTestProbeBody struct {
	reads  atomic.Int64
	closes atomic.Int64
	data   []byte
	offset int
}

// Read implements io.Reader.
func (b *pushTestProbeBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	if b.offset >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.offset:])
	b.offset += n
	return n, nil
}

// Close implements io.Closer.
func (b *pushTestProbeBody) Close() error {
	b.closes.Add(1)
	return nil
}

// TestPushSuccessBodyNotRead proves an opaque signed-host success body is never
// read: only closing it is allowed, so no unbounded or blocking drain exists.
func TestPushSuccessBodyNotRead(t *testing.T) {
	capture := new(pushTestCapture)
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, "https://upload.example.test", 0))
	t.Cleanup(apiServer.Close)
	probe := &pushTestProbeBody{data: []byte("opaque success body that must never be drained")}
	doer := pushTestDoerFunc(func(req *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, req.Body); err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       probe,
		}, nil
	})
	source := pushTestSource(t, map[string]string{"a.txt": "a"})
	if _, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), doer, PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{}); err != nil {
		t.Fatalf("PushDirectory: %v", err)
	}
	if reads := probe.reads.Load(); reads != 0 {
		t.Fatalf("success body was read %d times", reads)
	}
	if closes := probe.closes.Load(); closes != 1 {
		t.Fatalf("success body closes = %d, want 1", closes)
	}
}

// TestNewUploadClientRejectsRedirects pins the no-follow rule: the client
// returns the 3xx response instead of following it, so no token-bearing error
// or Location forward can happen. It also drops the injected cookie jar.
func TestNewUploadClientRejectsRedirects(t *testing.T) {
	var followed atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			followed.Add(1)
			return
		}
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	base := server.Client()
	base.Jar = new(pushTestJar)
	client := NewUploadClient(base)
	if client.Jar != nil {
		t.Fatal("upload client kept the injected cookie jar")
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, server.URL+"/upload?token="+pushTestUploadToken, strings.NewReader("x"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), pushTestUploadToken) {
			t.Fatal("redirect error leaked the upload token")
		}
		t.Fatalf("upload client request failed without a 3xx response")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want the original 307", resp.StatusCode)
	}
	if followed.Load() != 0 {
		t.Fatal("upload client followed the redirect")
	}
}

// pushTestJar is a no-op cookie jar used to prove NewUploadClient drops it.
type pushTestJar struct{}

// SetCookies implements http.CookieJar.
func (*pushTestJar) SetCookies(*url.URL, []*http.Cookie) {}

// Cookies implements http.CookieJar.
func (*pushTestJar) Cookies(*url.URL) []*http.Cookie { return nil }

// TestPushReadLimitedBounded proves a hostile signed-host body cannot grow
// memory or be mistaken for an envelope.
func TestPushReadLimitedBounded(t *testing.T) {
	raw, err := pushReadLimited(strings.NewReader(strings.Repeat("x", 32)), 8)
	if err == nil {
		t.Fatalf("oversized body accepted: %d bytes", len(raw))
	}
	raw, err = pushReadLimited(strings.NewReader("ok"), 8)
	if err != nil || string(raw) != "ok" {
		t.Fatalf("bounded read = %q, %v", raw, err)
	}
}

// TestPushCountingReaderCountsPartialReads proves bytes read before EOF are
// counted once.
func TestPushCountingReaderCountsPartialReads(t *testing.T) {
	counter := &pushCountingReader{
		reader:   bufio.NewReaderSize(strings.NewReader("abcdef"), 2),
		consumed: make(chan struct{}),
	}
	buf := make([]byte, 3)
	total := 0
	for {
		n, err := counter.Read(buf)
		total += n
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if got := counter.n.Load(); got != int64(total) || got != 6 {
		t.Fatalf("counted = %d, want 6", got)
	}
	if !counter.consumedByTransport() {
		t.Fatal("final EOF was not recorded")
	}
}

// pushTestDoerFunc adapts a function to HTTPDoer.
type pushTestDoerFunc func(*http.Request) (*http.Response, error)

// Do implements HTTPDoer.
func (f pushTestDoerFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

// TestPushTransportErrorDoesNotLeakURL proves a transport error quoting the
// signed URL is never returned, directly or through an unwrap chain.
func TestPushTransportErrorDoesNotLeakURL(t *testing.T) {
	capture := new(pushTestCapture)
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, "https://upload.example.test", 0))
	t.Cleanup(apiServer.Close)

	doer := pushTestDoerFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("put %s: connection reset", req.URL.String())
	})
	source := pushTestSource(t, map[string]string{"a.txt": "a"})
	_, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), doer, PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{})
	if err == nil {
		t.Fatal("transport failure was reported as success")
	}
	pushTestAssertNoSecret(t, err, pushTestUploadToken)
}

// pushTestAssertNoSecret walks the error chain and fails if any message
// contains secret.
func pushTestAssertNoSecret(t *testing.T, err error, secret string) {
	t.Helper()
	for current := err; current != nil; current = errors.Unwrap(current) {
		if strings.Contains(current.Error(), secret) {
			t.Fatal("error chain leaked the upload token")
		}
	}
}

// TestPushBlockedGitTransportErrorSanitized holds the archive producer in a
// git child process while the upload transport fails with a URL-bearing error.
// Our abort cancels git, but that cancellation must not replace the original
// transport failure, and the URL must not leak through the pipe.
func TestPushBlockedGitTransportErrorSanitized(t *testing.T) {
	capture := new(pushTestCapture)
	apiServer := httptest.NewServer(pushTestAPIHandler(t, capture, "https://upload.example.test", 0))
	t.Cleanup(apiServer.Close)

	binDir := t.TempDir()
	stateDir := t.TempDir()
	marker := filepath.Join(stateDir, "git-started")
	pidPath := filepath.Join(stateDir, "git-pid")
	script := "#!/bin/sh\n" +
		"echo $$ > " + execShellJoin([]string{pidPath}) + "\n" +
		": > " + execShellJoin([]string{marker}) + "\n" +
		"sleep 30\n"
	if err := os.WriteFile(filepath.Join(binDir, "git"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake git: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	source := t.TempDir()
	if err := os.Mkdir(filepath.Join(source, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir .git: %v", err)
	}
	gitStarted := false
	doer := pushTestDoerFunc(func(req *http.Request) (*http.Response, error) {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(marker); err == nil {
				gitStarted = true
				break
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		return nil, fmt.Errorf("put %s: backend refused", req.URL.String())
	})
	_, err := PushDirectory(t.Context(), pushTestClient(t, apiServer), doer, PushRequest{
		SandboxID: cliTestSandboxID,
		Source:    source,
	}, PushOutput{})
	if !gitStarted {
		t.Fatal("archive producer never reached the blocked git child")
	}
	if err == nil {
		t.Fatal("transport failure was reported as success")
	}
	pushTestAssertNoSecret(t, err, pushTestUploadToken)
	if errors.Is(err, context.Canceled) {
		t.Fatal("our abort replaced the real transport failure")
	}
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "upload_failed" {
		t.Fatal("want the sanitized transport failure")
	}

	rawPID, readErr := os.ReadFile(pidPath)
	if readErr != nil {
		t.Fatal("blocked git child did not record its pid")
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if parseErr != nil {
		t.Fatal("blocked git child recorded an invalid pid")
	}
	if killErr := syscall.Kill(pid, 0); !errors.Is(killErr, syscall.ESRCH) {
		t.Fatal("blocked git child was not stopped and reaped")
	}
}
