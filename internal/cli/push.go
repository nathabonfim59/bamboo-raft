package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// Push defaults and limits. The upload URL lifetime and extraction timeout are
// deliberately generous: an archive has no size cap, and a large upload or a
// big extraction must not inherit the short JSON convenience timeout.
const (
	pushUploadTTLSeconds       = 3600
	pushExtractTimeoutSeconds  = 600
	pushCleanupTimeout         = 5 * time.Second
	pushCleanupCommandSeconds  = 30
	pushResponseMaxBytes       = 64 << 10
	pushArchivePathPrefix      = "/tmp/bbraft-push-"
	pushGuestHome              = "/home/user"
	pushUploadPathSuffix       = "/files/upload-url"
	pushUploadContentType      = "application/json"
	pushArchiveRandomNameBytes = 8
)

// PushRequest is one directory push into a sandbox. Destination is an absolute
// guest path or one relative to /home/user; an empty Destination defaults to
// /home/user/<source basename>.
type PushRequest struct {
	SandboxID   string
	Source      string
	Destination string
}

// PushOutput selects how completion progress is reported. JSON, when non-nil,
// receives one non-secret JSON event and Progress is ignored. Progress, when
// non-nil, receives a short human completion notice. A caller that wants no
// output (a future run command) passes a nil JSON and io.Discard as Progress.
type PushOutput struct {
	Progress io.Writer
	JSON     io.Writer
}

// PushResult describes a completed push. Directory is the absolute guest
// destination, Files the number of regular files and symbolic links uploaded,
// and Bytes the number of tar bytes sent to the upload URL.
type PushResult struct {
	Directory string
	Files     int
	Bytes     int64
}

// HTTPDoer is the raw transport used for the signed upload URL. The helper
// never adds platform credentials to it; [NewUploadClient] returns a client
// that refuses redirects so a credential-bearing URL cannot be forwarded.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// PushDirectory streams a local directory into a sandbox. It uses
// [WriteArchive] to build the tar, uploads it through a temporary signed URL,
// and extracts it with [ExecuteCommand]. The archive is streamed through an
// io.Pipe and is never held in memory or written to a local temporary file.
//
// The source must be a directory; it is validated before any HTTP request. The
// destination is an absolute guest path or one relative to /home/user.
// Extraction creates the destination, preserves executable bits and symbolic
// links, and never removes files that exist remotely but not locally. The
// temporary tar is removed by the extraction command on success and failure,
// and best-effort after a failed upload.
func PushDirectory(ctx context.Context, api APIClient, upload HTTPDoer, request PushRequest, out PushOutput) (PushResult, error) {
	var result PushResult
	if api == nil {
		return result, &CLIError{Code: "invalid_request", Message: "push: no API client"}
	}
	if upload == nil {
		return result, &CLIError{Code: "invalid_request", Message: "push: no upload transport"}
	}
	if request.SandboxID == "" {
		return result, &CLIError{Code: "invalid_request", Message: "push: sandbox id is required"}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	source, base, err := pushResolveSource(request.Source)
	if err != nil {
		return result, err
	}
	destination, err := pushResolveDestination(request.Destination, base)
	if err != nil {
		return result, err
	}
	result.Directory = destination

	archivePath, err := pushArchivePath()
	if err != nil {
		return result, err
	}

	uploadURL, err := pushRequestUploadURL(ctx, api, request.SandboxID, archivePath)
	if err != nil {
		return result, err
	}

	info, n, err := pushUploadArchive(ctx, upload, uploadURL, source)
	if err != nil {
		// The PUT may have created a partial file. This is best effort and
		// bounded so a cancelled caller is not kept waiting long.
		pushCleanupArchive(ctx, api, request.SandboxID, archivePath)
		return result, err
	}
	result.Files = info.Files
	result.Bytes = n

	if err := pushExtract(ctx, api, request.SandboxID, archivePath, destination); err != nil {
		// The extraction never ran or failed before its own EXIT trap removed
		// the tar. A bounded, idempotent cleanup is safe in both cases and
		// never replaces the extraction error.
		pushCleanupArchive(ctx, api, request.SandboxID, archivePath)
		return result, err
	}

	if err := pushReport(out, request.SandboxID, result); err != nil {
		return result, err
	}
	return result, nil
}

// pushResolveSource validates the local source and returns its absolute path
// and the directory name used for the default destination. The absolute path
// is not symlink-resolved, so a symlinked workspace keeps the name the user
// passed.
func pushResolveSource(source string) (string, string, error) {
	if source == "" {
		return "", "", &CLIError{Code: "invalid_source", Message: "push: source directory is required"}
	}
	if strings.ContainsRune(source, '\x00') {
		return "", "", &CLIError{Code: "invalid_source", Message: "push: source path contains a NUL byte"}
	}
	abs, err := filepath.Abs(source)
	if err != nil {
		return "", "", &CLIError{Code: "invalid_source", Message: "push: cannot resolve source directory"}
	}
	stat, err := os.Stat(abs)
	if err != nil {
		return "", "", &CLIError{Code: "invalid_source", Message: "push: source directory is not accessible"}
	}
	if !stat.IsDir() {
		return "", "", &CLIError{Code: "invalid_source", Message: "push: source is not a directory"}
	}
	base := filepath.Base(filepath.Clean(abs))
	if base == "" || base == "." || base == string(filepath.Separator) {
		return "", "", &CLIError{Code: "invalid_source", Message: "push: source has no usable directory name"}
	}
	return abs, base, nil
}

// pushResolveDestination turns the requested extraction directory into a clean
// absolute guest path. Unlike the temporary archive, the destination is a
// normal exec path: absolute paths are kept and relative paths start at
// /home/user. NUL, traversal and the filesystem root are rejected before any
// request.
func pushResolveDestination(destination, base string) (string, error) {
	if strings.ContainsRune(destination, '\x00') {
		return "", &CLIError{Code: "invalid_destination", Message: "push: destination contains a NUL byte"}
	}
	if destination == "" {
		destination = base
	}
	for part := range strings.SplitSeq(destination, "/") {
		if part == ".." {
			return "", &CLIError{Code: "invalid_destination", Message: "push: destination must not contain .."}
		}
	}
	joined := destination
	if !path.IsAbs(joined) {
		joined = path.Join(pushGuestHome, joined)
	}
	cleaned := path.Clean(joined)
	if cleaned == "/" {
		return "", &CLIError{Code: "invalid_destination", Message: "push: destination must not be the filesystem root"}
	}
	return cleaned, nil
}

// pushArchivePath returns a random safe temporary tar path on the guest.
func pushArchivePath() (string, error) {
	var raw [pushArchiveRandomNameBytes]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", &CLIError{Code: "invalid_request", Message: "push: cannot choose a temporary archive name"}
	}
	return pushArchivePathPrefix + hex.EncodeToString(raw[:]) + ".tar", nil
}

// pushRequestUploadURL asks the API for a signed PUT URL for archivePath. It
// uses the client's raw request surface so a fake integration client does not
// need a new method, and validates every field before any archive byte is
// streamed.
func pushRequestUploadURL(ctx context.Context, client APIClient, sandboxID, archivePath string) (string, error) {
	ttl := pushUploadTTLSeconds
	body, err := json.Marshal(api.FileURLRequest{Path: archivePath, TTLSeconds: &ttl})
	if err != nil {
		return "", &CLIError{Code: "invalid_request", Message: "push: cannot encode the upload request"}
	}
	target := "/api/v1/sandboxes/" + url.PathEscape(sandboxID) + pushUploadPathSuffix
	// The raw client has no JSON convenience timeout, so issuance gets its own
	// bounded child while the archive upload keeps the caller's context.
	urlCtx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
	defer cancel()
	req, err := client.NewRequest(urlCtx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		if errors.Is(urlCtx.Err(), context.DeadlineExceeded) {
			return "", &CLIError{Code: "timeout", Message: "push: upload URL request timed out"}
		}
		return "", &CLIError{Code: "invalid_request", Message: "push: cannot build the upload request"}
	}
	req.Header.Set("Content-Type", pushUploadContentType)
	resp, err := client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(urlCtx.Err(), context.DeadlineExceeded) {
			return "", &CLIError{Code: "timeout", Message: "push: upload URL request timed out"}
		}
		return "", &CLIError{Code: "upload_url_failed", Message: "push: upload URL request failed"}
	}
	defer resp.Body.Close()

	raw, readErr := pushReadLimited(resp.Body, pushResponseMaxBytes)
	if readErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		if errors.Is(urlCtx.Err(), context.DeadlineExceeded) {
			return "", &CLIError{Code: "timeout", Message: "push: upload URL request timed out"}
		}
		return "", &CLIError{Code: "invalid_response", Message: "push: upload URL response could not be read"}
	}
	if resp.StatusCode != http.StatusOK {
		return "", decodeAPIError(resp.StatusCode, raw)
	}
	if !pushHasJSONMediaType(resp.Header) {
		return "", &CLIError{Code: "invalid_response", Message: "push: upload URL response has an unexpected media type"}
	}
	var response api.FileURLResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", &CLIError{Code: "invalid_response", Message: "push: upload URL response is invalid"}
	}
	if err := pushValidateUploadURL(response, archivePath); err != nil {
		return "", err
	}
	return response.URL, nil
}

// pushValidateUploadURL rejects an upload URL envelope that is not the pinned
// shape or that would leak credentials to an unsafe target. Error messages
// never quote the URL.
func pushValidateUploadURL(response api.FileURLResponse, archivePath string) error {
	invalid := &CLIError{Code: "invalid_response", Message: "push: upload URL response is invalid"}
	if !response.OK || response.Type != api.TypeFileUploadURL {
		return invalid
	}
	if response.Method != api.FileMethodPut {
		return invalid
	}
	if response.Path != archivePath {
		return invalid
	}
	if response.ExpiresAt.IsZero() || !response.ExpiresAt.After(time.Now()) {
		return invalid
	}
	parsed, err := url.Parse(response.URL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return invalid
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return invalid
	}
	return nil
}

// pushUploadArchive streams [WriteArchive] through a pipe into one PUT. The
// writer is always joined, including an early response, a reader failure and
// cancellation, so no goroutine survives the call.
func pushUploadArchive(ctx context.Context, upload HTTPDoer, target, source string) (ArchiveInfo, int64, error) {
	// This child context stops git and archive I/O as well as the HTTP write,
	// not only the pipe writer.
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	reader, writer := io.Pipe()
	type archiveOutcome struct {
		info ArchiveInfo
		err  error
	}
	done := make(chan archiveOutcome, 1)
	go func() {
		info, err := WriteArchive(uploadCtx, source, writer)
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			_ = writer.Close()
		}
		done <- archiveOutcome{info: info, err: err}
	}()

	counted := &pushCountingReader{reader: reader, consumed: make(chan struct{})}
	req, err := http.NewRequestWithContext(uploadCtx, http.MethodPut, target, counted)
	if err != nil {
		cancel()
		_ = reader.CloseWithError(errPushUploadAborted)
		<-done
		return ArchiveInfo{}, 0, &CLIError{Code: "invalid_request", Message: "push: cannot build the archive upload"}
	}
	resp, doErr := upload.Do(req)
	if doErr != nil {
		// The transport error can quote the signed URL, so it is never passed
		// through the pipe where the archive error path might print it. Our
		// own cancel makes the producer return a context error; that is not
		// the failure to report while the caller's context is still live.
		cancel()
		_ = reader.CloseWithError(errPushUploadAborted)
		outcome := <-done
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ArchiveInfo{}, 0, ctxErr
		}
		if outcome.err != nil && !pushArchiveAborted(outcome.err) {
			return ArchiveInfo{}, 0, fmt.Errorf("push: archive: %w", outcome.err)
		}
		return ArchiveInfo{}, 0, pushUploadError(doErr)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		cancel()
		_ = reader.CloseWithError(errPushUploadRejected)
		<-done
		return ArchiveInfo{}, 0, pushUploadRejection(resp.StatusCode)
	}
	if !counted.consumedByTransport() {
		// A real server answers only after it has read the whole archive. An
		// early 2xx is therefore a truncated upload and must not be accepted.
		cancel()
		_ = reader.CloseWithError(errPushUploadTruncated)
		<-done
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ArchiveInfo{}, 0, ctxErr
		}
		return ArchiveInfo{}, 0, &CLIError{Code: "upload_failed", Message: "push: upload ended before the archive was complete"}
	}

	outcome := <-done
	if outcome.err != nil {
		return ArchiveInfo{}, 0, fmt.Errorf("push: archive: %w", outcome.err)
	}
	// The success body of a v1-only URL is implementation-defined. It is
	// never parsed or drained: closing it when the defer runs is enough.
	return outcome.info, counted.n.Load(), nil
}

// errPushUploadRejected unblocks the archive writer after a non-2xx response.
var errPushUploadRejected = errors.New("push: upload rejected")

// errPushUploadTruncated unblocks the archive writer after an early 2xx.
var errPushUploadTruncated = errors.New("push: upload truncated")

// errPushUploadAborted unblocks the archive writer after a transport failure.
// It is a sentinel, not the transport error, so a URL-bearing error never
// reaches the archive error path.
var errPushUploadAborted = errors.New("push: upload aborted")

// pushArchiveAborted reports whether a producer error was caused by the
// upload's own abort (our child-context cancel or the pipe sentinel) rather
// than by a real archive failure. The caller's context is checked separately.
func pushArchiveAborted(err error) bool {
	return errors.Is(err, errPushUploadAborted) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// pushUploadRejection turns a signed-host failure into a safe status error. A
// signed host is not the bbraft API, and its body and headers are not read at
// all: either could quote the token-bearing URL or file content. Only the HTTP
// status is retained.
func pushUploadRejection(status int) error {
	return &APIError{
		Status:  status,
		Code:    fmt.Sprintf("http_%d", status),
		Message: "archive upload was rejected",
	}
}

// pushUploadError reduces a transport failure to a safe message. Redirect and
// URL errors can quote the signed URL, so their text is never retained.
func pushUploadError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	default:
		return &CLIError{Code: "upload_failed", Message: "push: archive upload failed"}
	}
}

// pushCountingReader counts the archive bytes the transport actually read and
// records the final EOF, which proves the transport consumed the whole tar.
type pushCountingReader struct {
	reader   io.Reader
	n        atomic.Int64
	consumed chan struct{}
	once     sync.Once
}

// Read implements io.Reader.
func (r *pushCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.n.Add(int64(n))
	if err != nil && errors.Is(err, io.EOF) {
		r.once.Do(func() { close(r.consumed) })
	}
	return n, err
}

// consumedByTransport reports whether the HTTP transport read the final EOF.
func (r *pushCountingReader) consumedByTransport() bool {
	select {
	case <-r.consumed:
		return true
	default:
		return false
	}
}

// pushExtract runs the remote extraction synchronously under an explicit
// positive timeout so push never performs the timeout-0 metadata discovery
// reserved for a run user command. The synchronous form is the one a v1-only server
// server accepts; the raw request path still allows the full 600-second bound.
// One command extracts and removes the tar through an EXIT trap that leaves the
// original exit status intact.
func pushExtract(ctx context.Context, client APIClient, sandboxID, archivePath, destination string) error {
	command := pushExtractCommand(archivePath, destination)
	_, err := ExecuteCommand(ctx, client, ExecutionRequest{
		SandboxID:      sandboxID,
		Command:        command,
		TimeoutSeconds: pushExtractTimeoutSeconds,
		Stream:         false,
	}, ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
	if err != nil {
		if exit, ok := errors.AsType[*ExitError](err); ok && exit.Err == nil {
			return &ExitError{
				Code: exit.Code,
				Err:  &CLIError{Code: "push_failed", Message: fmt.Sprintf("push: extract archive exited %d", exit.Code)},
			}
		}
		return err
	}
	return nil
}

// pushExtractCommand builds the remote shell script. Every path goes through
// execShellJoin so spaces, quotes, dollar signs and leading dashes stay
// literal.
func pushExtractCommand(archivePath, destination string) string {
	return "tmp=" + execShellJoin([]string{archivePath}) + "\n" +
		"dst=" + execShellJoin([]string{destination}) + "\n" +
		"cleanup() { rm -f -- \"$tmp\"; }\n" +
		"trap cleanup EXIT\n" +
		"mkdir -p -- \"$dst\" && tar -xf \"$tmp\" -C \"$dst\""
}

// pushCleanupArchive is the best-effort removal of a partial upload. It is
// detached from a cancelled caller but bounded, and its error is always
// discarded so the original failure is preserved.
func pushCleanupArchive(ctx context.Context, client APIClient, sandboxID, archivePath string) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pushCleanupTimeout)
	defer cancel()
	_, _ = ExecuteCommand(cleanupCtx, client, ExecutionRequest{
		SandboxID:      sandboxID,
		Command:        "rm -f -- " + execShellJoin([]string{archivePath}),
		TimeoutSeconds: pushCleanupCommandSeconds,
		Stream:         false,
	}, ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
}

// pushReport writes the completion notice. It never contains the upload URL or
// any credential.
func pushReport(out PushOutput, sandboxID string, result PushResult) error {
	if out.JSON != nil {
		event := struct {
			Event     string `json:"event"`
			SandboxID string `json:"sandboxId"`
			Directory string `json:"directory"`
			Files     int    `json:"files"`
			Bytes     int64  `json:"bytes"`
		}{
			Event:     "pushed",
			SandboxID: sandboxID,
			Directory: result.Directory,
			Files:     result.Files,
			Bytes:     result.Bytes,
		}
		raw, err := json.Marshal(event)
		if err != nil {
			return &CLIError{Code: "invalid_response", Message: "push: cannot encode completion event"}
		}
		raw = append(raw, '\n')
		if n, err := out.JSON.Write(raw); err != nil || n != len(raw) {
			return &CLIError{Code: "write_failed", Message: "push: write completion event failed"}
		}
		return nil
	}
	if out.Progress == nil {
		return nil
	}
	line := fmt.Sprintf("pushed %d files to %s\n", result.Files, result.Directory)
	if n, err := io.WriteString(out.Progress, line); err != nil || n != len(line) {
		return &CLIError{Code: "write_failed", Message: "push: write completion notice failed"}
	}
	return nil
}

// pushReadLimited reads at most limit bytes.
func pushReadLimited(r io.Reader, limit int) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > limit {
		return nil, errors.New("push: response is too large")
	}
	return raw, nil
}

// pushHasJSONMediaType reports whether the response is JSON, ignoring
// parameters such as charset.
func pushHasJSONMediaType(header http.Header) bool {
	mediaType, _, err := mime.ParseMediaType(header.Get("Content-Type"))
	return err == nil && strings.EqualFold(mediaType, "application/json")
}

// NewUploadClient returns a raw upload client that never follows a redirect,
// so a signed URL is not forwarded to another host. The injected client's
// transport, TLS configuration and timeout are preserved, while its cookie jar
// is dropped so no API-site cookie is sent to the signed host. Platform
// credentials are not added here.
//
// Redirects are reported as the original 3xx response, not as an error:
// net/http wraps a CheckRedirect error in a URL error that would quote the
// token-bearing signed URL.
func NewUploadClient(base *http.Client) *http.Client {
	var client http.Client
	if base != nil {
		client = *base
	}
	client.Jar = nil
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &client
}

// newPushCommand builds `bbraft push ID DIR [--to PATH]`.
func newPushCommand(a *App) *cobra.Command {
	var to string
	cmd := &cobra.Command{
		Use:   "push ID DIR",
		Short: "Copy a local directory into a sandbox",
		Long: "Copy a local directory into a sandbox as one tar archive.\n\n" +
			"In a git repository the archive holds tracked and untracked-not-ignored " +
			"files; outside one it holds every file except .git. Executable bits and " +
			"symbolic links are preserved, and remote files missing locally are kept.",
		Args: cobra.ExactArgs(2),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			out := PushOutput{Progress: a.out.Stderr}
			if a.out.JSON {
				out = PushOutput{JSON: a.out.Stdout}
			}
			_, err = PushDirectory(ctx, client, NewUploadClient(a.opts.HTTPClient), PushRequest{
				SandboxID:   args[0],
				Source:      args[1],
				Destination: to,
			}, out)
			return err
		}),
	}
	cmd.Flags().StringVar(&to, "to", "", "directory on the sandbox (default /home/user/<dir name>)")
	return cmd
}
