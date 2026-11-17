package cli

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// Response bounds of the detached process endpoints. A started or stopped
// response echoes the command text and its history record, so it gets the same
// generous bound as a synchronous command. A status response carries two
// pinned 8 MiB tails that JSON escaping can grow sixfold, so its bound leaves
// room for the worst valid answer.
const (
	execStartMaxBytes     = 8 << 20
	execKillMaxBytes      = 8 << 20
	execStatusMaxBytes    = 128 << 20
	execErrorBodyMaxBytes = 1 << 20
)

// execHTTPClient is the transport subset the detached process calls need.
// APIClient satisfies it, and tests can use a smaller fake without depending
// on the lifecycle methods added to APIClient by another command package.
type execHTTPClient interface {
	NewRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error)
	Do(req *http.Request) (*http.Response, error)
}

// execProcessAPI sends the pinned detached command operations over an
// authenticated API transport.
type execProcessAPI struct {
	client execHTTPClient
}

// execSandboxCommandsPath is the owner-scoped command collection path.
func execSandboxCommandsPath(sandboxID string) string {
	return "/api/v1/sandboxes/" + url.PathEscape(sandboxID) + "/commands"
}

// execProcessPath is the owner-scoped detached process path.
func execProcessPath(sandboxID string, processID uint32) string {
	return execSandboxCommandsPath(sandboxID) + "/" + strconv.FormatUint(uint64(processID), 10)
}

// startDetached starts a command in the background. The pinned request omits
// timeoutSeconds and stream: a detached command has no time limit and its
// output is read through the attach endpoint.
func (p execProcessAPI) startDetached(ctx context.Context, sandboxID, command, cwd string) (api.CommandStartedResponse, error) {
	var out api.CommandStartedResponse
	if sandboxID == "" {
		return out, &CLIError{Code: "invalid_request", Message: "exec: sandbox id is required"}
	}
	if strings.TrimSpace(command) == "" {
		return out, &CLIError{Code: "invalid_request", Message: "exec: command is required"}
	}
	body := struct {
		Command  string `json:"command"`
		Cwd      string `json:"cwd,omitzero"`
		Detached bool   `json:"detached"`
	}{Command: command, Cwd: cwd, Detached: true}
	raw, err := json.Marshal(body)
	if err != nil {
		return out, &CLIError{Code: "invalid_request", Message: "exec: encode request failed"}
	}
	answer, err := p.doJSON(ctx, http.MethodPost, execSandboxCommandsPath(sandboxID), nil, raw, execStartMaxBytes)
	if err != nil {
		return out, err
	}
	return execDecodeStarted(answer, command)
}

// status reads one detached process status, optionally from a byte offset.
func (p execProcessAPI) status(ctx context.Context, sandboxID string, processID uint32, query url.Values) (api.CommandStatusResponse, error) {
	var out api.CommandStatusResponse
	if processID == 0 {
		return out, &CLIError{Code: "invalid_request", Message: "exec: process id must be positive"}
	}
	answer, err := p.doJSON(ctx, http.MethodGet, execProcessPath(sandboxID, processID), query, nil, execStatusMaxBytes)
	if err != nil {
		return out, err
	}
	return execDecodeStatus(answer, processID)
}

// attach opens the pinned output stream of a detached process. It starts each
// stream at its raw byte offset. The caller owns and closes the response body.
func (p execProcessAPI) attach(ctx context.Context, sandboxID string, processID uint32, stdoutOffset, stderrOffset int64) (*http.Response, error) {
	if processID == 0 {
		return nil, &CLIError{Code: "invalid_request", Message: "exec: process id must be positive"}
	}
	if stdoutOffset < 0 || stderrOffset < 0 {
		return nil, &CLIError{Code: "invalid_request", Message: "exec: output offset must not be negative"}
	}
	query := url.Values{}
	query.Set("stdoutOffset", strconv.FormatInt(stdoutOffset, 10))
	query.Set("stderrOffset", strconv.FormatInt(stderrOffset, 10))
	req, err := p.client.NewRequest(ctx, http.MethodGet, execProcessPath(sandboxID, processID)+"/stream", nil)
	if err != nil {
		return nil, execInvalidResponse()
	}
	req.URL.RawQuery = query.Encode()
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		raw, readErr := execReadLimited(resp.Body, execErrorBodyMaxBytes)
		_ = resp.Body.Close()
		if readErr != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, execInvalidResponse()
		}
		return nil, decodeAPIError(resp.StatusCode, raw)
	}
	if !execHasMediaType(resp.Header, "application/x-ndjson") {
		_ = resp.Body.Close()
		return nil, execInvalidContentType()
	}
	return resp, nil
}

// kill stops a detached process and every process it started. The operation is
// idempotent: an already ended process answers 200 with signaled false.
func (p execProcessAPI) kill(ctx context.Context, sandboxID string, processID uint32) (api.CommandStopResponse, error) {
	var out api.CommandStopResponse
	if processID == 0 {
		return out, &CLIError{Code: "invalid_request", Message: "exec: process id must be positive"}
	}
	answer, err := p.doJSON(ctx, http.MethodDelete, execProcessPath(sandboxID, processID), nil, nil, execKillMaxBytes)
	if err != nil {
		return out, err
	}
	return execDecodeStopped(answer, sandboxID, processID)
}

// doJSON sends one JSON request, validates the media type and returns the
// bounded success body. A non-200 answer becomes the pinned API error; the
// error body is bounded independently.
func (p execProcessAPI) doJSON(ctx context.Context, method, path string, query url.Values, body []byte, successLimit int) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := p.client.NewRequest(ctx, method, path, reader)
	if err != nil {
		return nil, execInvalidResponse()
	}
	if query != nil {
		req.URL.RawQuery = query.Encode()
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	limit := successLimit
	if resp.StatusCode != http.StatusOK {
		limit = execErrorBodyMaxBytes
	}
	raw, readErr := execReadLimited(resp.Body, limit)
	if readErr != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, execInvalidResponse()
	}
	if resp.StatusCode != http.StatusOK {
		return nil, decodeAPIError(resp.StatusCode, raw)
	}
	if !execHasMediaType(resp.Header, "application/json") {
		return nil, execInvalidContentType()
	}
	return raw, nil
}

// execDecodeStarted validates the pinned command.started envelope and that it
// describes exactly the command that was sent.
func execDecodeStarted(raw []byte, wantCommand string) (api.CommandStartedResponse, error) {
	var out api.CommandStartedResponse
	members, err := execJSONMembers(raw)
	if err != nil {
		return out, execInvalidResponse()
	}
	if !execMemberBool(members, "ok", true) || !execMemberString(members, "type", api.TypeCommandStarted) ||
		!execMemberBool(members, "success", true) || !execMemberPositiveInt(members, "processId") ||
		!execMemberNonNegativeInt(members, "pid") || !execMemberString(members, "command", wantCommand) ||
		!execMemberNonEmptyString(members, "startedAt") {
		return out, execInvalidResponse()
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, execInvalidResponse()
	}
	if out.ProcessID == 0 || out.PID < 0 || out.Command != wantCommand || out.StartedAt.IsZero() {
		return out, execInvalidResponse()
	}
	if !out.OK || !out.Success || out.Type != api.TypeCommandStarted {
		return out, execInvalidResponse()
	}
	return out, nil
}

// execDecodeStatus validates the pinned command.status envelope. success is
// required but may be false: M2 reports running, lost, signal and non-zero-exit
// processes as unsuccessful while the status read itself succeeded. The wire
// status, running flag and exit members must stay coherent.
func execDecodeStatus(raw []byte, wantProcessID uint32) (api.CommandStatusResponse, error) {
	var out api.CommandStatusResponse
	members, err := execJSONMembers(raw)
	if err != nil {
		return out, execInvalidResponse()
	}
	status, ok := execMemberStringValue(members, "status")
	if !ok || (status != "running" && status != "exited" && status != "lost") {
		return out, execInvalidResponse()
	}
	if !execMemberBool(members, "ok", true) || !execMemberString(members, "type", api.TypeCommandStatus) ||
		!execMemberBoolValue(members, "success") || !execMemberPositiveInt(members, "processId") ||
		!execMemberBoolValue(members, "running") || !execMemberPresent(members, "exitCode") ||
		!execMemberStringKind(members, "stdout") || !execMemberStringKind(members, "stderr") {
		return out, execInvalidResponse()
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, execInvalidResponse()
	}
	if out.ProcessID != wantProcessID || out.Status != status {
		return out, execInvalidResponse()
	}
	switch status {
	case "running":
		if !out.Running || out.ExitCode != nil || out.Signal != nil {
			return out, execInvalidResponse()
		}
	case "lost":
		if out.Running || out.ExitCode != nil || out.Signal != nil {
			return out, execInvalidResponse()
		}
	case "exited":
		if out.Running || (out.ExitCode == nil) == (out.Signal == nil) {
			return out, execInvalidResponse()
		}
	}
	if out.ExitCode != nil && (*out.ExitCode < 0 || *out.ExitCode > 255) {
		return out, execInvalidResponse()
	}
	if err := execValidateStatusOffsets(out); err != nil {
		return out, err
	}
	return out, nil
}

// execValidateStatusOffsets rejects negative or backwards offset windows.
func execValidateStatusOffsets(out api.CommandStatusResponse) error {
	for _, pair := range [][2]*int64{
		{out.StdoutOffset, out.StdoutNextOffset},
		{out.StderrOffset, out.StderrNextOffset},
	} {
		start, next := pair[0], pair[1]
		if start != nil && *start < 0 {
			return execInvalidResponse()
		}
		if next != nil && *next < 0 {
			return execInvalidResponse()
		}
		if start != nil && next != nil && *next < *start {
			return execInvalidResponse()
		}
	}
	return nil
}

// execDecodeStopped validates the pinned command.stopped envelope and its
// optional history record. The nested record is validated from its raw object
// before the typed decode, so an absent required member cannot be synthesized
// as a zero value. processId is optional and nullable; when present it must
// name the stopped process. mode accepts the full pinned enum.
func execDecodeStopped(raw []byte, wantSandbox string, wantProcessID uint32) (api.CommandStopResponse, error) {
	var out api.CommandStopResponse
	members, err := execJSONMembers(raw)
	if err != nil {
		return out, execInvalidResponse()
	}
	if !execMemberBool(members, "ok", true) || !execMemberString(members, "type", api.TypeCommandStopped) ||
		!execMemberPositiveInt(members, "processId") || !execMemberBool(members, "running", false) ||
		!execMemberBoolValue(members, "signaled") || !execMemberPresent(members, "command") {
		return out, execInvalidResponse()
	}
	commandRaw := members["command"]
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, execInvalidResponse()
	}
	if out.ProcessID != wantProcessID || out.Running {
		return out, execInvalidResponse()
	}
	switch commandRaw.Kind() {
	case jsontext.KindNull:
		out.Command = nil
	case jsontext.KindBeginObject:
		record, err := execDecodeCommandRecord(commandRaw, wantSandbox, wantProcessID)
		if err != nil {
			return out, err
		}
		out.Command = &record
	default:
		return out, execInvalidResponse()
	}
	return out, nil
}

// execDecodeCommandRecord validates one pinned CommandRecord from its raw
// object. Every required member must be present with the pinned JSON kind; the
// optional processId is checked only when it is not null.
func execDecodeCommandRecord(raw jsontext.Value, wantSandbox string, wantProcessID uint32) (api.CommandRecord, error) {
	var record api.CommandRecord
	members, err := execJSONMembers(raw)
	if err != nil {
		return record, execInvalidResponse()
	}
	if !execMemberStringKind(members, "commandId") ||
		!execMemberStringKind(members, "command") ||
		!execMemberString(members, "sandboxId", wantSandbox) ||
		!execMemberStringKind(members, "mode") ||
		!execMemberStringKind(members, "status") ||
		!execMemberNonNegativeInt(members, "stdoutBytes") ||
		!execMemberNonNegativeInt(members, "stderrBytes") ||
		!execMemberBoolValue(members, "outputTruncated") ||
		!execMemberNonEmptyString(members, "startedAt") {
		return record, execInvalidResponse()
	}
	mode, _ := execMemberStringValue(members, "mode")
	switch mode {
	case "sync", "stream", "detached", "process":
	default:
		return record, execInvalidResponse()
	}
	status, _ := execMemberStringValue(members, "status")
	switch status {
	case "exited", "timed_out", "failed", "killed", "lost":
	default:
		return record, execInvalidResponse()
	}
	if value, ok := members["processId"]; ok && value.Kind() != jsontext.KindNull {
		if !execJSONInteger(value) {
			return record, execInvalidResponse()
		}
		processID, ok := execMemberIntValue(members, "processId")
		if !ok || processID != int64(wantProcessID) {
			return record, execInvalidResponse()
		}
	}
	if value, ok := members["cwd"]; ok && value.Kind() != jsontext.KindNull && !execJSONString(value) {
		return record, execInvalidResponse()
	}
	if value, ok := members["exitCode"]; ok && value.Kind() != jsontext.KindNull {
		exitCode, ok := execMemberIntValue(members, "exitCode")
		if !ok || exitCode < 0 || exitCode > 255 {
			return record, execInvalidResponse()
		}
	}
	if value, ok := members["signal"]; ok && value.Kind() != jsontext.KindNull && !execJSONString(value) {
		return record, execInvalidResponse()
	}
	if value, ok := members["finishedAt"]; ok && value.Kind() != jsontext.KindNull && !execJSONString(value) {
		return record, execInvalidResponse()
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return record, execInvalidResponse()
	}
	if record.StartedAt.IsZero() {
		return record, execInvalidResponse()
	}
	return record, nil
}

// execJSONMembers decodes one JSON object into its raw members.
func execJSONMembers(raw []byte) (map[string]jsontext.Value, error) {
	var members map[string]jsontext.Value
	if err := json.Unmarshal(raw, &members); err != nil {
		return nil, err
	}
	if members == nil {
		return nil, errors.New("not a JSON object")
	}
	return members, nil
}

// execMemberPresent reports whether name appears at all, including null.
func execMemberPresent(members map[string]jsontext.Value, name string) bool {
	_, ok := members[name]
	return ok
}

// execMemberBool reports whether name is the literal boolean want.
func execMemberBool(members map[string]jsontext.Value, name string, want bool) bool {
	value, ok := members[name]
	if !ok || !execJSONBool(value) {
		return false
	}
	raw := bytes.TrimSpace(value)
	return (string(raw) == "true") == want
}

// execMemberBoolValue reports whether name is any JSON boolean.
func execMemberBoolValue(members map[string]jsontext.Value, name string) bool {
	value, ok := members[name]
	return ok && execJSONBool(value)
}

// execMemberString reports whether name is the exact string want.
func execMemberString(members map[string]jsontext.Value, name, want string) bool {
	value, ok := members[name]
	if !ok || !execJSONString(value) {
		return false
	}
	var got string
	if err := json.Unmarshal(value, &got); err != nil {
		return false
	}
	return got == want
}

// execMemberStringValue returns name when it is a JSON string.
func execMemberStringValue(members map[string]jsontext.Value, name string) (string, bool) {
	value, ok := members[name]
	if !ok || !execJSONString(value) {
		return "", false
	}
	var got string
	if err := json.Unmarshal(value, &got); err != nil {
		return "", false
	}
	return got, true
}

// execMemberStringKind reports whether name is any JSON string.
func execMemberStringKind(members map[string]jsontext.Value, name string) bool {
	value, ok := members[name]
	return ok && execJSONString(value)
}

// execMemberNonEmptyString reports whether name is a non-empty JSON string.
func execMemberNonEmptyString(members map[string]jsontext.Value, name string) bool {
	value, ok := execMemberStringValue(members, name)
	return ok && strings.TrimSpace(value) != ""
}

// execMemberPositiveInt reports whether name is a positive JSON integer.
func execMemberPositiveInt(members map[string]jsontext.Value, name string) bool {
	got, ok := execMemberIntValue(members, name)
	return ok && got > 0
}

// execMemberNonNegativeInt reports whether name is a non-negative JSON
// integer.
func execMemberNonNegativeInt(members map[string]jsontext.Value, name string) bool {
	got, ok := execMemberIntValue(members, name)
	return ok && got >= 0
}

// execMemberIntValue returns name when it is a JSON integer.
func execMemberIntValue(members map[string]jsontext.Value, name string) (int64, bool) {
	value, ok := members[name]
	if !ok || !execJSONInteger(value) {
		return 0, false
	}
	got, err := strconv.ParseInt(string(bytes.TrimSpace(value)), 10, 64)
	if err != nil {
		return 0, false
	}
	return got, true
}
