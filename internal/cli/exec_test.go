package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// execTestSandboxPath is the resolved command path of the fixture sandbox.
const execTestSandboxPath = "/api/v1/sandboxes/" + cliTestSandboxID + "/commands"

// execTestWant is the expected command request built by the command.
type execTestWant struct {
	path    string
	command string
	cwd     string
	timeout int
	stream  bool
}

// execTestCheckCommandRequest validates the one POST the CLI may send.
func execTestCheckCommandRequest(t *testing.T, r *http.Request, want execTestWant) {
	t.Helper()
	if r.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", r.Method)
	}
	if got := r.URL.EscapedPath(); got != want.path {
		t.Errorf("path = %q, want %q", got, want.path)
	}
	if got := r.Header.Get("Authorization"); !cliTestSecretEqual(got, "Bearer test-key") {
		t.Error("Authorization does not carry the fixture bearer key")
	}
	if got := r.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if want.stream {
		if got := r.Header.Get("Accept"); got != compat.NDJSONMediaType {
			t.Errorf("Accept = %q, want %q", got, compat.NDJSONMediaType)
		}
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read request body: %v", err)
		return
	}
	var members map[string]jsontext.Value
	if err := json.Unmarshal(raw, &members); err != nil {
		t.Errorf("decode request members: %v", err)
		return
	}
	for _, absent := range []string{"detached"} {
		if _, ok := members[absent]; ok {
			t.Errorf("request sent unsupported member %q", absent)
		}
	}
	var body api.CommandRequest
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Errorf("decode command request: %v", err)
		return
	}
	if body.Command != want.command {
		t.Errorf("command = %q, want %q", body.Command, want.command)
	}
	if body.TimeoutSeconds == nil || *body.TimeoutSeconds != want.timeout {
		t.Errorf("timeoutSeconds = %v, want %d", body.TimeoutSeconds, want.timeout)
	}
	if body.Stream == nil || *body.Stream != want.stream {
		t.Errorf("stream = %v, want %v", body.Stream, want.stream)
	}
	if want.cwd == "" {
		if _, ok := members["cwd"]; ok {
			t.Error("request sent cwd when none was requested")
		}
	} else if body.Cwd == nil || *body.Cwd != want.cwd {
		t.Errorf("cwd = %v, want %q", body.Cwd, want.cwd)
	}
}

// execTestNewHuman builds a command environment in human-output mode.
func execTestNewHuman(t *testing.T, handler http.HandlerFunc) *cliTestEnv {
	t.Helper()
	return cliTestNew(t, handler, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
}

// execTestWriteSync writes a pinned synchronous command envelope.
func execTestWriteSync(t *testing.T, w http.ResponseWriter, response api.CommandResponse) {
	t.Helper()
	cliTestWriteJSON(t, w, http.StatusOK, response)
}

// execTestWriteRaw writes a raw response body with the given content type.
func execTestWriteRaw(t *testing.T, w http.ResponseWriter, status int, contentType, body string) {
	t.Helper()
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("write response: %v", err)
	}
}

// execTestWriteInfo writes the approved metadata envelope.
func execTestWriteInfo(t *testing.T, w http.ResponseWriter, streamMax, syncMax any) {
	t.Helper()
	cliTestWriteJSON(t, w, http.StatusOK, map[string]any{
		"ok":      true,
		"type":    execInfoType,
		"version": "exec-test",
		"commands": map[string]any{
			"streamMaxSeconds": streamMax,
			"syncMaxSeconds":   syncMax,
		},
	})
}

// TestExecCommandLine pins the one-line buffer and argv quoting semantics.
func TestExecCommandLine(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "single line unchanged", args: []string{`echo "$HOME" && ls`}, want: `echo "$HOME" && ls`},
		{name: "single empty unchanged", args: []string{""}, want: ""},
		{name: "single leading dash unchanged", args: []string{"-lc"}, want: "-lc"},
		{name: "argv simple", args: []string{"echo", "hi"}, want: "'echo' 'hi'"},
		{name: "argv empty", args: []string{"echo", ""}, want: "'echo' ''"},
		{name: "argv newline", args: []string{"printf", "a\nb"}, want: "'printf' 'a\nb'"},
		{name: "argv single quote", args: []string{"echo", "it's"}, want: `'echo' 'it'\''s'`},
		{
			name: "argv metacharacters",
			args: []string{"sh", "-c", "$(id); `id` | & > < *"},
			want: "'sh' '-c' '$(id); `id` | & > < *'",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := execCommandLine(tc.args); got != tc.want {
				t.Fatalf("execCommandLine(%q) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

// TestExecShellJoinRoundTrip proves the quoting reproduces each argv word
// exactly through a real POSIX shell.
func TestExecShellJoinRoundTrip(t *testing.T) {
	cases := [][]string{
		{},
		{""},
		{"plain"},
		{"two words"},
		{"it's"},
		{"a\nb"},
		{"$HOME"},
		{"*"},
		{"a\tb"},
		{"semi;colon|pipe&"},
	}
	for _, args := range cases {
		script := "set -- " + execShellJoin(args) + `; for a in "$@"; do printf '%s\0' "$a"; done`
		cmd := exec.CommandContext(t.Context(), "sh", "-c", script)
		got, err := cmd.Output()
		if err != nil {
			t.Fatalf("round trip %q: %v", args, err)
		}
		var want []byte
		for _, arg := range args {
			want = append(want, arg...)
			want = append(want, 0)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("round trip %q = %q, want %q", args, got, want)
		}
	}
}

// TestExecUsageErrorsNoHTTP proves parse and flag failures exit 2 before any
// request is sent.
func TestExecUsageErrorsNoHTTP(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "missing everything", args: nil},
		{name: "missing command", args: []string{cliTestSandboxID}},
		{name: "empty command", args: []string{cliTestSandboxID, ""}},
		{name: "whitespace command", args: []string{cliTestSandboxID, "   "}},
		{name: "terminator without command", args: []string{cliTestSandboxID, "--"}},
		{name: "negative timeout", args: []string{"--timeout", "-1", cliTestSandboxID, "true"}},
		{name: "timeout above uint32", args: []string{"--timeout", "4294967296", cliTestSandboxID, "true"}},
		{name: "timeout parse overflow", args: []string{"--timeout", "9223372036854775808", cliTestSandboxID, "true"}},
		{name: "unknown flag", args: []string{"--nope", cliTestSandboxID, "true"}},
		{name: "flag without value", args: []string{"--timeout"}},
		{name: "detach without command", args: []string{"--detach", cliTestSandboxID}},
		{name: "detach with timeout and no command", args: []string{"--detach", "--timeout", "30", cliTestSandboxID}},
		{name: "status", args: []string{"--status", "123", cliTestSandboxID, "true"}},
		{name: "kill", args: []string{"--kill", "123", cliTestSandboxID, "true"}},
		{name: "attach", args: []string{"--attach", "123", cliTestSandboxID, "true"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			env := cliTestNew(t, func(http.ResponseWriter, *http.Request) {
				calls.Add(1)
			}, nil)
			args := append([]string{"exec"}, tc.args...)
			if got := env.run(t, args...); got != 2 {
				t.Fatalf("exit status = %d, want 2; stderr=%q", got, env.stderr.String())
			}
			if calls.Load() != 0 {
				t.Fatalf("server received %d requests, want 0", calls.Load())
			}
		})
	}
}

// TestExecRequestShape pins flag handling and command construction.
func TestExecRequestShape(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want execTestWant
	}{
		{
			name: "defaults",
			args: []string{cliTestSandboxID, "true"},
			want: execTestWant{path: execTestSandboxPath, command: "true", timeout: 30},
		},
		{
			name: "cwd and timeout",
			args: []string{"--cwd", "src", "--timeout", "45", cliTestSandboxID, "go test"},
			want: execTestWant{path: execTestSandboxPath, command: "go test", cwd: "src", timeout: 45},
		},
		{
			name: "above 600 implies stream",
			args: []string{"--timeout", "900", cliTestSandboxID, "true"},
			want: execTestWant{path: execTestSandboxPath, command: "true", timeout: 900, stream: true},
		},
		{
			name: "stream flag",
			args: []string{"--stream", "--timeout", "45", cliTestSandboxID, "true"},
			want: execTestWant{path: execTestSandboxPath, command: "true", timeout: 45, stream: true},
		},
		{
			name: "multiple args are quoted argv",
			args: []string{cliTestSandboxID, "echo", "a b"},
			want: execTestWant{path: execTestSandboxPath, command: "'echo' 'a b'", timeout: 30},
		},
		{
			name: "late flags and terminator",
			args: []string{cliTestSandboxID, "--timeout", "15", "--", "printf", "%s\\n", "a b"},
			want: execTestWant{path: execTestSandboxPath, command: "'printf' '%s\\n' 'a b'", timeout: 15},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.want
			handler := func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == execInfoPath {
					// No detached member: this fixture is a legacy server.
					execTestWriteInfo(t, w, 7200, 600)
					return
				}
				execTestCheckCommandRequest(t, r, want)
				if want.stream {
					execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType,
						"{\"type\":\"started\"}\n{\"type\":\"exit\",\"exitCode\":0,\"success\":true,\"timedOut\":false}\n")
					return
				}
				execTestWriteSync(t, w, api.NewCommandResponse(true, new(0), "", "", false))
			}
			env := execTestNewHuman(t, handler)
			if got := env.run(t, append([]string{"exec"}, tc.args...)...); got != 0 {
				t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
			}
		})
	}
}

// TestExecEscapesSandboxID proves the id is escaped into the path.
func TestExecEscapesSandboxID(t *testing.T) {
	const id = "bx_a/b?c d"
	want := execTestWant{
		path:    "/api/v1/sandboxes/bx_a%2Fb%3Fc%20d/commands",
		command: "true",
		timeout: 30,
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		execTestCheckCommandRequest(t, r, want)
		execTestWriteSync(t, w, api.NewCommandResponse(true, new(0), "", "", false))
	}
	env := execTestNewHuman(t, handler)
	if got := env.run(t, "exec", id, "true"); got != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
	}
}

// TestExecSyncResponses covers the pinned synchronous exit shapes.
func TestExecSyncResponses(t *testing.T) {
	cases := []struct {
		name       string
		response   api.CommandResponse
		wantExit   int
		wantStdout string
		wantStderr string
	}{
		{
			name:       "success",
			response:   api.NewCommandResponse(true, new(0), "out", "err", false),
			wantExit:   0,
			wantStdout: "out",
			wantStderr: "err",
		},
		{
			name:       "exit seven",
			response:   api.NewCommandResponse(false, new(7), "out", "err", false),
			wantExit:   7,
			wantStdout: "out",
			wantStderr: "err",
		},
		{
			name:       "timeout",
			response:   api.NewCommandResponse(false, nil, "partial", "warn", true),
			wantExit:   124,
			wantStdout: "partial",
			wantStderr: "warn",
		},
		{
			name:       "signal null exit code",
			response:   api.NewCommandResponse(false, nil, "partial", "", false),
			wantExit:   1,
			wantStdout: "partial",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30}
			handler := func(w http.ResponseWriter, r *http.Request) {
				execTestCheckCommandRequest(t, r, want)
				execTestWriteSync(t, w, tc.response)
			}
			env := execTestNewHuman(t, handler)
			if got := env.run(t, "exec", cliTestSandboxID, "true"); got != tc.wantExit {
				t.Fatalf("exit status = %d, want %d; stderr=%q", got, tc.wantExit, env.stderr.String())
			}
			if env.stdout.String() != tc.wantStdout {
				t.Errorf("stdout = %q, want %q", env.stdout.String(), tc.wantStdout)
			}
			if env.stderr.String() != tc.wantStderr {
				t.Errorf("stderr = %q, want %q", env.stderr.String(), tc.wantStderr)
			}
		})
	}
}

// TestExecSyncJSON validates the emitted finished envelope against the pin.
func TestExecSyncJSON(t *testing.T) {
	want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30}
	response := api.NewCommandResponse(true, new(0), "out", "err", false)
	handler := func(w http.ResponseWriter, r *http.Request) {
		execTestCheckCommandRequest(t, r, want)
		execTestWriteSync(t, w, response)
	}
	env := cliTestNew(t, handler, nil)
	if got := env.run(t, "exec", cliTestSandboxID, "true"); got != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
	}
	line := strings.TrimSpace(env.stdout.String())
	validator := compat.Load(t)
	if err := validator.Response("command", http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(line)); err != nil {
		t.Fatalf("emitted sync response does not match the pin: %v", err)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 1 || lines[0]["type"] != api.TypeCommandFinished {
		t.Fatalf("json lines = %v", lines)
	}
	if lines[0]["stdout"] != "out" || lines[0]["stderr"] != "err" {
		t.Fatalf("json output = %v", lines[0])
	}
}

// TestExecSyncMalformed rejects envelopes that do not satisfy the pin.
func TestExecSyncMalformed(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "not json", body: "{"},
		{name: "wrong type", body: `{"ok":true,"type":"sandbox.info","success":true,"exitCode":0,"stdout":"","stderr":"","timedOut":false}`},
		{name: "not ok", body: `{"ok":false,"type":"command.finished","success":false,"exitCode":0,"stdout":"","stderr":"","timedOut":false}`},
		{name: "missing success", body: `{"ok":true,"type":"command.finished","exitCode":0,"stdout":"","stderr":"","timedOut":false}`},
		{name: "missing timedOut", body: `{"ok":true,"type":"command.finished","success":true,"exitCode":0,"stdout":"","stderr":""}`},
		{name: "missing exitCode", body: `{"ok":true,"type":"command.finished","success":true,"stdout":"","stderr":"","timedOut":false}`},
		{name: "missing stdout", body: `{"ok":true,"type":"command.finished","success":true,"exitCode":0,"stderr":"","timedOut":false}`},
		{name: "null success hides failure", body: `{"ok":true,"type":"command.finished","success":null,"exitCode":7,"stdout":"","stderr":"","timedOut":false}`},
		{name: "null timedOut", body: `{"ok":true,"type":"command.finished","success":false,"exitCode":7,"stdout":"","stderr":"","timedOut":null}`},
		{name: "null stdout", body: `{"ok":true,"type":"command.finished","success":true,"exitCode":0,"stdout":null,"stderr":"","timedOut":false}`},
		{name: "null stderr", body: `{"ok":true,"type":"command.finished","success":true,"exitCode":0,"stdout":"","stderr":null,"timedOut":false}`},
		{name: "null ok", body: `{"ok":null,"type":"command.finished","success":true,"exitCode":0,"stdout":"","stderr":"","timedOut":false}`},
		{name: "null type", body: `{"ok":true,"type":null,"success":true,"exitCode":0,"stdout":"","stderr":"","timedOut":false}`},
		{name: "timeout with code", body: `{"ok":true,"type":"command.finished","success":false,"exitCode":5,"stdout":"","stderr":"","timedOut":true}`},
		{name: "timeout success", body: `{"ok":true,"type":"command.finished","success":true,"exitCode":null,"stdout":"","stderr":"","timedOut":true}`},
		{name: "success mismatch", body: `{"ok":true,"type":"command.finished","success":true,"exitCode":7,"stdout":"","stderr":"","timedOut":false}`},
		{name: "negative exit code", body: `{"ok":true,"type":"command.finished","success":false,"exitCode":-1,"stdout":"","stderr":"","timedOut":false}`},
		{name: "string exit code", body: `{"ok":true,"type":"command.finished","success":false,"exitCode":"7","stdout":"","stderr":"","timedOut":false}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := func(w http.ResponseWriter, r *http.Request) {
				execTestWriteRaw(t, w, http.StatusOK, "application/json", tc.body)
			}
			env := execTestNewHuman(t, handler)
			got := env.run(t, "exec", cliTestSandboxID, "sentinel-command --secret")
			out := env.stdout.String() + env.stderr.String()
			if strings.Contains(out, "sentinel-command") {
				t.Fatal("a malformed response failure echoed the command")
			}
			if got != 1 {
				t.Fatalf("exit status = %d, want 1", got)
			}
			if !strings.Contains(env.stderr.String(), "bbraft:") {
				t.Fatal("want a CLI error line")
			}
		})
	}
}

// TestExecStreamFrames covers the happy stream and its terminal statuses.
func TestExecStreamFrames(t *testing.T) {
	body := strings.Join([]string{
		`{"type":"started"}`,
		`{"type":"stdout","data":"héllo "}`,
		`{"type":"stderr","data":"warn"}`,
		`{"type":"stdout","data":"€"}`,
		`{"type":"exit","exitCode":0,"success":true,"timedOut":false}`,
	}, "\n") + "\n"

	t.Run("human", func(t *testing.T) {
		want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30, stream: true}
		handler := func(w http.ResponseWriter, r *http.Request) {
			execTestCheckCommandRequest(t, r, want)
			execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType, body)
		}
		env := execTestNewHuman(t, handler)
		if got := env.run(t, "exec", "--stream", cliTestSandboxID, "true"); got != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
		}
		if env.stdout.String() != "héllo €" {
			t.Errorf("stdout = %q", env.stdout.String())
		}
		if env.stderr.String() != "warn" {
			t.Errorf("stderr = %q", env.stderr.String())
		}
	})

	t.Run("json frames match the pin", func(t *testing.T) {
		want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30, stream: true}
		handler := func(w http.ResponseWriter, r *http.Request) {
			execTestCheckCommandRequest(t, r, want)
			execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType, body)
		}
		env := cliTestNew(t, handler, nil)
		if got := env.run(t, "exec", "--stream", cliTestSandboxID, "true"); got != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
		}
		validator := compat.Load(t)
		header := http.Header{"Content-Type": []string{compat.NDJSONMediaType}}
		if err := validator.NDJSON("command", http.StatusOK, header, env.stdout.Bytes()); err != nil {
			t.Fatalf("emitted frames do not match the pin: %v", err)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 5 {
			t.Fatalf("frames = %d, want 5", len(lines))
		}
		if lines[0]["type"] != "started" || lines[len(lines)-1]["type"] != "exit" {
			t.Fatalf("frame order = %v", lines)
		}
	})

	t.Run("exit seven", func(t *testing.T) {
		exitBody := "{\"type\":\"started\"}\n{\"type\":\"exit\",\"exitCode\":7,\"success\":false,\"timedOut\":false}\n"
		want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30, stream: true}
		handler := func(w http.ResponseWriter, r *http.Request) {
			execTestCheckCommandRequest(t, r, want)
			execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType, exitBody)
		}
		env := execTestNewHuman(t, handler)
		if got := env.run(t, "exec", "--stream", cliTestSandboxID, "true"); got != 7 {
			t.Fatalf("exit status = %d, want 7; stderr=%q", got, env.stderr.String())
		}
	})

	t.Run("timeout exits 124", func(t *testing.T) {
		exitBody := "{\"type\":\"started\"}\n{\"type\":\"exit\",\"exitCode\":null,\"success\":false,\"timedOut\":true}\n"
		want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30, stream: true}
		handler := func(w http.ResponseWriter, r *http.Request) {
			execTestCheckCommandRequest(t, r, want)
			execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType, exitBody)
		}
		env := execTestNewHuman(t, handler)
		if got := env.run(t, "exec", "--stream", cliTestSandboxID, "true"); got != 124 {
			t.Fatalf("exit status = %d, want 124; stderr=%q", got, env.stderr.String())
		}
	})

	t.Run("large valid frame", func(t *testing.T) {
		data := strings.Repeat("x", 100<<10)
		largeBody := "{\"type\":\"started\"}\n" +
			"{\"type\":\"stdout\",\"data\":\"" + data + "\"}\n" +
			"{\"type\":\"exit\",\"exitCode\":0,\"success\":true,\"timedOut\":false}\n"
		want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30, stream: true}
		handler := func(w http.ResponseWriter, r *http.Request) {
			execTestCheckCommandRequest(t, r, want)
			execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType, largeBody)
		}
		env := execTestNewHuman(t, handler)
		if got := env.run(t, "exec", "--stream", cliTestSandboxID, "true"); got != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
		}
		if env.stdout.String() != data {
			t.Fatalf("stdout length = %d, want %d", env.stdout.Len(), len(data))
		}
	})
}

// TestExecStreamErrorFrame pins the terminal error behavior: one error, not a
// duplicate JSON error line after the frame was emitted.
func TestExecStreamErrorFrame(t *testing.T) {
	body := strings.Join([]string{
		`{"type":"started"}`,
		`{"type":"error","error":"sandbox_direct_failed","message":"the sandbox machine could not complete the call","retryable":false}`,
	}, "\n") + "\n"

	t.Run("human prints once", func(t *testing.T) {
		want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30, stream: true}
		handler := func(w http.ResponseWriter, r *http.Request) {
			execTestCheckCommandRequest(t, r, want)
			execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType, body)
		}
		env := execTestNewHuman(t, handler)
		if got := env.run(t, "exec", "--stream", cliTestSandboxID, "true"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		if strings.Count(env.stderr.String(), "the sandbox machine could not complete the call") != 1 {
			t.Fatalf("stderr = %q, want the message exactly once", env.stderr.String())
		}
	})

	t.Run("json does not duplicate", func(t *testing.T) {
		want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30, stream: true}
		handler := func(w http.ResponseWriter, r *http.Request) {
			execTestCheckCommandRequest(t, r, want)
			execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType, body)
		}
		env := cliTestNew(t, handler, nil)
		if got := env.run(t, "exec", "--stream", cliTestSandboxID, "true"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		if env.stderr.String() != "" {
			t.Fatalf("stderr = %q, want no duplicate error line", env.stderr.String())
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 2 || lines[1]["type"] != "error" {
			t.Fatalf("frames = %v", lines)
		}
	})
}

// TestExecStreamMalformed rejects every frame-order and shape violation.
func TestExecStreamMalformed(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "empty stream", body: ""},
		{name: "started only", body: `{"type":"started"}` + "\n"},
		{name: "output before started", body: `{"type":"stdout","data":"x"}` + "\n"},
		{name: "duplicate started", body: `{"type":"started"}` + "\n" + `{"type":"started"}` + "\n"},
		{name: "unknown type", body: `{"type":"started"}` + "\n" + `{"type":"bogus"}` + "\n"},
		{name: "malformed json", body: `{"type":"started"}` + "\n" + `{"type":` + "\n"},
		{name: "unknown member", body: `{"type":"started","bogus":true}` + "\n"},
		{name: "null stdout data", body: `{"type":"started"}` + "\n" + `{"type":"stdout","data":null}` + "\n"},
		{name: "wrong stdout data", body: `{"type":"started"}` + "\n" + `{"type":"stdout","data":5}` + "\n"},
		{name: "null retryable", body: `{"type":"started"}` + "\n" + `{"type":"error","error":"x","message":"m","retryable":null}` + "\n"},
		{name: "wrong retryable", body: `{"type":"started"}` + "\n" + `{"type":"error","error":"x","message":"m","retryable":"no"}` + "\n"},
		{name: "null error field", body: `{"type":"started"}` + "\n" + `{"type":"error","error":null,"message":"m","retryable":false}` + "\n"},
		{name: "trailing token", body: `{"type":"started"} {"type":"exit"}` + "\n"},
		{name: "array frame", body: `[]` + "\n"},
		{name: "null frame", body: `null` + "\n"},
		{
			name: "exit missing success",
			body: `{"type":"started"}` + "\n" + `{"type":"exit","exitCode":0,"timedOut":false}` + "\n",
		},
		{
			name: "exit missing timedOut",
			body: `{"type":"started"}` + "\n" + `{"type":"exit","exitCode":0,"success":true}` + "\n",
		},
		{
			name: "exit missing exitCode",
			body: `{"type":"started"}` + "\n" + `{"type":"exit","success":true,"timedOut":false}` + "\n",
		},
		{
			name: "timeout with code",
			body: `{"type":"started"}` + "\n" + `{"type":"exit","exitCode":5,"success":false,"timedOut":true}` + "\n",
		},
		{
			name: "success mismatch",
			body: `{"type":"started"}` + "\n" + `{"type":"exit","exitCode":5,"success":true,"timedOut":false}` + "\n",
		},
		{
			name: "error without code",
			body: `{"type":"started"}` + "\n" + `{"type":"error","message":"x","retryable":false}` + "\n",
		},
		{
			name: "frame after terminal",
			body: `{"type":"started"}` + "\n" +
				`{"type":"exit","exitCode":0,"success":true,"timedOut":false}` + "\n" +
				`{"type":"stdout","data":"late"}` + "\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := execTestWant{path: execTestSandboxPath, command: "true", timeout: 30, stream: true}
			handler := func(w http.ResponseWriter, r *http.Request) {
				execTestCheckCommandRequest(t, r, want)
				execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType, tc.body)
			}
			env := execTestNewHuman(t, handler)
			if got := env.run(t, "exec", "--stream", cliTestSandboxID, "true"); got != 1 {
				t.Fatalf("exit status = %d, want 1; stderr=%q", got, env.stderr.String())
			}
			if !strings.Contains(env.stderr.String(), "bbraft:") {
				t.Fatalf("stderr = %q, want an error line", env.stderr.String())
			}
		})
	}
}

// TestExecReadNDJSONLineBounded proves the frame reader has no 64 KiB cap and
// still bounds a single frame.
func TestExecReadNDJSONLineBounded(t *testing.T) {
	large := strings.Repeat("a", 100<<10) + "\n"
	reader := bufio.NewReaderSize(strings.NewReader(large), 8)
	line, err := execReadNDJSONLine(reader, 200<<10)
	if err != nil {
		t.Fatalf("read large line: %v", err)
	}
	if len(line) != len(large) {
		t.Fatalf("line length = %d, want %d", len(line), len(large))
	}

	reader = bufio.NewReaderSize(strings.NewReader(`{"type":"started"}`+"\n"), 8)
	if _, err := execReadNDJSONLine(reader, 8); !errors.Is(err, errExecFrameTooLarge) {
		t.Fatalf("bounded read error = %v, want errExecFrameTooLarge", err)
	}
}

// TestExecTimeoutZeroMetadata pins the approved discovery contract.
func TestExecTimeoutZeroMetadata(t *testing.T) {
	t.Run("success fetches once and sends the exact cap", func(t *testing.T) {
		var infoCalls, commandCalls atomic.Int64
		handler := func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case execInfoPath:
				infoCalls.Add(1)
				if r.Method != http.MethodGet {
					t.Errorf("metadata method = %s, want GET", r.Method)
				}
				if got := r.Header.Get("Authorization"); !cliTestSecretEqual(got, "Bearer test-key") {
					t.Error("metadata request is not authenticated")
				}
				execTestWriteInfo(t, w, 1234, 600)
			case execTestSandboxPath:
				commandCalls.Add(1)
				execTestCheckCommandRequest(t, r, execTestWant{
					path: execTestSandboxPath, command: "true", timeout: 1234, stream: true,
				})
				execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType,
					"{\"type\":\"started\"}\n{\"type\":\"exit\",\"exitCode\":0,\"success\":true,\"timedOut\":false}\n")
			default:
				t.Errorf("unexpected path %q", r.URL.Path)
			}
		}
		env := execTestNewHuman(t, handler)
		if got := env.run(t, "exec", "--timeout", "0", cliTestSandboxID, "true"); got != 0 {
			t.Fatalf("exit status = %d, want 0", got)
		}
		if infoCalls.Load() != 1 || commandCalls.Load() != 1 {
			t.Fatalf("metadata/command calls = %d/%d, want 1/1", infoCalls.Load(), commandCalls.Load())
		}
		if env.stderr.String() != "" {
			t.Fatal("want no fallback note")
		}
	})

	t.Run("only zero discovers", func(t *testing.T) {
		var infoCalls, commandCalls atomic.Int64
		handler := func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case execInfoPath:
				infoCalls.Add(1)
				t.Error("metadata fetched for a non-zero timeout")
			case execTestSandboxPath:
				commandCalls.Add(1)
				execTestCheckCommandRequest(t, r, execTestWant{
					path: execTestSandboxPath, command: "true", timeout: 30,
				})
				execTestWriteSync(t, w, api.NewCommandResponse(true, new(0), "", "", false))
			default:
				t.Errorf("unexpected path %q", r.URL.Path)
			}
		}
		env := execTestNewHuman(t, handler)
		if got := env.run(t, "exec", cliTestSandboxID, "true"); got != 0 {
			t.Fatalf("exit status = %d, want 0", got)
		}
		if infoCalls.Load() != 0 || commandCalls.Load() != 1 {
			t.Fatalf("metadata/command calls = %d/%d, want 0/1", infoCalls.Load(), commandCalls.Load())
		}
	})

	t.Run("missing endpoint falls back to 600 with one note", func(t *testing.T) {
		var infoCalls, commandCalls atomic.Int64
		handler := func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case execInfoPath:
				infoCalls.Add(1)
				execTestWriteRaw(t, w, http.StatusNotFound, "application/json",
					`{"ok":false,"type":"sandbox.error","status":404,"code":"not_found","message":"sentinel-404",`+
						`"error":{"status":404,"code":"not_found","message":"sentinel-404"},"requestId":"req"}`)
			case execTestSandboxPath:
				commandCalls.Add(1)
				execTestCheckCommandRequest(t, r, execTestWant{
					path: execTestSandboxPath, command: "true", timeout: 600, stream: true,
				})
				execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType,
					"{\"type\":\"started\"}\n{\"type\":\"exit\",\"exitCode\":0,\"success\":true,\"timedOut\":false}\n")
			default:
				t.Errorf("unexpected path %q", r.URL.Path)
			}
		}
		env := execTestNewHuman(t, handler)
		got := env.run(t, "exec", "--timeout", "0", cliTestSandboxID, "true")
		note := env.stderr.String()
		if strings.Contains(note, "sentinel-404") {
			t.Fatal("fallback note leaked the response")
		}
		if got != 0 {
			t.Fatalf("exit status = %d, want 0", got)
		}
		if infoCalls.Load() != 1 || commandCalls.Load() != 1 {
			t.Fatalf("metadata/command calls = %d/%d, want 1/1", infoCalls.Load(), commandCalls.Load())
		}
		if strings.Count(note, "\n") != 1 || !strings.Contains(note, "600") {
			t.Fatal("want exactly one 600-second note")
		}
	})

	t.Run("short fallback note stops before the command", func(t *testing.T) {
		var commandCalls atomic.Int64
		handler := func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case execInfoPath:
				execTestWriteRaw(t, w, http.StatusNotFound, "application/json",
					`{"ok":false,"type":"sandbox.error","status":404,"code":"not_found","message":"missing"}`)
			case execTestSandboxPath:
				commandCalls.Add(1)
				t.Error("command sent after the fallback note failed")
			default:
				t.Errorf("unexpected path %q", r.URL.Path)
			}
		}
		noteWriter := new(execTestShortWriter)
		env := cliTestNew(t, handler, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
			opts.Stderr = noteWriter
		})
		if got := env.run(t, "exec", "--timeout", "0", cliTestSandboxID, "true"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		if noteWriter.calls.Load() == 0 {
			t.Fatal("the fallback note writer was never called")
		}
		if commandCalls.Load() != 0 {
			t.Fatalf("command calls = %d, want 0", commandCalls.Load())
		}
	})

	t.Run("metadata failures do not fall back", func(t *testing.T) {
		cases := []struct {
			name  string
			write func(http.ResponseWriter)
		}{
			{
				name: "unauthorized",
				write: func(w http.ResponseWriter) {
					cliTestWriteJSON(t, w, http.StatusUnauthorized,
						cliTestErrorEnvelope(http.StatusUnauthorized, "unauthorized", "unauthorized"))
				},
			},
			{
				name: "server error",
				write: func(w http.ResponseWriter) {
					cliTestWriteJSON(t, w, http.StatusInternalServerError,
						cliTestErrorEnvelope(http.StatusInternalServerError, "http_500", "internal"))
				},
			},
			{name: "malformed", write: func(w http.ResponseWriter) { _, _ = io.WriteString(w, "{") }},
			{
				name: "wrong type",
				write: func(w http.ResponseWriter) {
					cliTestWriteJSON(t, w, http.StatusOK, map[string]any{
						"ok": true, "type": "other", "version": "x",
						"commands": map[string]any{"streamMaxSeconds": 600, "syncMaxSeconds": 600},
					})
				},
			},
			{
				name: "not ok",
				write: func(w http.ResponseWriter) {
					cliTestWriteJSON(t, w, http.StatusOK, map[string]any{
						"ok": false, "type": execInfoType, "version": "x",
						"commands": map[string]any{"streamMaxSeconds": 600, "syncMaxSeconds": 600},
					})
				},
			},
			{
				name: "missing version",
				write: func(w http.ResponseWriter) {
					cliTestWriteJSON(t, w, http.StatusOK, map[string]any{
						"ok": true, "type": execInfoType,
						"commands": map[string]any{"streamMaxSeconds": 600, "syncMaxSeconds": 600},
					})
				},
			},
			{
				name: "missing commands",
				write: func(w http.ResponseWriter) {
					cliTestWriteJSON(t, w, http.StatusOK, map[string]any{
						"ok": true, "type": execInfoType, "version": "x",
					})
				},
			},
			{
				name: "zero stream cap",
				write: func(w http.ResponseWriter) {
					execTestWriteInfo(t, w, 0, 600)
				},
			},
			{
				name: "oversized stream cap",
				write: func(w http.ResponseWriter) {
					execTestWriteInfo(t, w, 4294967296, 600)
				},
			},
			{
				name: "string stream cap",
				write: func(w http.ResponseWriter) {
					execTestWriteInfo(t, w, "7200", 600)
				},
			},
			{
				name: "missing sync cap",
				write: func(w http.ResponseWriter) {
					execTestWriteInfo(t, w, 7200, nil)
				},
			},
			{
				name: "oversized body",
				write: func(w http.ResponseWriter) {
					_, _ = io.WriteString(w, strings.Repeat("x", execMetadataMaxBytes+1))
				},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var infoCalls, commandCalls atomic.Int64
				handler := func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case execInfoPath:
						infoCalls.Add(1)
						tc.write(w)
					case execTestSandboxPath:
						commandCalls.Add(1)
						t.Error("command sent after a metadata failure")
					default:
						t.Errorf("unexpected path %q", r.URL.Path)
					}
				}
				env := execTestNewHuman(t, handler)
				got := env.run(t, "exec", "--timeout", "0", cliTestSandboxID, "true")
				if strings.Contains(env.stderr.String(), "600-second stream timeout") {
					t.Fatal("want no fallback note")
				}
				if got != 1 {
					t.Fatalf("exit status = %d, want 1", got)
				}
				if infoCalls.Load() != 1 || commandCalls.Load() != 0 {
					t.Fatalf("metadata/command calls = %d/%d, want 1/0", infoCalls.Load(), commandCalls.Load())
				}
			})
		}
	})

	t.Run("network failure does not fall back", func(t *testing.T) {
		var trips atomic.Int64
		handler := func(http.ResponseWriter, *http.Request) {
			t.Error("server reached after a transport failure")
		}
		env := cliTestNew(t, handler, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
			base := opts.HTTPClient.Transport
			opts.HTTPClient = &http.Client{Transport: execTestFailTransport{base: base, trips: &trips}}
		})
		got := env.run(t, "exec", "--timeout", "0", cliTestSandboxID, "sentinel-command")
		out := env.stderr.String()
		if strings.Contains(out, "600-second stream timeout") || strings.Contains(out, "sentinel-command") || strings.Contains(out, "sentinel transport failure") {
			t.Fatal("metadata network failure leaked a sentinel or fallback note")
		}
		if got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		if trips.Load() != 1 {
			t.Fatalf("metadata transports = %d, want 1", trips.Load())
		}
	})
}

// execTestFailTransport fails only the metadata request.
type execTestFailTransport struct {
	base  http.RoundTripper
	trips *atomic.Int64
}

func (t execTestFailTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == execInfoPath {
		t.trips.Add(1)
		return nil, errors.New("sentinel transport failure")
	}
	return t.base.RoundTrip(req)
}

// TestExecAmbiguousErrorDoesNotRetry proves a 502 is a single POST.
func TestExecAmbiguousErrorDoesNotRetry(t *testing.T) {
	const sentinel = "sentinel-response-body"
	var calls atomic.Int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		execTestWriteRaw(t, w, http.StatusBadGateway, "application/json",
			`{"ok":false,"type":"sandbox.error","status":502,"code":"sandbox_direct_failed",`+
				`"message":"the sandbox machine could not complete the call",`+
				`"error":{"status":502,"code":"sandbox_direct_failed","message":"the sandbox machine could not complete the call"},`+
				`"requestId":"req","secret":"`+sentinel+`"}`)
	}
	env := execTestNewHuman(t, handler)
	got := env.run(t, "exec", cliTestSandboxID, "sentinel-command")
	out := env.stderr.String()
	if strings.Contains(out, sentinel) || strings.Contains(out, "sentinel-command") {
		t.Fatal("stderr leaked request or response data")
	}
	if got != 1 {
		t.Fatalf("exit status = %d, want 1", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want exactly 1", calls.Load())
	}
	if !strings.Contains(out, "sandbox_direct_failed") {
		t.Fatal("want the safe error code")
	}
}

// execTestFailWriter always fails, for output-failure cancellation tests.
type execTestFailWriter struct {
	calls atomic.Int64
}

func (w *execTestFailWriter) Write([]byte) (int, error) {
	w.calls.Add(1)
	return 0, errors.New("sentinel write failure")
}

// execTestShortWriter reports a short write with no error.
type execTestShortWriter struct {
	calls atomic.Int64
}

func (w *execTestShortWriter) Write(p []byte) (int, error) {
	w.calls.Add(1)
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

// execTestErrorWriter carries a sentinel that must never reach CLI output.
type execTestErrorWriter struct{}

func (execTestErrorWriter) Write([]byte) (int, error) {
	return 0, errors.New("sentinel writer error")
}

// execTestRunCancelsOnWriter proves a broken or short writer cancels the HTTP
// request and closes the streaming body.
func execTestRunCancelsOnWriter(t *testing.T, human bool, writer io.Writer, calls func() int64) {
	t.Helper()
	canceled := make(chan struct{})
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", compat.NDJSONMediaType)
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, `{"type":"started"}`+"\n")
		_, _ = io.WriteString(w, `{"type":"stdout","data":"out"}`+"\n")
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-time.After(2 * time.Second):
			t.Error("request was not cancelled after the output writer failed")
		}
	}
	env := cliTestNew(t, handler, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return human }
		opts.Stdout = writer
	})
	got := env.run(t, "exec", "--stream", cliTestSandboxID, "true")
	if strings.Contains(env.stderr.String(), "sentinel") {
		t.Fatal("stderr leaked the writer error")
	}
	if got != 1 {
		t.Fatalf("exit status = %d, want 1", got)
	}
	if calls() == 0 {
		t.Fatal("the failing writer was never called")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("request context was not cancelled")
	}
}

// TestExecOutputFailureCancelsRequest covers failed and short writes in both
// text and JSON modes.
func TestExecOutputFailureCancelsRequest(t *testing.T) {
	t.Run("fail human", func(t *testing.T) {
		writer := new(execTestFailWriter)
		execTestRunCancelsOnWriter(t, true, writer, func() int64 { return writer.calls.Load() })
	})
	t.Run("fail json", func(t *testing.T) {
		writer := new(execTestFailWriter)
		execTestRunCancelsOnWriter(t, false, writer, func() int64 { return writer.calls.Load() })
	})
	t.Run("short human", func(t *testing.T) {
		writer := new(execTestShortWriter)
		execTestRunCancelsOnWriter(t, true, writer, func() int64 { return writer.calls.Load() })
	})
	t.Run("short json", func(t *testing.T) {
		writer := new(execTestShortWriter)
		execTestRunCancelsOnWriter(t, false, writer, func() int64 { return writer.calls.Load() })
	})
}

// TestExecWriteDetectsShortWrite covers the writer contract directly and keeps
// arbitrary writer error text out of the CLI error.
func TestExecWriteDetectsShortWrite(t *testing.T) {
	short := new(execTestShortWriter)
	if err := execWriteString(short, "abc"); err == nil {
		t.Fatal("short text write was not detected")
	}
	if err := execWriteJSONLine(short, api.CommandStreamFrame{Type: api.CommandFrameStarted}); err == nil {
		t.Fatal("short JSON write was not detected")
	}
	err := execWriteString(execTestErrorWriter{}, "data")
	if err == nil {
		t.Fatal("writer error was not reported")
	}
	if strings.Contains(err.Error(), "sentinel writer error") {
		t.Fatal("output error leaked the writer text")
	}
}

// TestExecParentCancellation proves a cancelled parent stops the stream and
// does not send a second request.
func TestExecParentCancellation(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	var calls atomic.Int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", compat.NDJSONMediaType)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"type":"started"}`+"\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(started)
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-time.After(2 * time.Second):
			t.Error("server request did not observe client cancellation")
		}
	}
	env := execTestNewHuman(t, handler)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan int, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		env.app.SetArgs([]string{"exec", "--stream", cliTestSandboxID, "true"})
		done <- env.app.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("command goroutine did not stop")
		}
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("server never started the stream")
	}
	cancel()
	select {
	case status := <-done:
		if status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("command did not stop after cancellation")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe cancellation")
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want 1", calls.Load())
	}
}

// TestExecCommandValidation proves the helper rejects clear request errors
// before building a request.
func TestExecCommandValidation(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1", "test-key", nil)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	cases := []struct {
		name    string
		client  APIClient
		request ExecutionRequest
	}{
		{name: "nil client", client: nil, request: ExecutionRequest{SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30}},
		{name: "empty id", client: client, request: ExecutionRequest{Command: "true", TimeoutSeconds: 30}},
		{name: "empty command", client: client, request: ExecutionRequest{SandboxID: cliTestSandboxID, TimeoutSeconds: 30}},
		{name: "whitespace command", client: client, request: ExecutionRequest{SandboxID: cliTestSandboxID, Command: " \t\n", TimeoutSeconds: 30}},
		{name: "negative timeout", client: client, request: ExecutionRequest{SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: -1}},
		{name: "timeout too large", client: client, request: ExecutionRequest{SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: execMaxTimeoutSeconds + 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ExecuteCommand(t.Context(), tc.client, tc.request, ExecutionOutput{})
			if _, ok := errors.AsType[*UsageError](err); !ok {
				t.Fatalf("error = %v, want *UsageError", err)
			}
		})
	}
}

// TestExecClientBoundIncludesReadiness pins the readiness allowance that keeps
// a starting-sandbox wait from stealing the command's full timeout.
func TestExecClientBoundIncludesReadiness(t *testing.T) {
	const timeout int64 = 30
	got := execClientBound(timeout)
	want := api.DefaultCommandReadyDeadline + time.Duration(timeout)*time.Second + execClientGrace
	if got != want {
		t.Fatalf("execClientBound(%d) = %v, want %v", timeout, got, want)
	}
}

// TestExecReadinessWaitKeepsDeadline proves the client deadline still contains
// the whole readiness allowance after a simulated readiness delay, so a
// starting sandbox cannot steal time from the command itself.
func TestExecReadinessWaitKeepsDeadline(t *testing.T) {
	handler := func(w http.ResponseWriter, _ *http.Request) {
		execTestWriteSync(t, w, api.NewCommandResponse(true, new(0), "", "", false))
	}
	env := cliTestNew(t, handler, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
		base := opts.HTTPClient.Transport
		opts.HTTPClient = &http.Client{Transport: execTestRoundTripper(func(req *http.Request) (*http.Response, error) {
			deadline, ok := req.Context().Deadline()
			if !ok {
				t.Error("command request has no deadline")
			} else if remaining := time.Until(deadline); remaining < api.DefaultCommandReadyDeadline+30*time.Second {
				t.Errorf("deadline remaining %v does not include the readiness allowance", remaining)
			}
			// Simulate the server waiting for a starting sandbox before it
			// starts the command.
			time.Sleep(100 * time.Millisecond)
			return base.RoundTrip(req)
		})}
	})
	if got := env.run(t, "exec", cliTestSandboxID, "true"); got != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
	}
}

// TestExecHTTPResponseContract pins the exact success status, media type and
// media parameters accepted for sync and stream responses.
func TestExecHTTPResponseContract(t *testing.T) {
	syncBody, err := json.Marshal(api.NewCommandResponse(true, new(0), "", "", false))
	if err != nil {
		t.Fatalf("marshal sync response: %v", err)
	}
	streamBody := "{\"type\":\"started\"}\n{\"type\":\"exit\",\"exitCode\":0,\"success\":true,\"timedOut\":false}\n"
	cases := []struct {
		name        string
		stream      bool
		status      int
		contentType string
		body        string
		wantExit    int
	}{
		{name: "sync media parameters", status: http.StatusOK, contentType: "application/json; charset=utf-8", body: string(syncBody), wantExit: 0},
		{name: "sync missing content type", status: http.StatusOK, body: string(syncBody), wantExit: 1},
		{name: "sync wrong content type", status: http.StatusOK, contentType: "text/plain", body: string(syncBody), wantExit: 1},
		{name: "sync created status", status: http.StatusCreated, contentType: "application/json", body: string(syncBody), wantExit: 1},
		{name: "sync no content status", status: http.StatusNoContent, contentType: "application/json", wantExit: 1},
		{name: "stream media parameters", stream: true, status: http.StatusOK, contentType: "application/x-ndjson; charset=utf-8", body: streamBody, wantExit: 0},
		{name: "stream missing content type", stream: true, status: http.StatusOK, body: streamBody, wantExit: 1},
		{name: "stream wrong content type", stream: true, status: http.StatusOK, contentType: "application/json", body: streamBody, wantExit: 1},
		{name: "stream created status", stream: true, status: http.StatusCreated, contentType: "application/x-ndjson", body: streamBody, wantExit: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}
			env := execTestNewHuman(t, handler)
			args := []string{"exec"}
			if tc.stream {
				args = append(args, "--stream")
			}
			args = append(args, cliTestSandboxID, "true")
			if got := env.run(t, args...); got != tc.wantExit {
				t.Fatalf("exit status = %d, want %d; stderr=%q", got, tc.wantExit, env.stderr.String())
			}
		})
	}
}

// TestExecMetadataResponseContract pins metadata status and media handling.
func TestExecMetadataResponseContract(t *testing.T) {
	infoBody := `{"ok":true,"type":"bbraft.info","version":"exec-test",` +
		`"commands":{"streamMaxSeconds":900,"syncMaxSeconds":600}}`
	t.Run("media parameters accepted", func(t *testing.T) {
		var infoCalls, commandCalls atomic.Int64
		handler := func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case execInfoPath:
				infoCalls.Add(1)
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				_, _ = io.WriteString(w, infoBody)
			case execTestSandboxPath:
				commandCalls.Add(1)
				execTestWriteRaw(t, w, http.StatusOK, compat.NDJSONMediaType,
					"{\"type\":\"started\"}\n{\"type\":\"exit\",\"exitCode\":0,\"success\":true,\"timedOut\":false}\n")
			default:
				t.Errorf("unexpected path %q", r.URL.Path)
			}
		}
		env := execTestNewHuman(t, handler)
		if got := env.run(t, "exec", "--timeout", "0", cliTestSandboxID, "true"); got != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
		}
		if infoCalls.Load() != 1 || commandCalls.Load() != 1 {
			t.Fatalf("metadata/command calls = %d/%d, want 1/1", infoCalls.Load(), commandCalls.Load())
		}
	})

	cases := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{name: "missing content type", status: http.StatusOK, body: infoBody},
		{name: "wrong content type", status: http.StatusOK, contentType: "text/plain", body: infoBody},
		{name: "created status", status: http.StatusCreated, contentType: "application/json", body: infoBody},
		{name: "no content status", status: http.StatusNoContent, contentType: "application/json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var commandCalls atomic.Int64
			handler := func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case execInfoPath:
					w.Header().Set("Content-Type", tc.contentType)
					w.WriteHeader(tc.status)
					_, _ = io.WriteString(w, tc.body)
				case execTestSandboxPath:
					commandCalls.Add(1)
					t.Error("command sent after an invalid metadata response")
				default:
					t.Errorf("unexpected path %q", r.URL.Path)
				}
			}
			env := execTestNewHuman(t, handler)
			if got := env.run(t, "exec", "--timeout", "0", cliTestSandboxID, "true"); got != 1 {
				t.Fatalf("exit status = %d, want 1; stderr=%q", got, env.stderr.String())
			}
			if commandCalls.Load() != 0 {
				t.Fatalf("command calls = %d, want 0", commandCalls.Load())
			}
			if strings.Contains(env.stderr.String(), "600-second stream timeout") {
				t.Fatalf("stderr = %q, want no fallback note", env.stderr.String())
			}
		})
	}
}

// execTestRoundTripper lets a test return a synthetic response or error.
type execTestRoundTripper func(*http.Request) (*http.Response, error)

func (f execTestRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// execTestReadCloser yields data then err.
type execTestReadCloser struct {
	data []byte
	err  error
}

func (r *execTestReadCloser) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

func (r *execTestReadCloser) Close() error { return nil }

// TestExecLocalWatchdogExits124 covers the local bound expiring in the
// response read and before headers.
func TestExecLocalWatchdogExits124(t *testing.T) {
	syncBody := func(req *http.Request) *http.Response {
		return &http.Response{
			Status:        "200 OK",
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          &execTestReadCloser{err: context.DeadlineExceeded},
			ContentLength: -1,
			Request:       req,
		}
	}
	streamBody := func(req *http.Request) *http.Response {
		return &http.Response{
			Status:        "200 OK",
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{"application/x-ndjson"}},
			Body:          &execTestReadCloser{data: []byte(`{"type":"started"}` + "\n"), err: context.DeadlineExceeded},
			ContentLength: -1,
			Request:       req,
		}
	}
	cases := []struct {
		name   string
		stream bool
		trip   execTestRoundTripper
	}{
		{name: "sync body read", trip: func(req *http.Request) (*http.Response, error) { return syncBody(req), nil }},
		{name: "stream body read", stream: true, trip: func(req *http.Request) (*http.Response, error) { return streamBody(req), nil }},
		{name: "before headers", trip: func(*http.Request) (*http.Response, error) { return nil, context.DeadlineExceeded }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := cliTestNew(t, func(http.ResponseWriter, *http.Request) {
				t.Error("server reached through the synthetic transport")
			}, func(opts *Options) {
				opts.IsTTY = func(io.Writer) bool { return true }
				opts.HTTPClient = &http.Client{Transport: tc.trip}
			})
			args := []string{"exec"}
			if tc.stream {
				args = append(args, "--stream")
			}
			args = append(args, cliTestSandboxID, "true")
			if got := env.run(t, args...); got != 124 {
				t.Fatalf("exit status = %d, want 124; stderr=%q", got, env.stderr.String())
			}
			if !strings.Contains(env.stderr.String(), "command timed out") {
				t.Fatalf("stderr = %q, want the safe timeout error", env.stderr.String())
			}
		})
	}
}

// TestExecBodyReadFailureClassification pins caller cancellation/deadline
// precedence over the local watchdog.
func TestExecBodyReadFailureClassification(t *testing.T) {
	canceledParent, cancel := context.WithCancel(context.Background())
	cancel()
	if got := execBodyReadFailure(canceledParent, context.Background(), context.DeadlineExceeded); !errors.Is(got, context.Canceled) {
		t.Fatalf("cancelled parent error = %v, want context.Canceled", got)
	}

	deadlineParent, deadlineCancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer deadlineCancel()
	<-deadlineParent.Done()
	if got := execBodyReadFailure(deadlineParent, context.Background(), context.DeadlineExceeded); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("deadline parent error = %v, want context.DeadlineExceeded", got)
	}

	requestDeadline, requestCancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer requestCancel()
	<-requestDeadline.Done()
	got := execBodyReadFailure(context.Background(), requestDeadline, context.DeadlineExceeded)
	exit, ok := errors.AsType[*ExitError](got)
	if !ok || exit.Code != 124 {
		t.Fatalf("local watchdog error = %v, want ExitError 124", got)
	}
}
