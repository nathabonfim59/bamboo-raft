package cli

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

func TestLoginValidatesThenSaves(t *testing.T) {
	const key = "bbraft_secret_key"
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != api.PathMe {
			t.Errorf("path = %q, want %s", r.URL.Path, api.PathMe)
		}
		if got := r.Header.Get("Authorization"); !cliTestSecretEqual(got, "Bearer "+key) {
			t.Error("Authorization header does not carry the expected key")
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewMeResponse(api.MeUser{ID: "acct_1", Login: "alice"}))
	}, func(opts *Options) {
		opts.Stdin = strings.NewReader(key + "\n")
	})
	if status := env.run(t, "login", "--key-stdin"); status != 0 {
		t.Fatalf("exit status = %d, want 0", status)
	}
	if strings.Contains(env.stdout.String(), key) {
		t.Fatal("stdout contains the key")
	}
	lines := cliTestJSONLines(t, env.stdout)
	if lines[0]["event"] != "logged_in" {
		t.Fatalf("event = %v, want logged_in", lines[0])
	}
	saved, err := LoadConfig(t.Context(), env.dir, func(string) string { return "" })
	if err != nil {
		t.Fatalf("load saved config: %v", err)
	}
	if !cliTestSecretEqual(saved.APIKey, key) || saved.APIURL != env.server.URL {
		t.Fatalf("saved config mismatch: url=%q keyMatches=%v", saved.APIURL, cliTestSecretEqual(saved.APIKey, key))
	}
}

func TestLoginDoesNotSaveRejectedKey(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusUnauthorized, cliTestErrorEnvelope(http.StatusUnauthorized, "unauthorized", "invalid api key"))
	}, func(opts *Options) {
		opts.Stdin = strings.NewReader("bad-key")
	})
	if status := env.run(t, "login", "--key-stdin"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if lines[0]["event"] != "error" || lines[0]["code"] != "unauthorized" {
		t.Fatalf("event = %v, want unauthorized", lines[0])
	}
	if strings.Contains(env.stdout.String(), "bad-key") || strings.Contains(env.stderr.String(), "bad-key") {
		t.Fatal("key leaked to stdout or stderr")
	}
	entries, err := os.ReadDir(env.dir)
	if err != nil {
		t.Fatalf("read config dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("config dir contains %v, want empty", entries)
	}
}

func TestLoginRejectsEmptySuccessEnvelope(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}, func(opts *Options) {
		opts.Stdin = strings.NewReader("good-key")
	})
	if status := env.run(t, "login", "--key-stdin"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	entries, err := os.ReadDir(env.dir)
	if err != nil {
		t.Fatalf("read config dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("config dir contains %v, want empty", entries)
	}
}

func TestLoginRequiresKeyStdin(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
	if status := env.run(t, "login"); status != 2 {
		t.Fatalf("exit status = %d, want 2", status)
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
}

func TestLoginRejectsEmptyKey(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, func(opts *Options) {
		opts.Stdin = strings.NewReader("  \n")
	})
	if status := env.run(t, "login", "--key-stdin"); status != 2 {
		t.Fatalf("exit status = %d, want 2", status)
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
}

func TestLoginHonoursCancelledContext(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, func(opts *Options) {
		opts.Stdin = strings.NewReader("good-key")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if status := env.runContext(ctx, "login", "--key-stdin"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
}

// TestLoginCancelsBlockedStdin proves a cancelled context closes a closable
// stdin, joins the reader and returns instead of hanging.
func TestLoginCancelsBlockedStdin(t *testing.T) {
	var hits atomic.Int64
	reader, writer := io.Pipe()
	defer writer.Close()
	barrier := cliTestNewBarrierReader(reader)
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, func(opts *Options) {
		opts.Stdin = barrier
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() { done <- env.runContext(ctx, "login", "--key-stdin") }()
	select {
	case <-barrier.started:
	case <-time.After(time.Second):
		t.Fatal("login did not start reading stdin")
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("exit status = %d, want 1", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("login did not return after cancellation")
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
	closed := make(chan error, 1)
	go func() {
		_, err := writer.Write([]byte("x"))
		closed <- err
	}()
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("stdin is still open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("write blocked; stdin was not closed")
	}
}
