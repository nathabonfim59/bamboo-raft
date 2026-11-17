package server_test

// Phase-exit coverage for the /api/bbraft/info detached capability. The
// extension document is optional-by-design: an old server omits
// commands.detached (or sends false), and only an explicit boolean true may
// select detached command execution. This test validates the published
// extension shape independently of the compiled-server test, which proves the
// real role wiring advertises true.

import (
	"encoding/json/v2"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// phaseExitInfoBody builds one /api/bbraft/info success body and applies an
// optional mutation to its commands object.
func phaseExitInfoBody(t *testing.T, mutate func(commands map[string]any)) []byte {
	t.Helper()
	response := api.NewInfoResponse("v1.2.3", api.InfoCommands{
		StreamMaxSeconds: 7200,
		SyncMaxSeconds:   api.DefaultSyncMaxSeconds,
	})
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal("marshal info response")
	}
	var members map[string]any
	if err := json.Unmarshal(raw, &members); err != nil {
		t.Fatal("decode info response")
	}
	commands, ok := members["commands"].(map[string]any)
	if !ok {
		t.Fatal("info response has no commands object")
	}
	if mutate != nil {
		mutate(commands)
	}
	body, err := json.Marshal(members)
	if err != nil {
		t.Fatal("marshal mutated info response")
	}
	return body
}

// TestPhaseExitInfoDetachedSpec proves the optional capability accepts an
// absent member, an explicit false and an explicit true, while every
// non-boolean JSON value is rejected instead of silently meaning legacy.
func TestPhaseExitInfoDetachedSpec(t *testing.T) {
	validator := compat.LoadFile(t, filepath.Join(compat.Root(t), "api", "bbraft-ext.yaml"))
	header := http.Header{"Content-Type": []string{"application/json"}}

	accepted := []struct {
		name    string
		mutate  func(commands map[string]any)
		want    any
		present bool
	}{
		{name: "absent", want: nil, present: false},
		{name: "explicit false", mutate: func(commands map[string]any) {
			commands["detached"] = false
		}, want: false, present: true},
		{name: "explicit true", mutate: func(commands map[string]any) {
			commands["detached"] = true
		}, want: true, present: true},
	}
	for _, test := range accepted {
		t.Run(test.name, func(t *testing.T) {
			body := phaseExitInfoBody(t, test.mutate)
			var decoded struct {
				Commands map[string]any `json:"commands"`
			}
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatal("decode info body")
			}
			got, present := decoded.Commands["detached"]
			if present != test.present || got != test.want {
				t.Fatalf("detached = %v (present %v), want %v (present %v)", got, present, test.want, test.present)
			}
			if err := validator.Response("bbraftInfo", http.StatusOK, header, body); err != nil {
				t.Errorf("info response failed the extension spec: %v", err)
			}
		})
	}

	rejected := []struct {
		name  string
		value any
	}{
		{name: "string", value: "true"},
		{name: "null", value: nil},
		{name: "number", value: 1},
		{name: "array", value: []any{}},
		{name: "object", value: map[string]any{}},
	}
	for _, test := range rejected {
		t.Run("reject "+test.name, func(t *testing.T) {
			body := phaseExitInfoBody(t, func(commands map[string]any) {
				commands["detached"] = test.value
			})
			if err := validator.Response("bbraftInfo", http.StatusOK, header, body); err == nil {
				t.Fatal("extension spec accepted a non-boolean detached member")
			}
		})
	}
}
