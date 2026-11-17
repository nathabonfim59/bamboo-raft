package api

import (
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func decodeJSONBody[T any](t *testing.T, body string) (T, error) {
	t.Helper()
	var value T
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	err := DecodeJSON(request, &value)
	return value, err
}

func TestDecodeJSONStrict(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		got, err := decodeJSONBody[struct {
			Name string `json:"name"`
		}](t, `{"name":"x"}`)
		if err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
		if got.Name != "x" {
			t.Errorf("name = %q", got.Name)
		}
	})

	tests := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"malformed", `{"name":`},
		{"unknown member", `{"name":"x","bogus":1}`},
		{"trailing value", `{"name":"x"} {"name":"y"}`},
		{"wrong kind", `[]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeJSONBody[struct {
				Name string `json:"name"`
			}](t, test.body)
			if err == nil {
				t.Fatal("DecodeJSON accepted an invalid body")
			}
			if _, ok := errors.AsType[*RequestError](err); !ok {
				t.Errorf("error = %T %v, want *RequestError", err, err)
			}
		})
	}

	t.Run("body cap", func(t *testing.T) {
		body := `{"name":"` + strings.Repeat("a", MaxJSONBodyBytes) + `"}`
		_, err := decodeJSONBody[struct {
			Name string `json:"name"`
		}](t, body)
		if err == nil {
			t.Fatal("DecodeJSON accepted a body over the cap")
		}
		if !strings.Contains(err.Error(), "exceeds") {
			t.Errorf("error = %v, want an exceeds message", err)
		}
	})
}

func TestDecodeJSONSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
		want error
	}{
		{"empty", "", ErrEmptyBody},
		{"syntax", `{"name":`, ErrInvalidJSON},
		{"semantic", `{"name":"x","bogus":1}`, ErrInvalidJSON},
		{"trailing", `{"name":"x"} {"name":"y"}`, ErrTrailingJSON},
		{"too large", `{"name":"` + strings.Repeat("a", MaxJSONBodyBytes) + `"}`, ErrBodyTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeJSONBody[struct {
				Name string `json:"name"`
			}](t, test.body)
			if err == nil {
				t.Fatal("DecodeJSON accepted an invalid body")
			}
			if !errors.Is(err, test.want) {
				t.Errorf("error = %v, want errors.Is(%v)", err, test.want)
			}
		})
	}
}

func TestDecodeJSONErrorsDoNotEchoValues(t *testing.T) {
	bodies := []string{
		`{"name":"top-secret-value","secret":1}`,
		`{"name":"top-secret-value",}`,
		`{"name":123}`,
		`{"name":"top-secret-value"} {"second":1}`,
	}
	for _, body := range bodies {
		_, err := decodeJSONBody[struct {
			Name string `json:"name"`
		}](t, body)
		if err == nil {
			t.Fatalf("DecodeJSON accepted %s", body)
		}
		text := err.Error()
		for _, leak := range []string{"top-secret-value", "json:", "within", "unable to unmarshal"} {
			if strings.Contains(text, leak) {
				t.Errorf("error %q leaks decoder text %q", text, leak)
			}
		}
	}
}

func TestTTLSeconds(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		set      bool
		disabled bool
		seconds  int
		hasValue bool
	}{
		{"absent", `{}`, false, false, 0, false},
		{"null", `{"ttlSeconds":null}`, true, true, 0, false},
		{"infinite", `{"ttlSeconds":"infinite"}`, true, true, 0, false},
		{"number", `{"ttlSeconds":60}`, true, false, 60, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := decodeJSONBody[CreateSandboxRequest](t, test.body)
			if err != nil {
				t.Fatalf("DecodeJSON: %v", err)
			}
			if request.TTLSeconds.Set() != test.set {
				t.Errorf("Set = %v, want %v", request.TTLSeconds.Set(), test.set)
			}
			if request.TTLSeconds.Disabled() != test.disabled {
				t.Errorf("Disabled = %v, want %v", request.TTLSeconds.Disabled(), test.disabled)
			}
			seconds, ok := request.TTLSeconds.Seconds()
			if ok != test.hasValue || seconds != test.seconds {
				t.Errorf("Seconds = %d,%v; want %d,%v", seconds, ok, test.seconds, test.hasValue)
			}
		})
	}

	for _, body := range []string{`{"ttlSeconds":"forever"}`, `{"ttlSeconds":true}`, `{"ttlSeconds":1.5}`} {
		if _, err := decodeJSONBody[CreateSandboxRequest](t, body); err == nil {
			t.Errorf("DecodeJSON accepted %s", body)
		}
	}
}

func TestTTLDefaults(t *testing.T) {
	request, err := decodeJSONBody[CreateSandboxRequest](t, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := request.TTL(); got == nil || *got != DefaultTTLSeconds {
		t.Errorf("TTL = %v, want %d", got, DefaultTTLSeconds)
	}
	if got := request.MachineType(); got != "default" {
		t.Errorf("MachineType = %q", got)
	}

	request, err = decodeJSONBody[CreateSandboxRequest](t, `{"ttlSeconds":null}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := request.TTL(); got != nil {
		t.Errorf("TTL = %v, want nil", got)
	}
}

func TestCreateSandboxRequestMarshalsTTL(t *testing.T) {
	absent := jsonMap(t, CreateSandboxRequest{})
	if _, ok := absent["ttlSeconds"]; ok {
		t.Error("absent ttlSeconds must be omitted")
	}

	disabled := jsonMap(t, CreateSandboxRequest{TTLSeconds: TTLSeconds{Present: true}})
	if value, ok := disabled["ttlSeconds"]; !ok || value != nil {
		t.Errorf("ttlSeconds = %v, present=%v; want explicit null", value, ok)
	}

	value := 60
	enabled := jsonMap(t, CreateSandboxRequest{TTLSeconds: TTLSeconds{Present: true, Value: &value}})
	if enabled["ttlSeconds"] != float64(60) {
		t.Errorf("ttlSeconds = %v", enabled["ttlSeconds"])
	}
}

func TestCreateSandboxRequestRepresentsFullPin(t *testing.T) {
	body := `{
		"type":"large",
		"ttlSeconds":7200,
		"labels":{"session":"abc"},
		"env":{"FOO":"bar"},
		"environment":"base",
		"noEnv":false,
		"snapshots":true,
		"monitoring":true,
		"failFast":false,
		"count":1,
		"setupScript":"true",
		"name":"job",
		"visibility":"private",
		"hostedPorts":[{"port":8080,"public":true,"title":"web"}],
		"files":[{"path":"/tmp/a","content":"x","encoding":"utf8"}],
		"org":"personal",
		"teamId":"team",
		"from":"snapshot"
	}`
	request, err := decodeJSONBody[CreateSandboxRequest](t, body)
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if request.MachineType() != "large" || *request.Name != "job" || *request.From != "snapshot" {
		t.Errorf("request = %+v", request)
	}
	if len(request.HostedPorts) != 1 || request.HostedPorts[0].Port != 8080 {
		t.Errorf("hostedPorts = %+v", request.HostedPorts)
	}
	if len(request.Files) != 1 || request.Files[0].Path != "/tmp/a" {
		t.Errorf("files = %+v", request.Files)
	}

	if _, err := decodeJSONBody[CreateSandboxRequest](t, `{"unknown":1}`); err == nil {
		t.Fatal("DecodeJSON accepted an unknown create field")
	}
}

func TestParseListSandboxesQueryDefaults(t *testing.T) {
	q, err := ParseListSandboxesQuery(url.Values{})
	if err != nil {
		t.Fatalf("ParseListSandboxesQuery: %v", err)
	}
	if q.Limit != DefaultListLimit || q.Sort != SortDescending || q.Cursor != nil {
		t.Errorf("defaults = %+v", q)
	}
	if q.States != nil || q.Name != nil || q.NamePrefix != nil || q.Labels != nil {
		t.Errorf("filters = %+v", q)
	}
}

func TestParseListSandboxesQueryFull(t *testing.T) {
	cursor := Cursor{
		Kind:      CursorKindSandboxes,
		Sort:      SortAscending,
		CreatedAt: time.Date(2026, 11, 8, 12, 0, 0, 123456000, time.UTC),
		ID:        "bx_23456789",
	}
	values := url.Values{
		"limit":      {"200"},
		"sort":       {"asc"},
		"cursor":     {cursor.Encode()},
		"state":      {"ready,idle"},
		"name":       {"job"},
		"namePrefix": {"job-"},
		"label":      {"session=abc", "env=prod"},
	}
	q, err := ParseListSandboxesQuery(values)
	if err != nil {
		t.Fatalf("ParseListSandboxesQuery: %v", err)
	}
	if q.Limit != 200 || q.Sort != SortAscending {
		t.Errorf("pagination = %+v", q)
	}
	if q.Cursor == nil || q.Cursor.ID != cursor.ID || !q.Cursor.CreatedAt.Equal(cursor.CreatedAt) {
		t.Errorf("cursor = %+v", q.Cursor)
	}
	if len(q.States) != 2 || q.States[0] != StateReady || q.States[1] != StateIdle {
		t.Errorf("states = %v", q.States)
	}
	if *q.Name != "job" || *q.NamePrefix != "job-" {
		t.Errorf("names = %v, %v", q.Name, q.NamePrefix)
	}
	wantLabels := []Label{{Key: "session", Value: "abc"}, {Key: "env", Value: "prod"}}
	if len(q.Labels) != 2 || q.Labels[0] != wantLabels[0] || q.Labels[1] != wantLabels[1] {
		t.Errorf("labels = %v", q.Labels)
	}
}

func TestParseListSandboxesQueryErrors(t *testing.T) {
	tests := []struct {
		name   string
		values url.Values
		param  string
	}{
		{"unknown", url.Values{"org": {"personal"}}, "org"},
		{"unsupported stoppedBefore", url.Values{"stoppedBefore": {"2026-11-08T12:00:00Z"}}, "stoppedBefore"},
		{"unsupported dataExpired", url.Values{"dataExpired": {"true"}}, "dataExpired"},
		{"unsupported include", url.Values{"include": {"cost"}}, "include"},
		{"name repeated", url.Values{"name": {"a", "b"}}, "name"},
		{"limit zero", url.Values{"limit": {"0"}}, "limit"},
		{"limit high", url.Values{"limit": {"201"}}, "limit"},
		{"limit text", url.Values{"limit": {"many"}}, "limit"},
		{"limit repeated", url.Values{"limit": {"1", "2"}}, "limit"},
		{"sort", url.Values{"sort": {"up"}}, "sort"},
		{"state empty entry", url.Values{"state": {"ready,"}}, "state"},
		{"label no equals", url.Values{"label": {"session"}}, "label"},
		{"label bad key", url.Values{"label": {"-bad=x"}}, "label"},
		{"label long value", url.Values{"label": {"k=" + strings.Repeat("v", 257)}}, "label"},
		{"cursor empty", url.Values{"cursor": {""}}, "cursor"},
		{"cursor garbage", url.Values{"cursor": {"not-a-cursor"}}, "cursor"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseListSandboxesQuery(test.values)
			if err == nil {
				t.Fatal("ParseListSandboxesQuery accepted invalid input")
			}
			queryErr, ok := errors.AsType[*QueryError](err)
			if !ok {
				t.Fatalf("error = %T %v, want *QueryError", err, err)
			}
			if queryErr.Param != test.param {
				t.Errorf("param = %q, want %q (%v)", queryErr.Param, test.param, err)
			}
		})
	}

	t.Run("cursor sort mismatch", func(t *testing.T) {
		cursor := Cursor{
			Kind:      CursorKindSandboxes,
			Sort:      SortAscending,
			CreatedAt: time.Now(),
			ID:        "bx_23456789",
		}
		_, err := ParseListSandboxesQuery(url.Values{"sort": {"desc"}, "cursor": {cursor.Encode()}})
		if err == nil || !strings.Contains(err.Error(), "sort") {
			t.Errorf("error = %v, want a sort mismatch", err)
		}
	})
}

func TestCursorRoundTrip(t *testing.T) {
	created := time.Date(2026, 11, 8, 12, 0, 0, 123456789, time.UTC)
	cursor := Cursor{Kind: CursorKindSandboxes, Sort: SortDescending, CreatedAt: created, ID: "bx_23456789"}
	raw := cursor.Encode()
	if raw != cursor.Encode() {
		t.Fatal("Encode is not deterministic")
	}
	decoded, err := DecodeCursor(CursorKindSandboxes, raw)
	if err != nil {
		t.Fatalf("DecodeCursor: %v", err)
	}
	if decoded.ID != cursor.ID || decoded.Sort != cursor.Sort || decoded.Kind != cursor.Kind {
		t.Errorf("decoded = %+v", decoded)
	}
	if !decoded.CreatedAt.Equal(created.Truncate(time.Microsecond)) {
		t.Errorf("createdAt = %v, want microsecond precision", decoded.CreatedAt)
	}

	if _, err := DecodeCursor(CursorKind("snapshots"), raw); err == nil {
		t.Error("DecodeCursor accepted a cursor from another collection")
	}
	if _, err := DecodeCursor(CursorKindSandboxes, raw+"x"); err == nil {
		t.Error("DecodeCursor accepted a tampered cursor")
	}
	if _, err := DecodeCursor(CursorKindSandboxes, ""); err == nil {
		t.Error("DecodeCursor accepted an empty cursor")
	}
	padded := base64.URLEncoding.EncodeToString([]byte(`{"v":1}`))
	if _, err := DecodeCursor(CursorKindSandboxes, padded); err == nil {
		t.Error("DecodeCursor accepted a padded cursor")
	}
}

func TestCursorEncodeRejectsInvalid(t *testing.T) {
	tests := []Cursor{
		{},
		{Kind: CursorKindSandboxes, Sort: SortAscending, ID: "bx_23456789"},
		{Kind: CursorKindSandboxes, Sort: SortAscending, CreatedAt: time.Now()},
		{Kind: CursorKindSandboxes, Sort: "sideways", CreatedAt: time.Now(), ID: "bx_23456789"},
	}
	for _, cursor := range tests {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Encode(%+v) did not panic", cursor)
				}
			}()
			cursor.Encode()
		}()
	}
}

func TestDecodeCursorZeroTimestamp(t *testing.T) {
	raw := base64.RawURLEncoding.EncodeToString([]byte(
		`{"v":1,"k":"sandboxes","s":"desc","t":"0001-01-01T00:00:00Z","i":"bx_23456789"}`,
	))
	cursor, err := DecodeCursor(CursorKindSandboxes, raw)
	if err == nil {
		t.Fatalf("DecodeCursor accepted a zero timestamp: %+v", cursor)
	}
}

func TestCursorRejectsBadPayloads(t *testing.T) {
	payloads := map[string]string{
		"unknown member": `{"v":1,"k":"sandboxes","s":"desc","t":"2026-11-08T12:00:00Z","i":"bx_23456789","extra":1}`,
		"version":        `{"v":2,"k":"sandboxes","s":"desc","t":"2026-11-08T12:00:00Z","i":"bx_23456789"}`,
		"kind":           `{"v":1,"k":"snapshots","s":"desc","t":"2026-11-08T12:00:00Z","i":"bx_23456789"}`,
		"sort":           `{"v":1,"k":"sandboxes","s":"sideways","t":"2026-11-08T12:00:00Z","i":"bx_23456789"}`,
		"empty id":       `{"v":1,"k":"sandboxes","s":"desc","t":"2026-11-08T12:00:00Z","i":""}`,
		"bad timestamp":  `{"v":1,"k":"sandboxes","s":"desc","t":"yesterday","i":"bx_23456789"}`,
	}
	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			raw := base64.RawURLEncoding.EncodeToString([]byte(payload))
			if _, err := DecodeCursor(CursorKindSandboxes, raw); err == nil {
				t.Error("DecodeCursor accepted an invalid payload")
			}
		})
	}
}

func TestReadIdempotencyKey(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	key, err := ReadIdempotencyKey(request)
	if err != nil || key != "" {
		t.Errorf("absent key = %q, %v", key, err)
	}

	request.Header.Set(IdempotencyKeyHeader, strings.Repeat("k", 255))
	key, err = ReadIdempotencyKey(request)
	if err != nil || len(key) != 255 {
		t.Errorf("255-char key = %d, %v", len(key), err)
	}

	request.Header.Set(IdempotencyKeyHeader, strings.Repeat("k", 256))
	if _, err := ReadIdempotencyKey(request); err == nil {
		t.Error("ReadIdempotencyKey accepted a 256-char key")
	}
}

func TestCommandRequestShape(t *testing.T) {
	request, err := decodeJSONBody[CommandRequest](t, `{"command":"make check","cwd":"/src","timeoutSeconds":7200,"stream":true}`)
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if request.Command != "make check" || *request.Cwd != "/src" || *request.TimeoutSeconds != 7200 || !*request.Stream {
		t.Errorf("request = %+v", request)
	}
	if request.Detached != nil {
		t.Errorf("detached = %v, want nil", request.Detached)
	}
}

func TestParseListSavedSnapshotsQuery(t *testing.T) {
	q, err := ParseListSavedSnapshotsQuery(url.Values{})
	if err != nil || q.Name != nil || q.NamePrefix != nil {
		t.Fatalf("defaults = %+v, %v", q, err)
	}
	q, err = ParseListSavedSnapshotsQuery(url.Values{"name": {"base"}, "namePrefix": {"ci-"}})
	if err != nil {
		t.Fatalf("ParseListSavedSnapshotsQuery: %v", err)
	}
	if *q.Name != "base" || *q.NamePrefix != "ci-" {
		t.Errorf("query = %+v", q)
	}
	for _, values := range []url.Values{
		{"limit": {"10"}},
		{"name": {"a", "b"}},
		{"namePrefix": {"a", "b"}},
	} {
		if _, err := ParseListSavedSnapshotsQuery(values); err == nil {
			t.Errorf("ParseListSavedSnapshotsQuery accepted %v", values)
		}
	}
}

func TestSavedSnapshotAndFileRequests(t *testing.T) {
	save, err := decodeJSONBody[SavedSnapshotSaveRequest](t, `{"sandboxId":"bx_23456789","name":"base"}`)
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if save.SandboxID != "bx_23456789" || save.Name != "base" {
		t.Errorf("save = %+v", save)
	}

	file, err := decodeJSONBody[FileURLRequest](t, `{"path":"/tmp/archive.tar","ttlSeconds":900}`)
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if file.Path != "/tmp/archive.tar" || *file.TTLSeconds != 900 {
		t.Errorf("file = %+v", file)
	}
}

func TestPageInfoJSONShape(t *testing.T) {
	raw, err := json.Marshal(NewPageInfo(nil, 100))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"nextCursor":null,"hasMore":false,"limit":100}` {
		t.Errorf("page info = %s", raw)
	}
}
