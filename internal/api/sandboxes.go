package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// coreMaxTTLSeconds is the pinned ttlSeconds maximum (30 days).
const coreMaxTTLSeconds = 2592000

// coreMaxEnvBytes is the pinned 64 KiB aggregate env limit.
const coreMaxEnvBytes = 64 << 10

// coreMaxLabels is the pinned label count.
const coreMaxLabels = 20

// coreMaxEnvVars is the pinned environment-variable count.
const coreMaxEnvVars = 100

// coreMaxNameRunes is the pinned display-name length.
const coreMaxNameRunes = 120

// coreEnvKeyPattern is the pinned env key syntax.
var coreEnvKeyPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

// coreReservedEnvName reports the pinned names a create must not override.
func coreReservedEnvName(name string) bool {
	switch name {
	case "ASCII_TOKEN", "ASCII_API_URL", "AGENT_ID", "PRODUCT_MODE",
		"ENVIRONMENT_ID", "SERVICE_PREVIEW_TOKEN":
		return true
	default:
		return false
	}
}

// me serves GET /api/v1/me. Only the authenticated account id is available in
// Phase 0.5; no login, email or plan is invented.
func (h *coreHandlers) me(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	apierr.Write(w, http.StatusOK, TypeUserInfo, NewMeResponse(MeUser{ID: owner.String()}))
}

// listSandboxes serves GET /api/v1/sandboxes with the accepted strict query
// parser and owner-scoped keyset pagination.
func (h *coreHandlers) listSandboxes(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	values, apiErr := coreParseQuery(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	query, err := ParseListSandboxesQuery(values)
	if err != nil {
		apierr.WriteError(w, r, queryFailure(err))
		return
	}
	states, apiErr := coreDBStates(query.States)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	labels, contradictory, apiErr := coreLabelMap(query.Labels)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if contradictory {
		// Two required values for one key can never both match. This is a
		// valid AND filter, so answer an empty page without touching SQL.
		page := NewPageInfo(nil, query.Limit)
		apierr.Write(w, http.StatusOK, TypeSandboxList, NewSandboxListResponse([]Sandbox{}, page))
		return
	}
	dbQuery := db.SandboxListQuery{
		Owner:      owner,
		States:     states,
		Name:       query.Name,
		NamePrefix: "",
		Labels:     labels,
		Limit:      query.Limit,
		Ascending:  query.Sort == SortAscending,
	}
	if query.NamePrefix != nil {
		dbQuery.NamePrefix = *query.NamePrefix
	}
	if query.Cursor != nil {
		dbQuery.After = &db.PageCursor{
			CreatedAt: query.Cursor.CreatedAt,
			ID:        query.Cursor.ID,
		}
	}

	rows, next, err := h.store.ListSandboxes(r.Context(), dbQuery)
	if err != nil {
		if apiErr := h.storeFailure(r.Context(), "list sandboxes", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	sandboxes := make([]Sandbox, len(rows))
	for i, row := range rows {
		view, apiErr := h.coreSandboxView(r.Context(), owner, row)
		if apiErr != nil {
			apierr.WriteError(w, r, apiErr)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		sandboxes[i] = view
	}
	var nextCursor *string
	if next != nil {
		if next.ID == "" || next.CreatedAt.IsZero() {
			if apiErr := h.storeFailure(r.Context(), "list sandboxes cursor", errors.New("store returned an empty next cursor")); apiErr != nil {
				apierr.WriteError(w, r, apiErr)
			}
			return
		}
		cursor := Cursor{
			Kind:      CursorKindSandboxes,
			Sort:      query.Sort,
			CreatedAt: next.CreatedAt,
			ID:        next.ID,
		}.Encode()
		nextCursor = &cursor
	}
	page := NewPageInfo(nextCursor, query.Limit)
	apierr.Write(w, http.StatusOK, TypeSandboxList, NewSandboxListResponse(sandboxes, page))
}

// queryFailure maps the accepted parser's error to the pinned invalid_query
// answer. Parser reasons are fixed text; no query value is echoed.
func queryFailure(err error) *apierr.Error {
	if queryErr, ok := errors.AsType[*QueryError](err); ok {
		return coreInvalidQuery(queryErr.Param, queryErr.Reason)
	}
	return coreInvalidQuery("query", "invalid query parameters")
}

// coreDBStates validates the requested state names against the pinned enum
// before the query reaches SQL.
func coreDBStates(states []SandboxState) ([]db.SandboxState, *apierr.Error) {
	if len(states) == 0 {
		return nil, nil
	}
	out := make([]db.SandboxState, len(states))
	for i, state := range states {
		if !coreValidState(state) {
			return nil, coreInvalidQuery("state", "unknown sandbox state")
		}
		out[i] = db.SandboxState(state)
	}
	return out, nil
}

// coreValidState reports whether state is one of the pinned sandbox states.
func coreValidState(state SandboxState) bool {
	switch state {
	case StateInit, StateProvisioning, StateProvisioned, StateCloning,
		StateReady, StateIdle, StateRunning, StateArchiving, StateArchived,
		StateError, StateCancelled:
		return true
	default:
		return false
	}
}

// coreLabelMap converts repeated key=value filters into the AND map the store
// expects. Repeating a key with a different value is a valid filter that can
// never match; contradictory reports it so the handler can answer an empty
// page without SQL, because a map cannot express both values.
func coreLabelMap(labels []Label) (values map[string]string, contradictory bool, apiErr *apierr.Error) {
	if len(labels) == 0 {
		return nil, false, nil
	}
	out := make(map[string]string, len(labels))
	for _, label := range labels {
		if existing, ok := out[label.Key]; ok && existing != label.Value {
			return nil, true, nil
		}
		out[label.Key] = label.Value
	}
	return out, false, nil
}

// getSandbox serves GET /api/v1/sandboxes/{sandboxId}. Foreign and missing ids
// are the same 404 and the response never carries env.
func (h *coreHandlers) getSandbox(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	row, err := h.store.Sandbox(r.Context(), owner, sandboxID)
	if err != nil {
		if apiErr := h.storeFailure(r.Context(), "get sandbox", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	sandbox, apiErr := h.coreSandboxView(r.Context(), owner, row)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if r.Context().Err() != nil {
		return
	}
	apierr.Write(w, http.StatusOK, TypeSandboxInfo, NewSandboxInfoResponse(sandbox))
}

// createSandbox serves POST /api/v1/sandboxes. It validates the pinned shape,
// then delegates placement and the durable create to admission.
func (h *coreHandlers) createSandbox(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	values, apiErr := coreParseQuery(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if apiErr := coreRejectUnknownQuery(values); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	input, apiErr := h.parseCreateSandbox(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	result, err := h.admission.Admit(r.Context(), AdmissionRequest{
		Owner:          owner,
		SandboxID:      coreNewSandboxID(),
		MachineType:    input.machine.Name,
		Resources:      input.resources(),
		From:           input.from,
		TypeExplicit:   input.typeExplicit,
		Name:           input.displayName(h.now()),
		Labels:         input.labels,
		Env:            input.env,
		TTLSeconds:     input.ttl,
		FailFast:       input.failFast,
		IdempotencyKey: input.idempotencyKey,
		RequestSHA256:  input.requestSHA256,
	})
	if err != nil {
		if apiErr := h.admissionFailure(r.Context(), input.failFast, err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if result.Sandbox.ID == "" || result.Sandbox.OwnerID != owner {
		if apiErr := h.storeFailure(r.Context(), "create admission result", errors.New("admission returned a malformed or foreign sandbox")); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	response := NewCreateSandboxResponse(createStatus(result), configuredTTL(result.Sandbox), sandboxFromRow(result.Sandbox))
	apierr.Write(w, http.StatusAccepted, TypeSandboxCreated, response)
}

// deleteSandbox serves DELETE /api/v1/sandboxes/{sandboxId}. The exact
// confirmation header is mandatory; acceptance is idempotent.
func (h *coreHandlers) deleteSandbox(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	if err := RequireConfirmDelete(r, sandboxID); err != nil {
		message := "deletion confirmation is required"
		if confirmErr, ok := errors.AsType[*ConfirmDeleteError](err); ok && confirmErr.Reason == ConfirmDeleteMismatch {
			message = "deletion confirmation does not match the sandbox id"
		}
		apierr.WriteError(w, r, coreConflict(message))
		return
	}
	operation, _, err := h.store.AcceptSandboxDeletion(r.Context(), owner, sandboxID, db.DeletionExplicit)
	if err != nil {
		if apiErr := h.storeFailure(r.Context(), "accept sandbox deletion", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	apierr.Write(w, http.StatusAccepted, TypeSandboxDeleting, NewSandboxDeletingResponse(deletionOperationFromRow(operation)))
}

// getDeletionOperation serves GET /api/v1/deletion-operations/{operationId}.
// It is owner-scoped and never cacheable.
func (h *coreHandlers) getDeletionOperation(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	operationID := r.PathValue(ParamOperationID)
	if operationID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	operation, err := h.store.DeletionOperation(r.Context(), owner, operationID)
	if err != nil {
		if apiErr := h.storeFailure(r.Context(), "get deletion operation", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	apierr.Write(w, http.StatusOK, TypeDeletionOperation, NewDeletionOperationResponse(deletionOperationFromRow(operation)))
}

// coreCreateInput is the validated create request. It holds no raw body text
// and no plaintext env outside the maps admission needs.
type coreCreateInput struct {
	machine        machinetype.Type
	from           string
	typeExplicit   bool
	ttl            *int
	name           string
	nameSet        bool
	labels         map[string]string
	env            map[string]string
	failFast       bool
	idempotencyKey string
	requestSHA256  [sha256.Size]byte
}

// coreDefaultNameLayout is the pinned display-name form: "Sandbox" plus the
// UTC creation minute, for example "Sandbox 2026-05-31 12:00".
const coreDefaultNameLayout = "2006-01-02 15:04"

// displayName returns the explicit name, including an explicit empty one, or
// the generated default when the request omitted name.
func (i coreCreateInput) displayName(now time.Time) string {
	if i.nameSet {
		return i.name
	}
	return "Sandbox " + now.UTC().Format(coreDefaultNameLayout)
}

// resources is the requested machine's canonical tuple. A restore whose type
// was omitted has no machine yet; admission fills it from the saved source.
func (i coreCreateInput) resources() db.Resources {
	if i.machine.Name == "" {
		return db.Resources{}
	}
	return db.Resources{
		VCPUs:     i.machine.VCPUs,
		MemoryMiB: i.machine.MemoryMiB,
		DiskGiB:   i.machine.DiskGiB,
	}
}

// coreCreateSyntax mirrors every pinned create member so omission can be told
// apart from an explicit null before the typed decode loses that difference.
type coreCreateSyntax struct {
	Type        jsontext.Value `json:"type,omitzero"`
	TTLSeconds  jsontext.Value `json:"ttlSeconds,omitzero"`
	Labels      jsontext.Value `json:"labels,omitzero"`
	Env         jsontext.Value `json:"env,omitzero"`
	Environment jsontext.Value `json:"environment,omitzero"`
	NoEnv       jsontext.Value `json:"noEnv,omitzero"`
	Snapshots   jsontext.Value `json:"snapshots,omitzero"`
	Monitoring  jsontext.Value `json:"monitoring,omitzero"`
	FailFast    jsontext.Value `json:"failFast,omitzero"`
	Count       jsontext.Value `json:"count,omitzero"`
	SetupScript jsontext.Value `json:"setupScript,omitzero"`
	Name        jsontext.Value `json:"name,omitzero"`
	Visibility  jsontext.Value `json:"visibility,omitzero"`
	HostedPorts jsontext.Value `json:"hostedPorts,omitzero"`
	Files       jsontext.Value `json:"files,omitzero"`
	Org         jsontext.Value `json:"org,omitzero"`
	TeamID      jsontext.Value `json:"teamId,omitzero"`
	From        jsontext.Value `json:"from,omitzero"`
}

// parseCreateSandbox reads and validates one create. The idempotency hash is
// computed over the exact request bytes; an omitted body is the default body
// and hashes as {}.
func (h *coreHandlers) parseCreateSandbox(r *http.Request) (coreCreateInput, *apierr.Error) {
	raw, present, readErr := coreReadCreateBody(r)
	if readErr != nil {
		return coreCreateInput{}, coreInvalidJSON("request body is not valid JSON")
	}
	var request CreateSandboxRequest
	hash := sha256.Sum256([]byte("{}"))
	if present {
		if apiErr := coreRequireJSONMedia(r); apiErr != nil {
			return coreCreateInput{}, apiErr
		}
		if apiErr := coreValidateCreateSyntax(raw); apiErr != nil {
			return coreCreateInput{}, apiErr
		}
		if err := json.Unmarshal(raw, &request, json.RejectUnknownMembers(true)); err != nil {
			return coreCreateInput{}, coreInvalidJSON("request body is not valid JSON")
		}
		hash = sha256.Sum256(raw)
	}

	from := ""
	if request.From != nil {
		from = *request.From
	}
	typeExplicit := request.Type != nil
	machineName := machinetype.Default
	if typeExplicit {
		if *request.Type == "" {
			return coreCreateInput{}, coreInvalidJSON("type must be a supported machine type")
		}
		machineName = *request.Type
	} else if from != "" {
		// A restore without a type inherits the saved snapshot's canonical
		// type. Admission resolves it from the immutable source row; the
		// API must not invent a default for an unknown legacy type.
		machineName = ""
	}
	var machine machinetype.Type
	if machineName != "" {
		looked, ok := machinetype.Lookup(machineName)
		if !ok {
			return coreCreateInput{}, coreInvalidJSON("type must be a supported machine type")
		}
		machine = looked
	}
	ttl := request.TTL()
	if ttl != nil && (*ttl < 1 || *ttl > coreMaxTTLSeconds) {
		return coreCreateInput{}, coreInvalidJSON("ttlSeconds must be an integer from 1 to 2592000, null or infinite")
	}
	name := ""
	nameSet := request.Name != nil
	if nameSet {
		name = *request.Name
		if utf8.RuneCountInString(name) > coreMaxNameRunes {
			return coreCreateInput{}, coreInvalidName("name must be at most 120 characters")
		}
	}
	if apiErr := coreValidateLabels(request.Labels); apiErr != nil {
		return coreCreateInput{}, apiErr
	}
	if apiErr := coreValidateEnv(request.Env); apiErr != nil {
		return coreCreateInput{}, apiErr
	}
	if apiErr := coreValidateUnsupportedCreate(request); apiErr != nil {
		return coreCreateInput{}, apiErr
	}
	key, keyErr := ReadIdempotencyKey(r)
	if keyErr != nil {
		return coreCreateInput{}, coreInvalidJSON("Idempotency-Key must be at most 255 characters")
	}
	return coreCreateInput{
		machine:        machine,
		from:           from,
		typeExplicit:   typeExplicit,
		ttl:            ttl,
		name:           name,
		nameSet:        nameSet,
		labels:         request.Labels,
		env:            request.Env,
		failFast:       request.FailFast != nil && *request.FailFast,
		idempotencyKey: key,
		requestSHA256:  hash,
	}, nil
}

// coreReadCreateBody reads at most MaxJSONBodyBytes+1 bytes and reports
// whether the client sent a body at all. An empty body is the pinned default
// (no members), not invalid JSON.
func coreReadCreateBody(r *http.Request) ([]byte, bool, error) {
	if r.Body == nil {
		return nil, false, nil
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, MaxJSONBodyBytes+1))
	if err != nil {
		return nil, false, err
	}
	if len(raw) > MaxJSONBodyBytes {
		return nil, false, ErrBodyTooLarge
	}
	if len(raw) == 0 {
		return nil, false, nil
	}
	return raw, true, nil
}

// coreRequireJSONMedia rejects a declared non-JSON create body. A missing
// Content-Type is accepted because the body is still decoded strictly.
func coreRequireJSONMedia(r *http.Request) *apierr.Error {
	raw := r.Header.Get("Content-Type")
	if raw == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return coreInvalidJSON("Content-Type must be application/json")
	}
	return nil
}

// coreValidateCreateSyntax enforces the pinned member kinds and nullability
// before the typed decode. The pinned object has no nullable member except
// ttlSeconds, so an explicit null anywhere else is invalid_json.
func coreValidateCreateSyntax(raw []byte) *apierr.Error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return coreInvalidJSON("request body must be a JSON object")
	}
	var syntax coreCreateSyntax
	if err := json.Unmarshal(raw, &syntax, json.RejectUnknownMembers(true)); err != nil {
		return coreInvalidJSON("request body is not valid JSON")
	}
	checks := []struct {
		field    string
		value    jsontext.Value
		ok       func(jsontext.Value) bool
		expected string
	}{
		{"type", syntax.Type, coreJSONString, "a string"},
		{"environment", syntax.Environment, coreJSONString, "a string"},
		{"noEnv", syntax.NoEnv, coreJSONBool, "a boolean"},
		{"snapshots", syntax.Snapshots, coreJSONBool, "a boolean"},
		{"monitoring", syntax.Monitoring, coreJSONBool, "a boolean"},
		{"failFast", syntax.FailFast, coreJSONBool, "a boolean"},
		{"count", syntax.Count, coreJSONInteger, "an integer"},
		{"setupScript", syntax.SetupScript, coreJSONString, "a string"},
		{"name", syntax.Name, coreJSONString, "a string"},
		{"visibility", syntax.Visibility, coreJSONString, "a string"},
		{"org", syntax.Org, coreJSONString, "a string"},
		{"teamId", syntax.TeamID, coreJSONString, "a string"},
		{"from", syntax.From, coreJSONString, "a string"},
		{"hostedPorts", syntax.HostedPorts, coreJSONArray, "an array"},
		{"files", syntax.Files, coreJSONArray, "an array"},
	}
	for _, check := range checks {
		if coreJSONAbsent(check.value) {
			continue
		}
		if !check.ok(check.value) {
			return coreInvalidJSON(check.field + " must be " + check.expected)
		}
	}
	if !coreJSONAbsent(syntax.TTLSeconds) {
		value := coreJSONTrim(syntax.TTLSeconds)
		if !bytes.Equal(value, []byte("null")) && !coreJSONString(value) && !coreJSONInteger(value) {
			return coreInvalidJSON(`ttlSeconds must be an integer, null or "infinite"`)
		}
	}
	if !coreJSONAbsent(syntax.Labels) {
		if !coreJSONObject(syntax.Labels) || !coreStringMapShape(syntax.Labels) {
			return coreInvalidJSON("labels must be an object of string values")
		}
	}
	if !coreJSONAbsent(syntax.Env) {
		if !coreJSONObject(syntax.Env) {
			return coreInvalidJSON("env must be an object of string values")
		}
		if !coreStringMapShape(syntax.Env) {
			return coreInvalidEnv("env values must be strings")
		}
	}
	return nil
}

// coreStringMapShape reports whether every value of a JSON object is a string.
func coreStringMapShape(raw jsontext.Value) bool {
	var entries map[string]jsontext.Value
	if err := json.Unmarshal(raw, &entries); err != nil {
		return false
	}
	for _, entry := range entries {
		if !coreJSONString(entry) {
			return false
		}
	}
	return true
}

// coreJSONTrim returns the raw member without surrounding whitespace.
func coreJSONTrim(raw jsontext.Value) []byte {
	return bytes.TrimSpace(raw)
}

// coreJSONAbsent reports whether the member was omitted.
func coreJSONAbsent(raw jsontext.Value) bool {
	return len(coreJSONTrim(raw)) == 0
}

// coreJSONString reports whether raw is a JSON string.
func coreJSONString(raw jsontext.Value) bool {
	value := coreJSONTrim(raw)
	return len(value) > 0 && value[0] == '"'
}

// coreJSONBool reports whether raw is a JSON boolean.
func coreJSONBool(raw jsontext.Value) bool {
	value := coreJSONTrim(raw)
	return bytes.Equal(value, []byte("true")) || bytes.Equal(value, []byte("false"))
}

// coreJSONObject reports whether raw is a JSON object.
func coreJSONObject(raw jsontext.Value) bool {
	value := coreJSONTrim(raw)
	return len(value) > 0 && value[0] == '{'
}

// coreJSONArray reports whether raw is a JSON array.
func coreJSONArray(raw jsontext.Value) bool {
	value := coreJSONTrim(raw)
	return len(value) > 0 && value[0] == '['
}

// coreJSONInteger reports whether raw is a JSON integer without an exponent
// or fraction.
func coreJSONInteger(raw jsontext.Value) bool {
	value := coreJSONTrim(raw)
	if len(value) == 0 || bytes.ContainsAny(value, ".eE") {
		return false
	}
	_, err := strconv.ParseInt(string(value), 10, 64)
	return err == nil
}

// coreValidateLabels enforces the pinned count, key syntax and value length.
func coreValidateLabels(labels map[string]string) *apierr.Error {
	if len(labels) > coreMaxLabels {
		return coreInvalidJSON("labels must contain at most 20 entries")
	}
	for key, value := range labels {
		if !labelKeyPattern.MatchString(key) {
			return coreInvalidJSON("labels contain an invalid key")
		}
		if len(value) > 256 {
			return coreInvalidJSON("label values must be at most 256 bytes")
		}
	}
	return nil
}

// coreValidateEnv enforces the pinned count, key syntax, reserved names,
// aggregate size and NUL rules. Messages never echo a key or value.
func coreValidateEnv(env map[string]string) *apierr.Error {
	if len(env) > coreMaxEnvVars {
		return coreInvalidEnv("env must contain at most 100 variables")
	}
	total := 0
	for key, value := range env {
		if !coreEnvKeyPattern.MatchString(key) || strings.IndexByte(key, 0) >= 0 {
			return coreInvalidEnv("env contains an invalid variable name")
		}
		if strings.IndexByte(value, 0) >= 0 {
			return coreInvalidEnv("env values must not contain NUL bytes")
		}
		if coreReservedEnvName(key) {
			return coreInvalidEnv("env contains a reserved variable name")
		}
		total += len(key) + len(value)
		if total > coreMaxEnvBytes {
			return coreInvalidEnv("env must be at most 64 KiB")
		}
	}
	return nil
}

// coreValidateUnsupportedCreate refuses every pinned member whose behavior
// Phase 0.5 cannot durably provide. Explicit snapshots and monitoring are
// rejected even when false because both set a durable per-sandbox policy; an
// explicit count is rejected because no batch/from create exists. noEnv is
// effective as-is: Phase 0.5 never injects account or platform credentials.
// Empty scripts and arrays request nothing extra and are accepted.
func coreValidateUnsupportedCreate(request CreateSandboxRequest) *apierr.Error {
	if request.Environment != nil && *request.Environment != "base" {
		return coreUnknownEnvironment()
	}
	if request.Snapshots != nil {
		return coreInvalidJSON("snapshots is not supported in Phase 0.5")
	}
	if request.Monitoring != nil {
		return coreInvalidJSON("monitoring is not supported in Phase 0.5")
	}
	if request.Count != nil {
		return coreInvalidJSON("count is not supported in Phase 0.5")
	}
	if request.SetupScript != nil && *request.SetupScript != "" {
		return coreInvalidJSON("setupScript is not supported in Phase 0.5")
	}
	if request.Visibility != nil {
		return coreInvalidJSON("visibility is not supported in Phase 0.5")
	}
	if len(request.HostedPorts) > 0 {
		return coreInvalidJSON("hostedPorts is not supported in Phase 0.5")
	}
	if len(request.Files) > 0 {
		return coreInvalidJSON("files is not supported in Phase 0.5")
	}
	if request.Org != nil {
		return coreInvalidJSON("org is not supported in Phase 0.5")
	}
	if request.TeamID != nil {
		return coreInvalidJSON("teamId is not supported in Phase 0.5")
	}
	return nil
}

// coreNewSandboxID mints the fresh id admission receives. A replay ignores it
// and returns the id stored under the idempotency key.
func coreNewSandboxID() string {
	return ids.Sandbox()
}
