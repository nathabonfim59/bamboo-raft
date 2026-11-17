// Package compat is the test support that validates bamboo-raft's actual
// /api/v1 responses and NDJSON frames against the committed OpenAPI document
// in api/bbraft-v1.yaml (and the extension document in api/bbraft-ext.yaml).
// It is imported only by tests.
package compat

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
)

// SpecPath is the repository path of the /api/v1 OpenAPI document.
const SpecPath = "api/bbraft-v1.yaml"

// NDJSONMediaType is the media type of streamed command frames. kin-openapi
// has no decoder for it, so we register one that decodes a single JSON value.
// NDJSON validates one line at a time through Response.
const NDJSONMediaType = "application/x-ndjson"

func init() {
	openapi3filter.RegisterBodyDecoder(NDJSONMediaType, decodeNDJSONFrame)
}

func decodeNDJSONFrame(r io.Reader, _ http.Header, _ *openapi3.SchemaRef, _ openapi3filter.EncodingFn) (any, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read NDJSON frame: %w", err)
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("decode NDJSON frame: %w", err)
	}
	return value, nil
}

// Operation identifies one operation of a loaded document.
type Operation struct {
	ID     string
	Method string
	Path   string
}

type indexedOperation struct {
	Operation
	item *openapi3.PathItem
	op   *openapi3.Operation
}

// Validator validates recorded HTTP responses against one OpenAPI document.
type Validator struct {
	ctx  context.Context
	doc  *openapi3.T
	ops  map[string]indexedOperation
	path string
}

// Root returns the repository root that contains go.mod and
// api/bbraft-v1.yaml.
func Root(t testing.TB) string {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("compat: %v", err)
	}
	return root
}

// Load opens the /api/v1 document (api/bbraft-v1.yaml) and returns a
// validator.
func Load(t testing.TB) *Validator {
	t.Helper()
	return LoadFile(t, filepath.Join(Root(t), SpecPath))
}

// LoadFile opens an OpenAPI document, such as api/bbraft-ext.yaml.
func LoadFile(t testing.TB, path string) *Validator {
	t.Helper()
	validator, err := load(t.Context(), path)
	if err != nil {
		t.Fatalf("compat: %v", err)
	}
	return validator
}

func load(ctx context.Context, path string) (*Validator, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	doc, err := loader.LoadFromData(data)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", path, err)
	}
	if err := doc.Validate(ctx); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	validator := &Validator{ctx: ctx, doc: doc, ops: make(map[string]indexedOperation), path: path}
	for pathTemplate, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			if op.OperationID == "" {
				continue
			}
			if _, exists := validator.ops[op.OperationID]; exists {
				return nil, fmt.Errorf("duplicate operationId %q in %s", op.OperationID, path)
			}
			validator.ops[op.OperationID] = indexedOperation{
				Operation: Operation{ID: op.OperationID, Method: strings.ToUpper(method), Path: pathTemplate},
				item:      item,
				op:        op,
			}
		}
	}
	return validator, nil
}

// OpenAPIVersion returns the document's openapi field.
func (v *Validator) OpenAPIVersion() string { return v.doc.OpenAPI }

// Operations lists the document's operations sorted by path and method.
func (v *Validator) Operations() []Operation {
	operations := make([]Operation, 0, len(v.ops))
	for _, op := range v.ops {
		operations = append(operations, op.Operation)
	}
	slices.SortFunc(operations, func(a, b Operation) int {
		return cmp.Or(cmp.Compare(a.Path, b.Path), cmp.Compare(a.Method, b.Method))
	})
	return operations
}

// Operation returns the method and path of operationID.
func (v *Validator) Operation(operationID string) (Operation, bool) {
	op, ok := v.ops[operationID]
	return op.Operation, ok
}

// HasOperation reports whether the document contains method and path. path may
// include the server base path (/api/v1), so route constants compare directly
// with OpenAPI path keys.
func (v *Validator) HasOperation(method, path string) bool {
	method = strings.ToUpper(method)
	path = v.relativePath(path)
	for _, op := range v.ops {
		if op.Method == method && op.Path == path {
			return true
		}
	}
	return false
}

// relativePath strips the document's server base path, if present.
func (v *Validator) relativePath(path string) string {
	for _, server := range v.doc.Servers {
		parsed, err := url.Parse(server.URL)
		if err != nil || parsed.Path == "" || parsed.Path == "/" {
			continue
		}
		if stripped, ok := strings.CutPrefix(path, parsed.Path); ok {
			return stripped
		}
	}
	return path
}

// undocumentedError is one approved exception: the exact wire codes accepted
// for a status the pinned operation response table omits, plus any extra
// envelope rule.
type undocumentedError struct {
	Codes []string
	// RequireRetryAfterSeconds requires error.details.retryAfterSeconds
	// to be a positive integer, the API shape for a retryable 503.
	RequireRetryAfterSeconds bool
}

// undocumentedErrorResponses is the narrow, explicit error-code matrix for
// error statuses whose codes the document's generic ErrorEnvelope does not
// pin down. It is keyed by operationId and HTTP status. When an entry exists
// it is enforced on top of the document's schema; a status the document omits
// is accepted only through an entry. There is no default fallback: a status
// or code outside this matrix fails, and so does every undocumented success.
//
// The standing internal-fault pair 500/http_500 is approved for every
// implemented operation but must be listed on each operation explicitly;
// there is no broad default. The command 502 wire code is
// sandbox_direct_failed; any other spelling is rejected.
var undocumentedErrorResponses = map[string]map[int]undocumentedError{
	"me": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"sandboxes": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"create": {
		http.StatusBadRequest: {Codes: []string{
			"invalid_json",
			"invalid_env",
			"invalid_name",
			"invalid_query",
			"unknown_environment",
		}},
		// Approved by the orchestrator: a create-from whose saved snapshot
		// is missing, foreign, not ready or has an unknown legacy disk.
		http.StatusNotFound:            {Codes: []string{"snapshot_not_found"}},
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"get": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"update": {
		// The pinned update table has 400/401/404 only; the approved
		// internal fault is 500/http_500.
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"stop": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"getStop": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"resume": {
		// The pinned resume table omits 400 although the pin's shared
		// error model defines 400 invalid_json, invalid_env and
		// unknown_environment (api_v1.md). The orchestrator approved
		// exactly these three codes; every other 400 code stays
		// rejected.
		http.StatusBadRequest: {Codes: []string{
			"invalid_json",
			"invalid_env",
			"unknown_environment",
		}},
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"deleteSandbox": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"getDeletionOperation": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"command": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
		http.StatusBadGateway:          {Codes: []string{"sandbox_direct_failed"}},
		http.StatusServiceUnavailable: {
			Codes:                    []string{"out_of_capacity"},
			RequireRetryAfterSeconds: true,
		},
	},
	"commandStatus": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
		// RESOURCE_EXHAUSTED is a transport or window-size failure, so
		// this operation answers the same approved 502 and never a 503.
		http.StatusBadGateway: {Codes: []string{"sandbox_direct_failed"}},
	},
	"attachCommand": {
		// The pinned attach table lists only 200/400/401/404/409. A
		// dial, transport or malformed-frame failure before the headers
		// is the approved direct-failure 502; after the headers it is an
		// error frame, never a second HTTP status. RESOURCE_EXHAUSTED is
		// not VM capacity here, so it never becomes a 503.
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
		http.StatusBadGateway:          {Codes: []string{"sandbox_direct_failed"}},
	},
	"stopCommand": {
		// The pinned stop table lists only 200/400/401/404/409. Record
		// reads and the post-kill observation can fault without a node
		// status, and a node transport failure is the direct-failure 502.
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
		http.StatusBadGateway:          {Codes: []string{"sandbox_direct_failed"}},
	},
	// The eight core file operations share one approved response table. The
	// pinned tables list only 200/400 (plus 413 for reads and writes); the
	// shared Unauthorized/NotFound/Conflict responses and the standing
	// internal-fault and direct-failure policies supply the rest.
	"readFile":           fileErrorResponses,
	"writeFile":          fileErrorResponses,
	"deleteFile":         fileErrorResponses,
	"listFiles":          fileErrorResponses,
	"getFileInfo":        fileErrorResponses,
	"createFolder":       fileErrorResponses,
	"moveFile":           fileErrorResponses,
	"setFilePermissions": fileErrorResponses,
	"createDownloadUrl": {
		// The pinned table declares 400/401/404/409. Issuance never contacts
		// a node, so it has no approved 502 and never emits 503; only the
		// standing internal-fault answer is added.
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"sshKey": {
		// The orchestrator approved exactly these three statuses for the
		// pinned sshKey operation. A control-plane fault is the standing
		// 500; any node or factory failure is the direct-failure 502; a
		// guest that has not generated its host key yet is the retryable
		// 503 with a positive retry hint. A stopped sandbox or a stale boot
		// stays the declared 400 machine_not_running, never a 409.
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
		http.StatusBadGateway:          {Codes: []string{"sandbox_direct_failed"}},
		http.StatusServiceUnavailable: {
			Codes:                    []string{"ssh_host_key_unavailable"},
			RequireRetryAfterSeconds: true,
		},
	},
	"createUploadUrl": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"listSavedSnapshots": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"saveSavedSnapshot": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"getSavedSnapshot": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
	"deleteSavedSnapshot": {
		http.StatusInternalServerError: {Codes: []string{"http_500"}},
	},
}

// fileErrorResponses is the approved exception table shared by the eight core
// file operations. A file call can miss the bearer credential, miss the
// owner-scoped sandbox, see a settled sandbox with no usable machine, fault in
// the control plane or fail at the node. The pinned file tables declare only
// 200/400 (and 413 for reads and writes), so each of those statuses is listed
// here explicitly; there is no cross-operation fallback. RESOURCE_EXHAUSTED is
// a transport failure for file RPCs, never VM capacity, so 503 is rejected.
var fileErrorResponses = map[int]undocumentedError{
	http.StatusUnauthorized: {Codes: []string{"unauthorized"}},
	http.StatusNotFound:     {Codes: []string{"not_found"}},
	http.StatusConflict: {
		Codes: []string{"machine_not_running", "sandbox_not_ready", "sandbox_starting"},
	},
	http.StatusInternalServerError: {Codes: []string{"http_500"}},
	http.StatusBadGateway:          {Codes: []string{"sandbox_direct_failed"}},
}

// Response validates one complete response body against operationID. header
// must carry the actual Content-Type. Statuses not documented for the
// operation, wrong media types, missing required members, invalid const, enum
// or null values all fail. The only exceptions are the exact status/code pairs
// in undocumentedErrorResponses, validated against the pinned ErrorEnvelope.
func (v *Validator) Response(operationID string, status int, header http.Header, body []byte) error {
	indexed, ok := v.ops[operationID]
	if !ok {
		return fmt.Errorf("compat: operation %q is not in %s", operationID, v.path)
	}
	if header == nil {
		header = http.Header{}
	}
	exception, hasException := undocumentedErrorResponses[operationID][status]
	if hasException {
		if err := v.validateUndocumentedError(status, exception, header, body); err != nil {
			return fmt.Errorf("compat: %s response %d: %w", operationID, status, err)
		}
	}
	if !responsesDocumentStatus(indexed.op.Responses, status) {
		if !hasException {
			return fmt.Errorf("compat: %s response %d is not documented and has no approved exception", operationID, status)
		}
		return nil
	}
	request := &http.Request{
		Method: indexed.Method,
		URL:    &url.URL{Path: indexed.Path},
		Header: http.Header{},
	}
	route := &routers.Route{
		Spec:      v.doc,
		Path:      indexed.Path,
		PathItem:  indexed.item,
		Method:    indexed.Method,
		Operation: indexed.op,
	}
	input := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Request: request,
			Route:   route,
		},
		Options: &openapi3filter.Options{
			MultiError:            true,
			IncludeResponseStatus: true,
		},
		Status: status,
		Header: header,
		Body:   io.NopCloser(bytes.NewReader(body)),
	}
	if err := openapi3filter.ValidateResponse(v.ctx, input); err != nil {
		return fmt.Errorf("compat: %s response %d: %w", operationID, status, err)
	}
	return nil
}

func responsesDocumentStatus(responses *openapi3.Responses, status int) bool {
	if responses == nil {
		return false
	}
	return responses.Status(status) != nil || responses.Default() != nil
}

// validateUndocumentedError checks an approved exception shape: an exact
// ErrorEnvelope with ok=false, type sandbox.error, matching top-level and
// nested status/code, one of the listed codes, and any extra rule the entry
// carries.
func (v *Validator) validateUndocumentedError(status int, exception undocumentedError, header http.Header, body []byte) error {
	contentType, _, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(contentType, "application/json") {
		return fmt.Errorf("Content-Type %q is not application/json", header.Get("Content-Type"))
	}
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return errors.New("body is not valid JSON")
	}
	schemaRef := v.doc.Components.Schemas["ErrorEnvelope"]
	if schemaRef == nil || schemaRef.Value == nil {
		return errors.New("pinned ErrorEnvelope schema is missing")
	}
	var schemaOptions []openapi3.SchemaValidationOption
	if v.doc.IsOpenAPI31OrLater() {
		schemaOptions = append(schemaOptions, openapi3.EnableJSONSchema2020())
	}
	if err := schemaRef.Value.VisitJSON(value, schemaOptions...); err != nil {
		return fmt.Errorf("body does not match the pinned ErrorEnvelope: %w", err)
	}
	var envelope struct {
		OK     bool   `json:"ok"`
		Type   string `json:"type"`
		Status int    `json:"status"`
		Code   string `json:"code"`
		Error  struct {
			Code    string         `json:"code"`
			Status  int            `json:"status"`
			Details map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return errors.New("body is not a valid error envelope")
	}
	if envelope.OK {
		return errors.New("ok must be false on an error response")
	}
	if envelope.Type != "sandbox.error" {
		return fmt.Errorf("type %q is not sandbox.error", envelope.Type)
	}
	if envelope.Status != status {
		return fmt.Errorf("top-level status %d does not match HTTP status %d", envelope.Status, status)
	}
	if envelope.Error.Status != status {
		return fmt.Errorf("nested error status %d does not match HTTP status %d", envelope.Error.Status, status)
	}
	if !slices.Contains(exception.Codes, envelope.Code) {
		return fmt.Errorf("code %q is not approved for HTTP status %d", envelope.Code, status)
	}
	if envelope.Error.Code != envelope.Code {
		return fmt.Errorf("nested error code %q does not match top-level code %q", envelope.Error.Code, envelope.Code)
	}
	if exception.RequireRetryAfterSeconds {
		raw, ok := envelope.Error.Details["retryAfterSeconds"]
		seconds, isNumber := raw.(float64)
		if !ok || !isNumber || seconds <= 0 || math.Trunc(seconds) != seconds {
			return errors.New("error.details.retryAfterSeconds must be a positive integer")
		}
	}
	return nil
}

// NDJSON validates every non-empty line of body as one response frame of
// operationID. header must use the application/x-ndjson media type.
func (v *Validator) NDJSON(operationID string, status int, header http.Header, body []byte) error {
	frames := 0
	for i, line := range bytes.Split(body, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		frames++
		if err := v.Response(operationID, status, header, line); err != nil {
			return fmt.Errorf("frame %d: %w", i+1, err)
		}
	}
	if frames == 0 {
		return fmt.Errorf("compat: %s stream has no frames", operationID)
	}
	return nil
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		if isFile(filepath.Join(dir, "go.mod")) && isFile(filepath.Join(dir, SpecPath)) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("repository root not found")
		}
		dir = parent
	}
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
