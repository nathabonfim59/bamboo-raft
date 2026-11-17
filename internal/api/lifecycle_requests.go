package api

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"strconv"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// lifecycleMaxTTLSeconds is the pinned ttlSeconds maximum (30 days).
const lifecycleMaxTTLSeconds = 2592000

// lifecycleReadBody reads one optional JSON object body. It reports present
// false for a missing or empty body; PATCH and resume decide whether that is
// accepted.
func lifecycleReadBody(r *http.Request) (jsontext.Value, bool, *apierr.Error) {
	raw, present, err := coreReadCreateBody(r)
	if err != nil {
		return nil, false, coreInvalidJSON("request body is not valid JSON")
	}
	if !present {
		return nil, false, nil
	}
	if apiErr := coreRequireJSONMedia(r); apiErr != nil {
		return nil, false, apiErr
	}
	if !coreJSONObject(raw) {
		return nil, false, coreInvalidJSON("request body must be a JSON object")
	}
	return raw, true, nil
}

// parseStopSandbox reads the optional stop body. force defaults to false.
func parseStopSandbox(r *http.Request) (bool, *apierr.Error) {
	raw, present, apiErr := lifecycleReadBody(r)
	if apiErr != nil || !present {
		return false, apiErr
	}
	var syntax struct {
		Force jsontext.Value `json:"force,omitzero"`
	}
	if err := json.Unmarshal(raw, &syntax, json.RejectUnknownMembers(true)); err != nil {
		return false, coreInvalidJSON("request body is not valid JSON")
	}
	if coreJSONAbsent(syntax.Force) {
		return false, nil
	}
	if !coreJSONBool(syntax.Force) {
		return false, coreInvalidJSON("force must be a boolean")
	}
	var request StopSandboxRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return false, coreInvalidJSON("request body is not valid JSON")
	}
	return request.Force != nil && *request.Force, nil
}

// parseUpdateSandbox reads and validates one PATCH body. The pinned request
// body is required, so an empty body is invalid_json. An empty object is a
// valid no-op patch.
func parseUpdateSandbox(r *http.Request) (db.PatchSandboxParams, *apierr.Error) {
	raw, present, apiErr := lifecycleReadBody(r)
	if apiErr != nil {
		return db.PatchSandboxParams{}, apiErr
	}
	if !present {
		return db.PatchSandboxParams{}, coreInvalidJSON("request body must be a JSON object")
	}
	var syntax struct {
		Name       jsontext.Value `json:"name,omitzero"`
		TTLSeconds jsontext.Value `json:"ttlSeconds,omitzero"`
		Subdomain  jsontext.Value `json:"subdomain,omitzero"`
	}
	if err := json.Unmarshal(raw, &syntax, json.RejectUnknownMembers(true)); err != nil {
		return db.PatchSandboxParams{}, coreInvalidJSON("request body is not valid JSON")
	}
	var params db.PatchSandboxParams
	if !coreJSONAbsent(syntax.Name) {
		if !coreJSONString(syntax.Name) {
			return db.PatchSandboxParams{}, coreInvalidJSON("name must be a string")
		}
		var name string
		if err := json.Unmarshal(syntax.Name, &name); err != nil {
			return db.PatchSandboxParams{}, coreInvalidJSON("name must be a string")
		}
		name = coreTruncateName(name)
		if name == "" {
			return db.PatchSandboxParams{}, coreInvalidName("name must be between 1 and 120 characters")
		}
		params.Name = &name
	}
	if !coreJSONAbsent(syntax.TTLSeconds) {
		ttl, apiErr := parseTTLOption(syntax.TTLSeconds)
		if apiErr != nil {
			return db.PatchSandboxParams{}, apiErr
		}
		params.TTL = ttl
	}
	if !coreJSONAbsent(syntax.Subdomain) {
		if !coreJSONString(syntax.Subdomain) {
			return db.PatchSandboxParams{}, coreInvalidJSON("subdomain must be a string")
		}
		var subdomain string
		if err := json.Unmarshal(syntax.Subdomain, &subdomain); err != nil {
			return db.PatchSandboxParams{}, coreInvalidJSON("subdomain must be a string")
		}
		if err := db.ValidateSubdomain(subdomain); err != nil {
			return db.PatchSandboxParams{}, coreInvalidJSON("subdomain must be 3 to 40 lowercase letters, digits or single hyphens, with no reserved suffix")
		}
		params.Subdomain = &subdomain
	}
	return params, nil
}

// coreTruncateName keeps the pinned backend behavior: a name longer than 120
// Unicode characters is truncated rather than refused.
func coreTruncateName(name string) string {
	const max = db.MaxSandboxNameLength
	runes := []rune(name)
	if len(runes) <= max {
		return name
	}
	return string(runes[:max])
}

// parseTTLOption parses the PATCH/resume tri-state TTL: omitted (handled by
// the caller), explicit null, or an integer in the pinned range.
func parseTTLOption(raw jsontext.Value) (db.TTLOption, *apierr.Error) {
	value := bytes.TrimSpace(raw)
	if bytes.Equal(value, []byte("null")) {
		return db.TTLOption{Set: true}, nil
	}
	if !coreJSONInteger(value) {
		return db.TTLOption{}, coreInvalidJSON("ttlSeconds must be an integer from 1 to 2592000 or null")
	}
	seconds, err := strconv.Atoi(string(value))
	if err != nil || seconds < 1 || seconds > lifecycleMaxTTLSeconds {
		return db.TTLOption{}, coreInvalidJSON("ttlSeconds must be an integer from 1 to 2592000 or null")
	}
	return db.TTLOption{Set: true, Seconds: &seconds}, nil
}

// resumeInput is the validated resume body. MachineType and Resources are both
// zero when the request kept the stored shape.
type resumeInput struct {
	failFast    bool
	machineType string
	resources   db.Resources
	env         db.EnvChange
	ttl         db.TTLOption
	changes     resumeChanges
}

// parseResumeSandbox reads the optional resume body. Unsupported environment
// and noEnv requests are rejected explicitly instead of being ignored.
func parseResumeSandbox(r *http.Request) (resumeInput, *apierr.Error) {
	raw, present, apiErr := lifecycleReadBody(r)
	if apiErr != nil || !present {
		return resumeInput{}, apiErr
	}
	var syntax struct {
		FailFast    jsontext.Value `json:"failFast,omitzero"`
		Type        jsontext.Value `json:"type,omitzero"`
		Env         jsontext.Value `json:"env,omitzero"`
		Environment jsontext.Value `json:"environment,omitzero"`
		NoEnv       jsontext.Value `json:"noEnv,omitzero"`
		TTLSeconds  jsontext.Value `json:"ttlSeconds,omitzero"`
	}
	if err := json.Unmarshal(raw, &syntax, json.RejectUnknownMembers(true)); err != nil {
		return resumeInput{}, coreInvalidJSON("request body is not valid JSON")
	}
	var input resumeInput
	if !coreJSONAbsent(syntax.FailFast) {
		if !coreJSONBool(syntax.FailFast) {
			return resumeInput{}, coreInvalidJSON("failFast must be a boolean")
		}
		var failFast bool
		if err := json.Unmarshal(syntax.FailFast, &failFast); err != nil {
			return resumeInput{}, coreInvalidJSON("failFast must be a boolean")
		}
		input.failFast = failFast
	}
	if !coreJSONAbsent(syntax.Type) {
		if !coreJSONString(syntax.Type) {
			return resumeInput{}, coreInvalidJSON("type must be a supported machine type")
		}
		var name string
		if err := json.Unmarshal(syntax.Type, &name); err != nil {
			return resumeInput{}, coreInvalidJSON("type must be a supported machine type")
		}
		machine, ok := machinetype.Lookup(name)
		if !ok {
			return resumeInput{}, coreInvalidJSON("type must be a supported machine type")
		}
		input.machineType = machine.Name
		input.resources = db.Resources{
			VCPUs:     machine.VCPUs,
			MemoryMiB: machine.MemoryMiB,
			DiskGiB:   machine.DiskGiB,
		}
		input.changes.shape = true
	}
	if !coreJSONAbsent(syntax.Env) {
		if !coreJSONObject(syntax.Env) {
			return resumeInput{}, coreInvalidJSON("env must be an object of string values")
		}
		if !coreStringMapShape(syntax.Env) {
			return resumeInput{}, coreInvalidEnv("env values must be strings")
		}
		var values map[string]string
		if err := json.Unmarshal(syntax.Env, &values); err != nil {
			return resumeInput{}, coreInvalidEnv("env values must be strings")
		}
		if apiErr := coreValidateEnv(values); apiErr != nil {
			return resumeInput{}, apiErr
		}
		input.env = db.EnvChange{Set: true, Values: values}
		input.changes.env = true
	}
	if !coreJSONAbsent(syntax.Environment) {
		if !coreJSONString(syntax.Environment) {
			return resumeInput{}, coreInvalidJSON("environment must be a string")
		}
		var environment string
		if err := json.Unmarshal(syntax.Environment, &environment); err != nil {
			return resumeInput{}, coreInvalidJSON("environment must be a string")
		}
		if environment != "base" {
			return resumeInput{}, coreUnknownEnvironment()
		}
		input.changes.environment = true
	}
	if !coreJSONAbsent(syntax.NoEnv) {
		if !coreJSONBool(syntax.NoEnv) {
			return resumeInput{}, coreInvalidJSON("noEnv must be a boolean")
		}
		var noEnv bool
		if err := json.Unmarshal(syntax.NoEnv, &noEnv); err != nil {
			return resumeInput{}, coreInvalidJSON("noEnv must be a boolean")
		}
		if noEnv {
			return resumeInput{}, coreInvalidJSON("noEnv is not supported in Phase 1")
		}
	}
	if !coreJSONAbsent(syntax.TTLSeconds) {
		ttl, apiErr := parseTTLOption(syntax.TTLSeconds)
		if apiErr != nil {
			return resumeInput{}, apiErr
		}
		input.ttl = ttl
	}
	return input, nil
}
