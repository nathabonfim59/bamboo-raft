package compat

import (
	"net/http"
	"testing"
)

// TestProcessStreamFixturesValidate checks the attach NDJSON and stop response
// fixtures against the v1 API document. Each frame is one complete response for
// the attach operation, matching the pin's CommandAttachFrame schema.
func TestProcessStreamFixturesValidate(t *testing.T) {
	validator := Load(t)
	t.Run("attach frames", func(t *testing.T) {
		if err := validator.NDJSON("attachCommand", http.StatusOK,
			mediaType(ndjsonMediaType), fixture(t, "command-attached.ndjson")); err != nil {
			t.Errorf("attach fixture does not validate: %v", err)
		}
	})
	t.Run("stop response", func(t *testing.T) {
		if err := validator.Response("stopCommand", http.StatusOK,
			mediaType(jsonMediaType), fixture(t, "command-stopped.json")); err != nil {
			t.Errorf("stop fixture does not validate: %v", err)
		}
	})
}
