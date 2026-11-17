package compat

// Phase-exit route coverage for the detached command endpoints. The existing
// route-constant tests predate command status, attach and stop; a missing
// subset entry would mean the published document no longer describes a route
// the compiled server registers.

import (
	"net/http"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// TestPhaseExitCommandRoutesMatchSubset keeps every detached command route
// constant present in api/bbraft-v1.yaml.
func TestPhaseExitCommandRoutesMatchSubset(t *testing.T) {
	subset := Load(t)
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodPost, api.CommandsPath},
		{http.MethodGet, api.ProcessStatusPath},
		{http.MethodDelete, api.ProcessStatusPath},
		{http.MethodGet, api.CommandAttachPath},
	}
	for _, route := range routes {
		if !subset.HasOperation(route.method, route.path) {
			t.Errorf("%s %s is registered but absent from api/bbraft-v1.yaml", route.method, route.path)
		}
	}
}
