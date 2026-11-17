package api

import "net/http"

// Route patterns of the Phase 0.5 core subset. They include the /api/v1 prefix
// and match the v1 API paths.
const (
	PathMe                = "/api/v1/me"
	PathSandboxes         = "/api/v1/sandboxes"
	PathSandbox           = "/api/v1/sandboxes/{sandboxId}"
	PathSandboxStop       = "/api/v1/sandboxes/{sandboxId}/stop"
	PathSandboxStops      = "/api/v1/sandboxes/{sandboxId}/stops/{stopId}"
	PathSandboxResume     = "/api/v1/sandboxes/{sandboxId}/resume"
	PathDeletionOperation = "/api/v1/deletion-operations/{operationId}"
)

// Path parameter names set by the route patterns. Handlers read them with
// r.PathValue.
const (
	ParamSandboxID   = "sandboxId"
	ParamStopID      = "stopId"
	ParamOperationID = "operationId"
)

// Handlers holds the phase 0.5 core and Phase 1 lifecycle route handlers. The
// server owns the middleware stack (request id, auth, error rendering);
// Register only maps the paths. A nil handler leaves its route unregistered,
// so a partial server can start while a feature is disabled.
type Handlers struct {
	Me                   http.HandlerFunc
	ListSandboxes        http.HandlerFunc
	CreateSandbox        http.HandlerFunc
	GetSandbox           http.HandlerFunc
	UpdateSandbox        http.HandlerFunc
	DeleteSandbox        http.HandlerFunc
	GetDeletionOperation http.HandlerFunc
	StopSandbox          http.HandlerFunc
	GetSandboxStop       http.HandlerFunc
	ResumeSandbox        http.HandlerFunc
}

// Register adds the core v1 routes to mux. The mux should be the
// server's root mux; the patterns carry the /api/v1 prefix.
func Register(mux *http.ServeMux, h Handlers) {
	handle := func(pattern string, handler http.HandlerFunc) {
		if handler != nil {
			mux.HandleFunc(pattern, handler)
		}
	}
	handle("GET "+PathMe, h.Me)
	handle("GET "+PathSandboxes, h.ListSandboxes)
	handle("POST "+PathSandboxes, h.CreateSandbox)
	handle("GET "+PathSandbox, h.GetSandbox)
	handle("PATCH "+PathSandbox, h.UpdateSandbox)
	handle("DELETE "+PathSandbox, h.DeleteSandbox)
	handle("GET "+PathDeletionOperation, h.GetDeletionOperation)
	handle("POST "+PathSandboxStop, h.StopSandbox)
	handle("GET "+PathSandboxStops, h.GetSandboxStop)
	handle("POST "+PathSandboxResume, h.ResumeSandbox)
}
