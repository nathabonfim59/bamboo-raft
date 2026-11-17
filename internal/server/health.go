package server

import "net/http"

// HealthPath is the unauthenticated liveness endpoint of the api role. It is
// deliberately minimal: it says the HTTP process is serving, never reveals
// build, database, host or node details, and does not gate traffic.
const HealthPath = "/healthz"

// health answers liveness without touching a database or a node.
func health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
