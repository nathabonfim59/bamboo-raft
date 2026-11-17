package api

import "net/http"

// Middleware wraps one API handler.
type Middleware func(http.Handler) http.Handler

// Chain wraps h with middleware. The first middleware is the outermost, so
// Chain(h, a, b) runs a, then b, then h.
func Chain(h http.Handler, middleware ...Middleware) http.Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		h = middleware[i](h)
	}
	return h
}

// ConfirmDeleteHeader is the API header that must exactly equal the target
// sandbox or snapshot id before a deletion is accepted.
const ConfirmDeleteHeader = "X-Ascii-Confirm-Delete"

// ConfirmDeleteReason says why a confirmation header was refused.
type ConfirmDeleteReason string

// Confirmation failures.
const (
	ConfirmDeleteMissing  ConfirmDeleteReason = "missing"
	ConfirmDeleteMismatch ConfirmDeleteReason = "mismatch"
)

// ConfirmDeleteError reports a missing or mismatched confirmation header.
// Handlers map it to the pinned 409 deletion confirmation error.
type ConfirmDeleteError struct {
	Target string
	Reason ConfirmDeleteReason
}

// Error implements error without echoing the header value.
func (e *ConfirmDeleteError) Error() string {
	if e.Reason == ConfirmDeleteMissing {
		return "missing " + ConfirmDeleteHeader + " header"
	}
	return ConfirmDeleteHeader + " does not match " + e.Target
}

// RequireConfirmDelete checks that X-Ascii-Confirm-Delete exactly equals
// target. The header is not a secret, so plain comparison is intentional.
func RequireConfirmDelete(r *http.Request, target string) error {
	value := r.Header.Get(ConfirmDeleteHeader)
	if value == "" {
		return &ConfirmDeleteError{Target: target, Reason: ConfirmDeleteMissing}
	}
	if value != target {
		return &ConfirmDeleteError{Target: target, Reason: ConfirmDeleteMismatch}
	}
	return nil
}

// NoStore is middleware for responses that must never be cached, notably
// deletion-operation polls.
func NoStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
