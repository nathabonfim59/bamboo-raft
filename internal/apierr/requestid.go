package apierr

import (
	"context"
	"net/http"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

type requestIDKey struct{}

// RequestID is the middleware that gives every request an id of the shape
// req_<26 random chars>, stored in its context for WriteError and echoed
// back in the X-Request-Id response header.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := NewRequestID()
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// NewRequestID returns a fresh request id.
func NewRequestID() string { return ids.New("req_", 26) }

// RequestIDFrom returns the id the RequestID middleware stored, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
