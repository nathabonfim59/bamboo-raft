package apierr

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeMap maps a failed nodev1 RPC to the error the API answers, per
// the table in proto/bbraft/node/v1/node.proto:
//
//	NOT_FOUND            → 404 (NotFound code)
//	INVALID_ARGUMENT     → 400 (InvalidArgument code)
//	FAILED_PRECONDITION  → 409 sandbox_not_ready
//	RESOURCE_EXHAUSTED   → 503 out_of_capacity
//	ALREADY_EXISTS       → 409 conflict
//	anything else        → 502 sandbox_direct_failed
//
// The node's own error text never reaches the client; messages are fixed.
type NodeMap struct {
	// NotFound is the code for codes.NotFound (CodeNotFound or
	// CodeSnapshotNotFound). Zero defaults to CodeNotFound.
	NotFound string
	// InvalidArgument is the code for codes.InvalidArgument
	// (CodeInvalidPath or CodeInvalidJSON). Zero defaults to
	// CodeInvalidPath.
	InvalidArgument string
	// Retryable is reported as details.retryable on sandbox_direct_failed
	// answers. File operations give the same result again (true); a
	// command may already be running (false).
	Retryable bool
}

// Map converts err, which must be a gRPC status error from a nodev1 call.
// A non-status error is treated like any other node failure.
func (m NodeMap) Map(err error) *Error {
	st, ok := status.FromError(err)
	if !ok {
		return m.directFailed()
	}
	switch st.Code() {
	case codes.NotFound:
		return New(http.StatusNotFound, orDefault(m.NotFound, CodeNotFound), "not found")
	case codes.InvalidArgument:
		return New(http.StatusBadRequest, orDefault(m.InvalidArgument, CodeInvalidPath), "invalid request")
	case codes.FailedPrecondition:
		return New(http.StatusConflict, CodeSandboxNotReady, "sandbox is not ready").Retryable(false)
	case codes.ResourceExhausted:
		return New(http.StatusServiceUnavailable, CodeOutOfCapacity, "node has no capacity left")
	case codes.AlreadyExists:
		return New(http.StatusConflict, CodeConflict, "the request conflicts with existing state")
	default:
		return m.directFailed()
	}
}

func (m NodeMap) directFailed() *Error {
	return New(http.StatusBadGateway, CodeSandboxDirectFailed,
		"the sandbox machine could not complete the call").Retryable(m.Retryable)
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
