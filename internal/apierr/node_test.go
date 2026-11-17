package apierr

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestNodeMap runs the full mapping table from the node.proto header. The
// node's own error text must never reach the client.
func TestNodeMap(t *testing.T) {
	const nodeSecret = "node internal: /var/lib/bbraft secret-path"
	tests := []struct {
		name       string
		err        error
		m          NodeMap
		wantStatus int
		wantCode   string
	}{
		{"not found default", status.Error(codes.NotFound, nodeSecret), NodeMap{}, 404, CodeNotFound},
		{"not found snapshot", status.Error(codes.NotFound, nodeSecret), NodeMap{NotFound: CodeSnapshotNotFound}, 404, CodeSnapshotNotFound},
		{"invalid default", status.Error(codes.InvalidArgument, nodeSecret), NodeMap{}, 400, CodeInvalidPath},
		{"invalid json variant", status.Error(codes.InvalidArgument, nodeSecret), NodeMap{InvalidArgument: CodeInvalidJSON}, 400, CodeInvalidJSON},
		{"precondition", status.Error(codes.FailedPrecondition, nodeSecret), NodeMap{}, 409, CodeSandboxNotReady},
		{"exhausted", status.Error(codes.ResourceExhausted, nodeSecret), NodeMap{}, 503, CodeOutOfCapacity},
		{"exists", status.Error(codes.AlreadyExists, nodeSecret), NodeMap{}, 409, CodeConflict},
		{"unavailable", status.Error(codes.Unavailable, nodeSecret), NodeMap{}, 502, CodeSandboxDirectFailed},
		{"deadline", status.Error(codes.DeadlineExceeded, nodeSecret), NodeMap{}, 502, CodeSandboxDirectFailed},
		{"internal", status.Error(codes.Internal, nodeSecret), NodeMap{}, 502, CodeSandboxDirectFailed},
		{"unknown", status.Error(codes.Unknown, nodeSecret), NodeMap{}, 502, CodeSandboxDirectFailed},
		{"canceled", status.Error(codes.Canceled, nodeSecret), NodeMap{}, 502, CodeSandboxDirectFailed},
		{"not a status", errors.New(nodeSecret), NodeMap{}, 502, CodeSandboxDirectFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := tc.m.Map(tc.err)
			if e.Status != tc.wantStatus || e.Code != tc.wantCode {
				t.Fatalf("Map = %d %s, want %d %s", e.Status, e.Code, tc.wantStatus, tc.wantCode)
			}
			if strings.Contains(e.Message, nodeSecret) || strings.Contains(e.Message, "secret-path") {
				t.Fatalf("node text leaked: %q", e.Message)
			}
			for k := range e.Details {
				if s, ok := e.Details[k].(string); ok && strings.Contains(s, nodeSecret) {
					t.Fatalf("node text leaked in details: %v", e.Details)
				}
			}
		})
	}
}

func TestNodeMapRetryableFlag(t *testing.T) {
	err := status.Error(codes.Unavailable, "connection lost")

	e := NodeMap{Retryable: true}.Map(err)
	if e.Details["retryable"] != true {
		t.Fatalf("retryable = %v", e.Details)
	}
	e = NodeMap{}.Map(err)
	if e.Details["retryable"] != false {
		t.Fatalf("retryable = %v", e.Details)
	}

	// FAILED_PRECONDITION always reports retryable: false (spec: the
	// sandbox is in a state that cannot run commands).
	e = NodeMap{Retryable: true}.Map(status.Error(codes.FailedPrecondition, "x"))
	if e.Code != CodeSandboxNotReady || e.Details["retryable"] != false {
		t.Fatalf("precondition = %s %v", e.Code, e.Details)
	}
}
