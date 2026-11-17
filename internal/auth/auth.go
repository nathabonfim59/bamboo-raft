// Package auth holds tenant identity and the credential checks of the
// control plane. Bearer API keys arrive configured raw and
// are kept only as SHA-256 digests; node tokens are configured as digests.
// All comparisons are fixed-length and constant time, and raw credentials
// never appear in errors or logs.
package auth

import "context"

// Identity is the authenticated tenant of a request. Owner is the account
// id that scopes every sandbox, snapshot and key row.
type Identity struct {
	// Owner is the account id the request acts for.
	Owner string
	// KeyID names the configured api_keys record that authenticated.
	KeyID string
}

type identityKey struct{}

// WithIdentity returns a context carrying id.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFrom returns the identity Middleware stored, or false.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	return id, ok
}
