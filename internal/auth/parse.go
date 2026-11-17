package auth

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
)

// readCredFile reads a credential file honoring ctx. The file formats live
// with their loaders; this only shares the cancellation and I/O handling.
func readCredFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("auth: read %s: %w", path, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return raw, nil
}

// unmarshalCred decodes one credential file. Unknown members are rejected
// so a misplaced raw key cannot slip past the schema. The returned error
// never wraps the decoder's: JSON errors may echo file content, and these
// files hold credentials. A syntactic failure reports only its byte
// offset; anything else is a generic "invalid" error.
func unmarshalCred(path, what string, raw []byte, v any) error {
	if err := json.Unmarshal(raw, v, json.RejectUnknownMembers(true)); err != nil {
		if se, ok := errors.AsType[*jsontext.SyntacticError](err); ok {
			return fmt.Errorf("auth: %s: invalid JSON at byte offset %d", path, se.ByteOffset)
		}
		return fmt.Errorf("auth: %s: invalid %s file", path, what)
	}
	return nil
}
