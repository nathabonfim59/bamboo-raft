package db

import (
	"encoding/json/v2"
	"fmt"
)

// jsonMarshal encodes v with encoding/json/v2.
func jsonMarshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("db: encode json: %w", err)
	}
	return b, nil
}

// jsonUnmarshal decodes b into v with encoding/json/v2.
func jsonUnmarshal(b []byte, v any) error {
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("db: decode json: %w", err)
	}
	return nil
}
