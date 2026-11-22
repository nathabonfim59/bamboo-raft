package s3test

import "testing"

func TestParseKeyOutput(t *testing.T) {
	tests := []struct {
		name       string
		output     string
		wantID     string
		wantSecret string
	}{
		{
			name: "garage table",
			output: `===== KEY INFORMATION =====
Key ID: GK0123456789abcdef
Secret key: 9876543210fedcba
Permissions: create buckets, etc.
`,
			wantID:     "GK0123456789abcdef",
			wantSecret: "9876543210fedcba",
		},
		{
			name: "indented labels",
			output: `  Key ID:  AKIAEXAMPLE
  Secret key: secretvalue
`,
			wantID:     "AKIAEXAMPLE",
			wantSecret: "secretvalue",
		},
		{
			name:   "unrelated output",
			output: "Error: no key\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, secret := parseKeyOutput(tt.output)
			if id != tt.wantID || secret != tt.wantSecret {
				t.Fatalf("parseKeyOutput = (%q, %q), want (%q, %q)", id, secret, tt.wantID, tt.wantSecret)
			}
		})
	}
}
