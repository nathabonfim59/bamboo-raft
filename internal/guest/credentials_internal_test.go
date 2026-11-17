package guest

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestResolveUserGroups checks the initgroups(3) semantics of the group
// resolution: primary group first, then every group listing the user as a
// member, deduplicated and sorted, with unrelated groups (root's, service
// groups) left out and malformed lines rejected.
func TestResolveUserGroups(t *testing.T) {
	valid := strings.Join([]string{
		"root:x:0:root",
		"kvm:x:78:user",
		"wheel:x:10:root,admin",
		"users:x:100:user,other",
		"dup:x:78:user", // duplicate gid under another name
		"empty:x:900:",
		"# a comment",
		"",
	}, "\n")
	tests := []struct {
		name       string
		data       string
		user       string
		primaryGid int
		want       []uint32
		wantErr    string
	}{
		{
			name:       "primary plus member groups, sorted, deduplicated",
			data:       valid,
			user:       "user",
			primaryGid: 1000,
			want:       []uint32{1000, 78, 100},
		},
		{
			name:       "no member groups leaves only the primary",
			data:       valid,
			user:       "stranger",
			primaryGid: 1001,
			want:       []uint32{1001},
		},
		{
			name:       "root user gets only groups listing root",
			data:       valid,
			user:       "root",
			primaryGid: 0,
			want:       []uint32{0, 10},
		},
		{
			name:    "wrong field count is an error",
			data:    "disk:x:6:user:extra\n",
			wantErr: "want 4 fields",
		},
		{
			name:    "non-numeric group id is an error",
			data:    "bad:x:notanumber:user\n",
			wantErr: "bad group id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveUserGroups([]byte(tt.data), tt.user, tt.primaryGid)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("resolveUserGroups error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveUserGroups: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("resolveUserGroups(%q, %d) = %v, want %v", tt.user, tt.primaryGid, got, tt.want)
			}
		})
	}
}

// TestCredential builds credentials the way Exec and the file helper do:
// the account's resolved group list with the test seam off, the agent's
// own groups with it on, and loud errors for a missing account or an
// unusable group database. This is the regression for the orchestrator's
// kvm bug: before the fix the credential cleared every supplementary group
// (Groups:[] where the account's groups belonged).
func TestCredential(t *testing.T) {
	name, err := accountName(os.Getuid())
	if err != nil {
		t.Skipf("test uid %d has no account on this machine: %v", os.Getuid(), err)
	}
	groupFile := filepath.Join(t.TempDir(), "group")
	writeGroups := func(content string) {
		t.Helper()
		if err := os.WriteFile(groupFile, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeGroups("kvm:x:78:" + name + "\nwheel:x:10:svc\n")

	s := &Service{uid: os.Getuid(), gid: os.Getgid(), groupFile: groupFile}
	cred, err := s.credential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	want := []uint32{uint32(os.Getgid()), 78}
	if cred.Uid != uint32(os.Getuid()) || cred.Gid != uint32(os.Getgid()) || cred.NoSetGroups || !slices.Equal(cred.Groups, want) {
		t.Fatalf("credential = %+v, want uid/gid of the test account with groups %v", cred, want)
	}

	s.noSetGroups = true
	cred, err = s.credential()
	if err != nil {
		t.Fatalf("credential (no setgroups): %v", err)
	}
	if !cred.NoSetGroups || cred.Groups != nil {
		t.Fatalf("credential (no setgroups) = %+v, want NoSetGroups and no explicit list", cred)
	}

	s.noSetGroups = false
	s.groupFile = filepath.Join(t.TempDir(), "missing")
	if _, err := s.credential(); err == nil {
		t.Fatal("credential with a missing group file succeeded, want an error")
	}
	s.groupFile = groupFile
	writeGroups("broken line\n")
	if _, err := s.credential(); err == nil {
		t.Fatal("credential with a malformed group file succeeded, want an error")
	}

	// A uid without an account must never resolve to another name's
	// groups, even with a group file listing that other name.
	s.uid = 4294967294 // nobody's valid account in a guest layout
	if _, err := s.credential(); err == nil {
		t.Fatal("credential for an unknown uid succeeded, want an error")
	}
}

// TestAccountName pins the lookup error shape for a uid without an
// account: a clear error, not a fallback name.
func TestAccountName(t *testing.T) {
	if _, err := accountName(4294967294); err == nil {
		t.Fatal("accountName(4294967294) succeeded, want an error")
	}
	name, err := accountName(os.Getuid())
	if err != nil {
		t.Skipf("test uid %d has no account on this machine: %v", os.Getuid(), err)
	}
	if name == "" {
		t.Fatalf("accountName(%d) = %q, want the account's name", os.Getuid(), name)
	}
}
