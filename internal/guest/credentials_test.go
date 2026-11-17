// Live credential fixture: `id -G` through the service must report the
// sandbox user's supplementary groups (initgroups semantics). Adopting an
// explicit group list needs CAP_SETGID, so the live exec check runs only
// as root in throwaway VMs; the unprivileged construction and parser
// regressions live in credentials_internal_test.go.
package guest_test

import (
	"os"
	"os/exec"
	"os/user"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
)

// TestExecExtraGroupRoot runs only in throwaway VMs
// (BBRAFT_TEST_GROUPS_ROOT=1 and euid 0, with a guest-layout "user"
// account as in the v1 image). It adds a group listing the sandbox user to
// /etc/group and checks that `id -G` through the service reports exactly
// what id(1) itself says the account belongs to after the change — the
// account's real groups (the refreshed VM user already belongs to kvm and
// sudo) plus the new one, and no unrelated service groups. /etc/group is
// restored afterwards, leaving no group changes behind.
//
// This is the live regression for the orchestrator's kvm bug: before the
// fix the credential cleared every supplementary group, so a user added to
// a group still could not use its devices.
func TestExecExtraGroupRoot(t *testing.T) {
	if os.Getenv("BBRAFT_TEST_GROUPS_ROOT") != "1" {
		t.Skip("set BBRAFT_TEST_GROUPS_ROOT=1 in a throwaway VM to run the real /etc/group fixture")
	}
	if os.Geteuid() != 0 {
		t.Fatal("BBRAFT_TEST_GROUPS_ROOT=1 requires root")
	}
	u, err := user.Lookup("user")
	if err != nil {
		t.Fatalf("no guest-layout user account: %v", err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		t.Fatal(err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		t.Fatal(err)
	}

	const extra = 60513 // above the system range, outside any image default
	backup, err := os.ReadFile("/etc/group")
	if err != nil {
		t.Fatal(err)
	}
	// The restore is registered before the file is touched: whatever fails
	// below, the group database goes back to exactly what it was.
	t.Cleanup(func() {
		if err := os.WriteFile("/etc/group", backup, 0o644); err != nil {
			t.Errorf("restore /etc/group: %v", err)
		}
	})
	line := "bbraft-grouptest:x:" + strconv.Itoa(extra) + ":user\n"
	if err := os.WriteFile("/etc/group", append(slices.Clone(backup), line...), 0o644); err != nil {
		t.Fatal(err)
	}

	// id(1) is the oracle for the account's expected group set.
	oracle, err := exec.Command("id", "-G", u.Username).Output()
	if err != nil {
		t.Fatalf("id -G %s: %v", u.Username, err)
	}
	want := strings.Fields(string(oracle))
	slices.Sort(want)
	if !slices.Contains(want, strconv.Itoa(extra)) {
		t.Fatalf("id -G %s = %v after the change, want it to include the new group %d", u.Username, want, extra)
	}

	h := newHarness(t, func(c *guest.Config) {
		c.UID, c.GID, c.NoSetGroups = uid, gid, false
	})
	res := h.exec(t, `id -G`)
	got := strings.Fields(res.Stdout)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("id -G through the service = %v, want the account's set %v", got, want)
	}
}
