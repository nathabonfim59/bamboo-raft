package guest

import (
	"fmt"
	"os"
	"os/user"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// credential returns the sandbox user credential for exec'd commands and
// helper processes: the configured uid and gid plus the user's group list
// resolved from the configured group(5) file with initgroups(3) semantics.
// Resolution failures are returned, never silently dropped: running a
// command with fewer groups than its user has is the kvm-class bug this
// exists to prevent.
//
// With NoSetGroups (the explicit unprivileged test seam) the child keeps
// the agent's own groups instead: adopting an explicit list needs
// CAP_SETGID, which only the real root agent has. The resolved list
// contains only groups the account is a member of — the primary group
// first, then the rest sorted and deduplicated — so a root agent passes
// none of root's or service groups on.
func (s *Service) credential() (*syscall.Credential, error) {
	cred := &syscall.Credential{Uid: uint32(s.uid), Gid: uint32(s.gid)}
	if s.noSetGroups {
		cred.NoSetGroups = true
		return cred, nil
	}
	name, err := accountName(s.uid)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.groupFile)
	if err != nil {
		return nil, fmt.Errorf("read group file %s: %w", s.groupFile, err)
	}
	cred.Groups, err = resolveUserGroups(data, name, s.gid)
	if err != nil {
		return nil, fmt.Errorf("parse group file %s: %w", s.groupFile, err)
	}
	return cred, nil
}

// accountName resolves the account name of uid through the system user
// database (the pure-Go implementation reads /etc/passwd, so static builds
// work). A uid without an account is an error: group membership is
// meaningless without the account it belongs to, and guessing a name would
// hand the process another account's groups.
func accountName(uid int) (string, error) {
	u, err := user.LookupId(strconv.Itoa(uid))
	if err != nil {
		return "", fmt.Errorf("lookup account for uid %d: %w", uid, err)
	}
	return u.Username, nil
}

// resolveUserGroups parses group(5) data and returns the group ids the
// named user belongs to with initgroups(3) semantics: its primary group
// first, then every group that lists the user as a member, deduplicated
// and sorted. Blank lines and comments are skipped; any other malformed
// line is an error, so a broken group database fails loudly instead of
// quietly dropping membership.
func resolveUserGroups(data []byte, user string, primaryGid int) ([]uint32, error) {
	ids := []uint32{uint32(primaryGid)}
	seen := map[uint32]bool{uint32(primaryGid): true}
	for i, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("group file line %d: want 4 fields, got %d", i+1, len(parts))
		}
		gid, err := strconv.ParseUint(parts[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("group file line %d: bad group id %q", i+1, parts[2])
		}
		for _, member := range strings.Split(parts[3], ",") {
			if member != user || seen[uint32(gid)] {
				continue
			}
			seen[uint32(gid)] = true
			ids = append(ids, uint32(gid))
		}
	}
	slices.Sort(ids[1:])
	return ids, nil
}
