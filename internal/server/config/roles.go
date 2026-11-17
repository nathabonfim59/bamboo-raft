package config

import (
	"fmt"
	"slices"
	"strings"
)

// Role is one of the server's separable duties. Roles
// share nothing in-process: they talk through Postgres, the object store,
// gRPC and HTTP only.
type Role string

// The Phase 0.5 roles. "all" (RolesAll) selects every one in the same
// process, using the same network clients as separate processes.
const (
	RoleAPI       Role = "api"       // public /api/v1 + /api/bbraft
	RoleLifecycle Role = "lifecycle" // job queue workers, placement, reaper
	RoleNodeHub   Role = "nodehub"   // nodelink listener + internal node gateway
)

// RolesAll is the -roles value that selects every role.
const RolesAll = "all"

// Roles is the set of roles this process runs.
type Roles map[Role]bool

// ParseRoles parses the -roles value: "all", or a comma-separated list of
// role names without duplicates.
func ParseRoles(s string) (Roles, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("config: roles must not be empty")
	}
	if s == RolesAll {
		return Roles{RoleAPI: true, RoleLifecycle: true, RoleNodeHub: true}, nil
	}
	roles := Roles{}
	for _, name := range strings.Split(s, ",") {
		role := Role(strings.TrimSpace(name))
		switch role {
		case RoleAPI, RoleLifecycle, RoleNodeHub:
			if roles[role] {
				return nil, fmt.Errorf("config: duplicate role %q", name)
			}
			roles[role] = true
		case RolesAll:
			return nil, fmt.Errorf("config: %q cannot mix with other roles", RolesAll)
		default:
			return nil, fmt.Errorf("config: unknown role %q (want %s, %s, %s or %s)",
				name, RoleAPI, RoleLifecycle, RoleNodeHub, RolesAll)
		}
	}
	return roles, nil
}

// Has reports whether r runs role.
func (r Roles) Has(role Role) bool { return r[role] }

// String lists the roles sorted, for logs and flag help.
func (r Roles) String() string {
	names := make([]string, 0, len(r))
	for role := range r {
		names = append(names, string(role))
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}
