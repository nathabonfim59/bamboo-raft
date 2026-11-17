package guest

import (
	"cmp"
	"os"
	"path"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resolve maps a v1 API path onto the service's configured roots.
func (s *Service) resolve(p string) (*root, string, string, error) {
	return resolveIn(s.homeRoot, s.tmpRoot, p)
}

// resolveIn maps a v1 API path onto the given confinement roots: relative
// paths start at homeSpec, absolute paths must be /home/user/... or /tmp/....
// It returns the confinement root that owns the path, the path relative to
// that root (no ".." components), the spec canonical path (/home/user/... or
// /tmp/...) and the configured root directory. Symlink escapes are caught by
// the returned root at open time (and by [fileServer.walk] across roots), not
// here.
func resolveIn(homeRoot, tmpRoot *os.Root, p string) (rt *root, rel, canonical string, err error) {
	if p == "" {
		return nil, "", "", status.Error(codes.InvalidArgument, "path is required")
	}
	if !path.IsAbs(p) {
		p = homeSpec + "/" + p
	}
	p = path.Clean(p)
	switch {
	case p == homeSpec || strings.HasPrefix(p, homeSpec+"/"):
		rel = strings.TrimPrefix(strings.TrimPrefix(p, homeSpec), "/")
		return &root{r: homeRoot, spec: homeSpec}, cmp.Or(rel, "."), p, nil
	case p == tmpSpec || strings.HasPrefix(p, tmpSpec+"/"):
		rel = strings.TrimPrefix(strings.TrimPrefix(p, tmpSpec), "/")
		return &root{r: tmpRoot, spec: tmpSpec}, cmp.Or(rel, "."), p, nil
	}
	return nil, "", "", status.Errorf(codes.InvalidArgument, "path %q is outside %s and %s", p, homeSpec, tmpSpec)
}

// root pairs an [os.Root] with the API path prefix it was opened for.
type root struct {
	r    *os.Root
	spec string
}
