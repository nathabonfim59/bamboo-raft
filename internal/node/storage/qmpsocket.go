package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"path"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// QMPSocketName is the QMP socket a non-jailed QEMU creates in its VM
// directory (vms/<id>/qmp.sock). QEMU removes it on a clean quit; a crashed
// or killed QEMU leaves it behind.
const QMPSocketName = "qmp.sock"

// RemoveStaleQMPSocket removes the QMP socket a non-jailed QEMU of VM id
// left behind. The caller must have proven that QEMU gone: the launch-time
// refusal of an existing socket stays the guard against a live one. Only a
// socket is removed; anything else under that name is an error and left
// alone. A missing socket is fine.
func (s *Store) RemoveStaleQMPSocket(id string) error {
	if !ids.ValidSandbox(id) {
		return fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	rel := path.Join(vmsDir, id, QMPSocketName)
	info, err := s.root.Lstat(rel)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: vm %s: inspect %s: %w", id, rel, err)
	}
	if info.Mode().Type() != fs.ModeSocket {
		return fmt.Errorf("storage: vm %s: %s has type %v; leaving it alone", id, rel, info.Mode().Type())
	}
	if err := s.root.Remove(rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("storage: vm %s: remove %s: %w", id, rel, err)
	}
	return nil
}
