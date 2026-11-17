package storage

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockDir takes the exclusive node lock. The lock is advisory (flock) and
// held for the lifetime of the Store: startup reconciliation may kill
// processes and delete state, so two managers must never run at once.
func lockDir(root *os.Root) (*os.File, error) {
	f, err := root.OpenFile(lockName, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", lockName, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("storage: %s: %w", lockName, ErrLocked)
		}
		return nil, fmt.Errorf("storage: lock %s: %w", lockName, err)
	}
	return f, nil
}

func unlock(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		f.Close()
		return fmt.Errorf("storage: unlock: %w", err)
	}
	return f.Close()
}
