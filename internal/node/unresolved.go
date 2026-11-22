package node

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// requireResolved makes an operation that would act on, or commit state for, a
// VM record whose restart recovery could not decide ownership resolve it
// first, exactly as DeleteVM does. It must be called with the VM operation
// gate held (acquireVMOperation) and without v.mu, so the resolution is
// serialized with every other operation on the same record.
//
// A record that is still unresolved afterwards refuses the operation with
// UNAVAILABLE: the caller commits nothing, starts no QEMU and changes no
// state. The contract reserves UNAVAILABLE for "the node cannot act right
// now", which is exactly a VM whose QEMU a restart could neither adopt nor
// prove gone.
func (m *Manager) requireResolved(v *vm) error {
	if err := m.resolveUnresolved(v); err != nil {
		return status.Errorf(codes.Unavailable,
			"sandbox %s: the VM's QEMU could not be resolved after a restart: %v", v.id, err)
	}
	return nil
}
