package node

// removeStaleQMPSocket removes the qmp.sock a non-jailed QEMU of VM id left
// in its VM directory. QEMU unlinks it on a clean quit, but a crashed or
// killed QEMU does not, and the launch refuses an existing socket, so the VM
// could never start again. Callers have proven that QEMU gone: settleJail
// for a process handle of ours (exit watcher, stop, failed boot, maintenance
// end) and restart recovery for a recorded QEMU found dead. Nothing that
// merely failed to launch removes it, so the launch-time refusal still
// guards a socket nobody proved stale. Only a socket is removed; anything
// else is kept (and a start keeps refusing it). Jailed sockets live in the
// pinned control directory and are settled by the jail's reclaim.
func (m *Manager) removeStaleQMPSocket(id string) {
	if m.jail != nil {
		return
	}
	if err := m.store.RemoveStaleQMPSocket(id); err != nil {
		m.log.Warn("node: stale QMP socket kept; a start refuses it", "sandbox", id, "error", err)
	}
}
