// Package jailer launches one QEMU process per sandbox with a dedicated Unix
// identity, cgroup v2 limits, mount/PID/IPC namespaces and a minimal chroot.
//
// The package deliberately does not build QEMU argv or own QMP. The caller
// supplies argv and an ordered descriptor list; descriptor i is child fd 3+i.
// TAP and vhost-net are always fds 3 and 4; kernel, initrd, vhost-vsock and
// the disks follow in the caller's order, and every descriptor must be one
// of those. The argv must match the supported forms on validateArgs: fixed
// control paths (GuestQMPPath, GuestSerialPath), /proc/self/fd/N for every
// host resource, and one explicit -blockdev per disk from the oldest backing
// layer to the writable overlay, each overlay naming its predecessor as
// backing. QEMU therefore never resolves a stored backing name.
//
// The runtime is staged, never mounted: the QEMU binary at its declared host
// path, its PT_INTERP and DT_NEEDED closure (found by parsing ELF, never by
// running ldd or the loader) and Config.FirmwareFiles, each copied read-only
// from a root-owned, not group/other writable source. No host tree, data
// directory or glob is exposed.
//
// Start uses a same-PID re-execution helper. One clone3 creates it as the
// direct child in its namespaces and inside its owned cgroup, so it is
// contained from birth; it waits while the parent verifies that membership
// and commits its identity, then attaches only the declared resources (the
// control directory and serial log arrive as detached, non-recursive,
// nosuid/nodev/noexec mount clones made by the parent with open_tree),
// chroots, drops every group and capability, sets no_new_privs and execs the
// staged QEMU. No intermediary process remains: the QMP SO_PEERCRED check
// observes exactly Cmd.Process.Pid. The exec is proven by comparing the
// child's executable inode with the staged binary, not by the status pipe.
//
// UIDs, cgroups and jail directories are durable reservations, persisted
// before each host mutation and removed only by inode. Close never releases
// them. Release and Reconcile remove them only after the recorded process is
// proven dead through a pidfd or host boot id plus /proc start time, and its
// cgroup is empty; an unresolved process, an uncertain write or a replaced
// entry keeps every resource. A permanent lock file serializes Managers of
// one StateDir, including across processes.
package jailer
