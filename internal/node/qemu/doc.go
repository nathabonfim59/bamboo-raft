// Package qemu builds QEMU's argv and owns one QEMU process per sandbox VM.
// There is no shell and no libvirt: argv is an explicit slice,
// and the only control channel is the QMP client in internal/node/qmp.
//
// Start refuses to run as root, rejects a pre-existing QMP socket instead of
// connecting to or deleting it, and proves through SO_PEERCRED that the QMP
// server is the child it just started. The process runs in its own process
// group as the unprivileged node user. QEMU gets -sandbox on (seccomp) with
// obsolete syscalls, privilege elevation, spawning and scheduler changes
// denied; the full jailer (own uid, namespaces, chroot, cgroups) is a later
// phase.
//
// The process lifetime is independent of the context passed to Start: that
// context bounds only the spawn and QMP handshake. CreateVM may return while
// the VM keeps running, and a nodelink reconnect must not kill it.
//
// # Fixed CPU model, nesting and no silent downgrade
//
// The CPU model is Config.CPUModel (default Haswell-noTSX) with enforce always
// set, so the default is "-cpu Haswell-noTSX,enforce". Config.CPU and
// ResolveCPU own the policy: NestedAuto adds "+vmx" only for a GenuineIntel
// host where every CPU reports vmx; NestedOff adds nothing; NestedOn requires
// that same Intel+vmx combination and fails otherwise. AMD hosts get no +svm in
// phase 0.5: the fixed model keeps an Intel CPUID vendor, and the in-guest
// kvm_amd refuses a non-AMD vendor (Linux __kvm_is_svm_supported), so
// advertising SVM would be misleading. Start logs the AMD reason at Info.
//
// QEMU's default "check" only warns and filters features the host lacks;
// "enforce" aborts instead, so Info.cpu_model cannot claim a model the VM does
// not have. The machine has a single accelerator entry, accel=kvm, and QEMU
// only tries the next accelerator when the list has more than one, so there is
// no TCG fallback. A host without the required features fails the boot.
//
// # Test-only host passthrough
//
// Production Config rejects the host-dependent "host" and "max" models.
// Config.AllowHostCPU (default false) is the explicit opt-in used only by the
// KVM test harness and image probe; with it and CPUModel exactly "host",
// Config.CPU calls ResolveTestCPU, which accepts only "host", leaves max
// rejected, applies the same nested policy (so Intel auto may yield
// "host,+vmx,enforce" and AMD auto "host,enforce") and still appends enforce.
// That is a Manager/test-config decision, never an environment read inside
// this package.
//
// # Disk cache, flushes and the snapshot contract
//
// The root disk is opened as
//
//	-blockdev driver=qcow2,lazy-refcounts=off,file.driver=file,...,file.cache.direct=off,file.cache.no-flush=off
//	-device virtio-blk-pci,drive=rootdisk,write-cache=on,werror=stop,rerror=stop
//
// which is the documented writeback combination: cache.writeback=on,
// cache.direct=off, cache.no-flush=off (QEMU 10.0.0 qemu-options.hx, "-drive
// cache=writeback" table). Writes are acknowledged when they reach the host
// page cache, and guest flush requests are honoured rather than ignored.
// lazy-refcounts=off only disables lazy reference-count updates; QEMU still
// has qcow2 metadata caches, and the flush path below is what covers them.
// werror=stop and rerror=stop make QEMU pause and record an error, which is
// what makes query-block's io-status available for the check below
// (qapi/block-core.json, BlockInfo.io-status is only present for virtio-blk
// when the VM is configured to stop on errors).
//
// The approved A3 sequence is guest Freeze, QMP stop, reflink copy, QMP cont,
// guest Thaw. Stop is correct here because QEMU 10.0.0's do_vm_stop drains and
// flushes block devices after pausing the vCPUs (system/cpus.c:
// bdrv_drain_all(); ret = bdrv_flush_all()). The asynchronous STOP event is
// emitted before those calls, so Pause waits for the command reply, checks
// query-status, and requires the rootdisk's top-level BlockInfo io-status to be
// "ok"; it never treats the STOP event alone as proof.
//
// Two limits are deliberate and must stay documented:
//
//   - qmp_stop discards bdrv_flush_all's return value (monitor/qmp-cmds.c,
//     qmp_stop ignores vm_stop's result), so a stop reply cannot prove the
//     flush succeeded. A3 must validate copied qcow2 images and their data
//     with real-QEMU tests, not only with this API.
//   - A3 must fsync the copied file and its directory before publishing the
//     snapshot. QEMU does not own the copy.
//
// cache=directsync was rejected: it makes every write synchronous, QEMU
// documents that cache.writeback=off has "a major impact on performance", and
// it does not remove the need for the guest freeze or the A3 fsync. See
// docs/node.md for the operator-facing summary.
//
// # Hostname and guest state
//
// Config.Hostname is passed as the kernel parameter
// systemd.hostname=<name>, which systemd consumes before any guest unit runs.
// It is not a QEMU device property and does not depend on a restore hook. The
// QEMU command line carries only the sandbox id via -name.
package qemu
