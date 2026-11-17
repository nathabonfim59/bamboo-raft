# Guest image

The guest image is an Ubuntu 24.04 (noble) amd64 image for direct kernel boot:
a kernel, an initrd and a whole-disk ext4 `root.qcow2`. It contains the
toolchain our test jobs need (Go, Node 24, a C toolchain, Podman), an sshd
with a per-boot ed25519 host key, and the `bbraft-guest` agent, and nothing
else that needs a credential. The current content version is `v1`; every
version directory is immutable, so content changes bump `images/version`
(never rebuild a published version in place).

Build entry points live in `images/` and stay separate from the repository
root Makefile:

```sh
make -C images image    # build images/out/<version>/
make -C images smoke    # unprivileged QEMU + vhost-vsock acceptance run
make -C images check    # shell syntax, publish/pin/manifest regressions, smoke tests
make -C images mkosi-config-check  # pinned mkosi's real workspace/source check
make -C images cpu-probe  # cheap unprivileged CPU-only QEMU probe
make -C images review   # root check, image check, CPU probe, image build, smoke
make -C images review-node  # A2 gate: prereqs, KVM test presence gate, build/smoke, non-snapshot KVM tests
make -C images review-all  # full A3 gate: prereqs, presence gate, node binary + role gate, build/smoke, full KVM incl. Snapshot
make -C images image-unpinned-test  # TEST-ONLY unpinned archive build (VM only)
make -C images smoke-unpinned-test  # boot the disposable test image explicitly
```

Artifacts are gitignored under `images/out/` and `images/.cache/`. The
test-only unpinned mode of "Test-only unpinned archive mode" below uses the
separate `images/out-unpinned-test/` and `images/.cache-unpinned-test/`.

## Host requirements

The build runs on a KVM-capable Linux host. The approved test VM base already has
`qemu-system-x86 qemu-utils passt debootstrap squashfs-tools e2fsprogs cpio
zstd postgresql`; the base setup adds `sudo git curl ca-certificates jq make
xz-utils util-linux gdisk dosfstools systemd-container`. The image build adds
exactly three packages: `python3` (mkosi 27.1 needs Python >= 3.9),
`python3-pefile` (mkosi's `KernelType.identify` imports `pefile` in the host
Python sandbox even with `Bootable=no`, because the default `SplitArtifacts`
copies the kernel out of the directory image), and `mtools` (part of mkosi's
disk/ESP tooling; this config uses `Format=directory`, so it is not normally
exercised). An audit of mkosi 27.1's modules found no other non-stdlib Python
import reached by this build. The image runner does not install host packages.
`build.sh` preflights `import pefile` with the exact interpreter mkosi's
`python_binary` will use, before sudo, downloads or any build work. It pins
that interpreter via `MKOSI_INTERPRETER` (must resolve under `/usr`, matching
mkosi's fallback rule) and preserves it across sudo; a drifted base fails
immediately with the `python3-pefile` install hint.

The build deliberately does **not** use the distribution's mkosi package. It
downloads the pinned mkosi source tarball, verifies its sha256 and runs
`bin/mkosi` from `images/.cache/`. Nothing is installed system-wide.

The build must run as root (or through passwordless `sudo`): `mkfs.ext4 -d`
must preserve `uid/gid 1000` inside `root.qcow2`, and the published artifacts
are chowned back to the invoking user. The QEMU smoke test runs unprivileged
and is the only part that touches `/dev/kvm` and `/dev/vhost-vsock`.

## Pins

`images/pins.sh` is the place tool and input pins live; `build.sh` asserts
that the duplicate `Snapshot=` in `images/mkosi.conf` matches it.
`images/version` names the output directory and the manifest id; bump it when
content changes (current content: `ubuntu-24.04-v1`). A published directory is
never replaced: `build.sh` refuses an existing version and `publish.sh` uses an
atomic no-clobber rename.

| Input | Version | Upstream | sha256 |
|---|---|---|---|
| mkosi | 27.1 | `https://github.com/systemd/mkosi/archive/refs/tags/v27.1.tar.gz` | `33520d662e151d6c43e910c3840ac971fd6023e4488d9c10599e199a07f77d8b` |
| Ubuntu snapshot | `20261108T182718Z` | `https://snapshot.ubuntu.com` | recorded in `packages.json` |
| Go | 1.27.3 (from `go.mod`) | `https://go.dev/dl/go1.27.3.linux-amd64.tar.gz` | `65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40` |
| Node.js | 24.22.2 | `https://nodejs.org/dist/v24.22.2/node-v24.22.2-linux-x64.tar.xz` | `48cc0e21e048790e54c0cf5749c2725005313d652651c40b7a21feb9697aaf75` |

Ubuntu packages come from a pinned `snapshot.ubuntu.com` snapshot
(`UBUNTU_SNAPSHOT` in `images/pins.sh`, also `Snapshot=` in
`images/mkosi.conf`; Ubuntu 24.04's apt enables snapshots automatically).
`noble`, `noble-updates`, `noble-security` and `universe` are used. The exact
installed package set and versions are recorded in `packages.json` (mkosi's
package manifest) next to every image.

## Layout

`images/out/<version>/` contains exactly these fixed filenames, never host
paths:

| File | Meaning |
|---|---|
| `kernel` | Ubuntu `vmlinuz-<kver>` for direct boot |
| `initrd` | Ubuntu `initrd.img-<kver>`, gzip, `MODULES=most` |
| `root.qcow2` | whole-disk ext4 image, no partition table, virtual size 10 GiB by default |
| `manifest.json` | id, distribution, toolchain, disk metadata and artifact hashes |
| `packages.json` | mkosi package manifest (JSON) for reproducibility records |

`manifest.json` schema (version 1):

```json
{
  "schemaVersion": 1,
  "id": "ubuntu-24.04-v1",
  "createdAt": "2026-11-08T00:00:00Z",
  "sourceDateEpoch": 1735689600,
  "distribution": {"id": "ubuntu", "version": "24.04", "codename": "noble", "architecture": "amd64",
                   "snapshot": "20261108T182718Z"},
  "kernel": {"version": "6.8.0-147-generic"},
  "root": {"filesystem": "ext4", "format": "qcow2", "diskGiB": 10,
           "virtualSizeBytes": 10737418240,
           "growth": "whole-disk ext4 resized online by bbraft-grow-root.service before bbraft-guest.service (growpart first when root is a partition)"},
  "toolchain": {"go": "1.27.3", "node": "24.22.2", "podmanPackage": "4.9.3+ds1-1ubuntu0.1"},
  "build": {"mkosi": "mkosi 27.1", "durationSeconds": 300},
  "artifacts": {
    "kernel": {"sha256": "...", "size": 123},
    "initrd": {"sha256": "...", "size": 123},
    "root.qcow2": {"sha256": "...", "size": 123},
    "packages.json": {"sha256": "...", "size": 123}
  }
}
```

The SSH identity and growth contract is expressed by the image contents and
the in-guest check, not by new manifest fields: schema version 1 and the
node/verifier's decoded fields are unchanged, so an existing node accepts an
image v1 directory and an older published image still verifies. SSH paths for
`AddSSHKey` consumers are recorded in "SSH host identity and access" below.
A test-only unpinned manifest additionally carries
`"snapshot": "unpinned"` and an informational `"mirror"` URL in
`distribution`; the node decodes neither field, and the release publisher
refuses either test marker ("Test-only unpinned archive mode" below).

`toolchain.podmanPackage` is the dpkg package version read from the guest
`/var/lib/dpkg/status` at build time, not a runtime `podman --version` query
(the post-mkosi chroot may not have `/proc`).

`images/verify.sh <image-dir>` checks the schema, the exact artifact set, every
hash and size, that `packages.json` is JSON, and that `root.qcow2` reports
`format: qcow2` with no backing file and a virtual size matching the manifest.
For a test-marked image it also requires both reserved markers (the id suffix
and `distribution.snapshot: "unpinned"`) plus a non-empty http(s)
`distribution.mirror`; ordinary and legacy manifests are unchanged. The node
repeats the integrity checks when it installs an image (A2).
`make -C images check-manifests` verifies every built image directory under
`out/` (`OUT=<dir>` overrides it, including paths containing spaces), skips a
missing/empty/non-image output directory successfully, and stops at the first
verify failure so a later good directory cannot mask it.
`images/check_manifests_test.sh` reproduces both pre-fix failures with
fixtures and rejects a non-qcow2 root.

## Disk and root growth

`root.qcow2` is a whole-disk ext4 filesystem with no partition table:

- QEMU attaches it as a virtio-blk disk; the recommended kernel command line
  is `console=ttyS0,115200 root=/dev/vda rootfstype=ext4 rw rootwait=1`.
- The base virtual size is 10 GiB (`BBRAFT_IMAGE_DISK_GIB`), which is exactly
  `nodetest`'s default 10 GiB disk. A2 may therefore keep the suite defaults.
- A node creates a per-VM qcow2 overlay whose virtual size is the requested
  disk size and grows it with `qemu-img resize` when the sandbox asks for more
  than the base. The guest runs `bbraft-grow-root.service` at every boot, so
  the filesystem fills the overlay before the guest agent is ready.
- Growth handles both layouts. If the root device is a partition (e.g.
  `/dev/vda1`), the service runs `growpart` on the parent disk first; growpart
  reports `NOCHANGE` (exit 1) when the partition already fills the disk, which
  is not an error. It then runs `resize2fs` on the root device, which is an
  online grow for ext4 and a no-op on an already-full filesystem. A whole-disk
  root (this image) has no parent, so growpart is skipped and only
  `resize2fs` runs. A real growpart or resize2fs failure fails the unit.
- `bbraft-guest.service` has `Requires=bbraft-grow-root.service` and `After=`
  that unit, so guest readiness cannot precede growth; a failed grow is a
  failed boot rather than a smaller filesystem.
- The smoke test boots a 12 GiB overlay, grows the stopped overlay to 14 GiB
  and boots it again; each boot fails unless the guest filesystem is within 5%
  of the current device size (ext4 metadata) and the second boot's device is
  at least the grown size, so the grow path is exercised end to end and after
  resume.

## SSH host identity and access

The image ships `openssh-server`, `openssh-client` (for `ssh-keygen` and
`ssh-keyscan`) and `openssh-sftp-server` (SFTP/SCP), but **no host key**:

- `bbraft-ssh-host-key.service` (`Before=ssh.service ssh.socket ssh@.service
  bbraft-guest.service`, enabled) runs `/usr/local/sbin/bbraft-ssh-host-key`
  at every boot, so the key exists before sshd starts **and before the guest
  agent can report readiness**. The guest agent orders after the key unit and
  wants it, but does not require it: a key-generation failure leaves
  `AddSSHKey` UNAVAILABLE while Exec/files keep working. The
  script removes any stale key in `/run/ssh`
  or `/etc/ssh`, generates a fresh ed25519 key at
  `/run/ssh/ssh_host_ed25519_key` (0600 root) with its public part at the
  canonical `/run/ssh/ssh_host_ed25519_key.pub` (0644), and leaves `/run/sshd`
  for privilege separation. `/run` is tmpfs, so the private key never reaches
  the image, the sandbox disk or a snapshot.
- **Canonical paths for `AddSSHKey` consumers (A8):** the guest returns the
  contents of `/run/ssh/ssh_host_ed25519_key.pub`; the private key is
  `/run/ssh/ssh_host_ed25519_key` and must never be read out. Both paths are
  fixed; `BBRAFT_SSH_KEY_DIR`, `BBRAFT_SSH_PRIVSEP_DIR` and
  `BBRAFT_SSH_ETC_DIR` exist only for the fast shell test.
- `/etc/ssh/sshd_config` names exactly that one `HostKey` (which disables
  sshd's compiled-in `/etc/ssh/ssh_host_*` defaults), sets
  `HostKeyAlgorithms ssh-ed25519`, public-key authentication only
  (`PasswordAuthentication no`, `KbdInteractiveAuthentication no`,
  `PermitRootLogin no`, `AllowUsers user`), `StrictModes yes`, and
  `AuthorizedKeysFile .ssh/authorized_keys`. `ssh.service` is enabled and
  `ssh.socket` is disabled so every sshd entry point is ordered after the key
  unit.
- The user's `~/.ssh/authorized_keys` is not baked in. The node installs one
  key through `AddSSHKey` (A8), which writes to the sandbox disk; it survives
  stop/start because `/home/user` is on the disk. The node supplies the
  externally reachable host and port, and publishes the current host key
  through the API.
- **Client-visible change on resume:** because the key is generated per boot,
  a stopped sandbox started again (resumed from its disk) presents a different
  ed25519 host key. Clients must not pin the old key; they should trust the
  key published by the API for the current boot (the frozen proto keeps
  `SSHAccess.host/port` as the reachable endpoint). Until the key exists the
  `AddSSHKey` response reports `503 ssh_host_key_unavailable`/UNAVAILABLE
 , so a client never caches a key from a previous boot.
- The in-guest `bbraft-toolchain-check` proves sshd is enabled and active, the
  key is valid ed25519 with 0600 root ownership, no key exists in `/etc/ssh`,
  the effective `sshd -T` config matches the contract, port 22 advertises the
  same fingerprint (via `ssh-keyscan` on loopback), an sshd restart within the
  boot keeps the key, the live systemd graph shows the guest agent Requires
  growth and is ordered after the key unit while the key unit is Before sshd
  and the agent, and `growpart` is installed. It prints
  `PASS sshd host key ED25519 <SHA256 fingerprint>`, which the smoke harness
  compares across two boots to prove rotation. The image contains no
  authorized key, so the listening check cannot authenticate.

## Guest networking (TAP mode vs user-mode)

The image configures its NIC in exactly one of two ways, chosen by the node's
kernel command line; nothing in the image changes between them.

- **QEMU user-mode networking** (the explicit dev/test fallback,
  `-node.network-mode=user`): no `ip=` parameter. The image's
  `20-bbraft.network` DHCP configuration is unchanged, which is what the
  unprivileged KVM tests and QEMU slirp need.
- **TAP mode** (production): the node passes the static form
  `ip=<guest>::<gateway>:<netmask>:::<off>[:<dns0>[:<dns1>]]`.
  `bbraft-network-config.service` runs before `systemd-networkd.service` and
  writes `/run/systemd/network/10-bbraft-static.network`, ordered before
  `20-bbraft.network`, with `DHCP=no`, the address, the gateway and the
  configured public resolvers (networkd publishes them to
  `systemd-resolved`). The kernel `ip=` parameter and networkd therefore agree
  on the same address, and networkd keeps it rather than running a DHCP client
  for a server the node never chose.
  - The parser is strict and shell-only: kernel data is never evaluated,
    globbed or passed through `sh -c`. Exactly one `ip=` is accepted with an
    empty server/hostname/device, `autoconf=off`, plain IPv4 addresses, a
    contiguous `/8`..`/30` netmask, distinct usable unicast guest and gateway
    addresses (never loopback, unspecified, multicast, reserved or the limited
    broadcast address, and never the network/broadcast address of the subnet),
    and at most two unicast resolvers outside the guest subnet.
  - Malformed input **fails closed**: the generator writes a matching unit
    with `DHCP=no`, `IPv6AcceptRA=no` and `LinkLocalAddressing=no`, so networkd
    configures no address, DHCP or RA of its own instead of guessing or falling
    back to DHCP, and it logs the reason to the journal. The boot still
    completes, so the node can report and an operator can inspect the sandbox.
  - A **real failure** (unreadable command line, an unwritable
    `/run/systemd/network`, a failed atomic replace) is different: the unit
    exits non-zero and the `systemd-networkd.service.d` drop-in makes networkd
    `Requires=`+`After=` it, so networkd is not started and the image's DHCP
    unit cannot take over. The failed generator unit is visible in the journal,
    and the guest agent keeps serving over vsock, so the sandbox remains
    observable.
  - The stock `systemd-network-generator.service` is masked in the image, and
    the mask is not best-effort: two components parsing `ip=` could disagree
    about the address, gateway or DNS, so the build fails rather than leave
    both enabled. Exactly one component owns the kernel command line.
  - The generated unit contains no secret: only the guest address, gateway and
    the public resolvers the node already passes on the kernel command line.
- Fast regressions: `images/net_config_test.sh` runs the real generator with
  injected command lines (`BBRAFT_CMDLINE_FILE`, `BBRAFT_NETWORK_DIR` are
  test-only overrides) and covers both modes plus the malformed table. The
  real boot path is exercised by the TAP-mode KVM acceptance run.

## Build steps (`images/build.sh`)

1. Validate host tools and every input that later feeds privileged arithmetic
   or deletion: image id is one safe component (`[a-z0-9][a-z0-9._-]`, max 64),
   `BBRAFT_IMAGE_DISK_GIB` is canonical digits 1..1024, `SOURCE_DATE_EPOCH` is
   canonical digits 1..4102444800, and the output/cache roots are not `/`.
   Length is checked before any shell integer comparison, so huge or
   leading-zero values fail cleanly. Refuse an existing
   `images/out/<version>/` before any expensive work; published versions are
   immutable. `BBRAFT_BUILD_START`, when supplied, is canonical digits and
   bounded before the duration arithmetic. Before sudo, `build.sh` asserts
   `pins.sh` `GO_VERSION` equals the `go` directive in `go.mod`, resolves
   `GO` (bare name, mise shim or explicit path) through its `GOROOT` to the
   real absolute `bin/go`, and requires `GOTOOLCHAIN=local go env GOVERSION`
   to match; it preserves that absolute path across sudo so secure_path cannot
   select an older Go. After the sudo re-exec it re-validates the inputs and
   re-runs both preflights. It also prints the host
   `qemu-system-x86_64 --version` and `qemu-img --version` read-only (no QEMU
   launch) for review-log evidence.
2. Fetch the pinned mkosi tarball into `images/.cache/`, verify sha256 and run
   `bin/mkosi` with `Format=directory`, the pinned `Snapshot=` and
   `images/mkosi.conf`. `mkosi.conf` sets `BuildSources=` to the empty string:
   the build does not mount the host source tree into the sandbox (the
   `.chroot` postinstall script is bound explicitly), which also keeps the
   workspace/cache under `images/.cache/` outside any configured source
   directory. Before `mkosi build`, `images/mkosi_config_check.py` imports the
   pinned mkosi source and calls its own
   `check_workspace_directory()` on the parsed exact build argv, so a
   workspace/source regression fails before packages or image files are
   touched. `make -C images mkosi-config-check` runs the same check when the
   pinned mkosi is cached (`MKOSI_BIN=` overrides the path).
3. Run `images/mkosi.postinst.chroot` inside the image (the `.chroot` suffix is
   required by mkosi 27.1): create `user` (uid/gid 1000, HOME `/home/user`,
   passwordless sudo), `/home/user/.ssh` (0700, empty), subuid/subgid, DHCP via
   systemd-networkd/resolved plus `systemd-networkd-wait-online`, VC/adapter
   modules, guest/grow/SSH-key units, timezone UTC, then explicitly
   `update-initramfs -c` for the installed kernel with `MODULES=most`. It also
   removes any package-generated `/etc/ssh/ssh_host_*`, enables
   `bbraft-ssh-host-key.service` and `ssh.service`, disables `ssh.socket`, and
   validates the committed `sshd_config` with `sshd -t` against a throwaway key
   that it then deletes.
4. Build `cmd/bbraft-guest` with the host Go (`CGO_ENABLED=0`) and install it
   as `/usr/local/bin/bbraft-guest`, driven by the unit in
   `images/mkosi.extra/`.
5. Install the pinned Go and Node tarballs after checking their sha256, add
   `/etc/profile.d` PATH entries and `/usr/local/bin` symlinks.
6. Verify the boot-critical drivers with `check_kernel_modules.sh`: a driver
   must be a real module in the initrd/modules tree or proven builtin by
   `CONFIG_<symbol>=y`/exact `modules.builtin`; absent everywhere fails. On
   Ubuntu `6.8.0-149-generic`, `CONFIG_VIRTIO_BLK/PCI/NET=y` while vsock and
   its virtio transport are modules. `check_kernel_modules_test.sh` covers
   module, config-builtin, modules.builtin-builtin and missing cases,
   including the old initrd-only rejection.
7. `truncate` + `mkfs.ext4 -d` the image root, `e2fsck -fn`, convert to qcow2
   with `qemu-img`, check it, write `manifest.json`, run `verify.sh`, then
   `publish.sh` renames the staged version directory into place. Before mkfs,
   `build.sh` rejects any `/etc/ssh/ssh_host_*` file or a baked
   `authorized_keys` and requires sshd, ssh-keygen, the sftp server, the sshd
   config and all three new unit/script files. The stage leaf is the image id
   (inside a temporary parent) so the verifier's basename/id check is
   exercised on the real publish path. `publish.sh` takes an atomic `mkdir`
   lock, re-checks that the destination does not exist and only then performs
   a same-filesystem `mv`; an existing version and its sentinel contents are
   never touched. `images/publish_test.sh` covers no-clobber, sentinel
   survival, unsafe ids, mismatched manifest ids and lock serialization
   without root or QEMU.

Environment knobs: `BBRAFT_IMAGE_OUT`, `BBRAFT_IMAGE_CACHE`,
`BBRAFT_IMAGE_VERSION`, `BBRAFT_IMAGE_DISK_GIB`, `SOURCE_DATE_EPOCH`.
The test-only mode adds `BBRAFT_IMAGE_TEST_UNPINNED`,
`BBRAFT_IMAGE_TEST_OUT` and `BBRAFT_IMAGE_TEST_CACHE` (see its section).

## Guest contents

- Ubuntu 24.04, systemd, `bash -lc` as `user` (uid/gid 1000), HOME
  `/home/user`, passwordless sudo. The image includes the real `apt` package
  (mkosi itself uses the host apt, so the guest needs it explicitly for
  postinst cleanup and later use) and `tzdata` for UTC.
- `bbraft-guest.service`, enabled, root (Freeze/Thaw need `CAP_SYS_ADMIN`),
  AF_VSOCK port 10789 with the approved A1.1 defaults (`-guest.uid/gid 1000`,
  `-guest.home /home/user`, `-guest.tmp /tmp`, `-guest.freeze-lease 2m`,
  `-guest.vsock-cid any`; `BBRAFT_GUEST_*` fallbacks). It requires and is
  ordered after `bbraft-grow-root.service`, is ordered after
  `bbraft-ssh-host-key.service`, and is ordered after
  `systemd-tmpfiles-setup.service` and `network-online.target` (the network
  ordering is best-effort so a guest without a route still serves vsock). A
  failed grow therefore prevents readiness; the smoke test additionally
  asserts the root grew to the requested disk and that DHCP and DNS work.
  `/etc/bbraft/guest.env` sets
  `BBRAFT_GUEST_ARGS=-guest.vsock-port 10789`; other arguments can be appended
  there. The guest holds no platform credentials.
- `bbraft-grow-root.service` (oneshot, enabled) grows a partition with
  `growpart` when needed, then resizes the root filesystem to the block
  device. `bbraft-guest.service` has `Requires=` and `After=` this unit.
- `bbraft-ssh-host-key.service` (oneshot, enabled) generates the per-boot
  ed25519 host key in `/run/ssh`; `ssh.service` (openssh-server) is enabled
  with `ssh.socket` disabled. See "SSH host identity and access" above.
- systemd-networkd DHCP on `en*`/`eth*`, systemd-resolved, systemd-timesyncd,
  with `systemd-networkd-wait-online.service` enabled.
- `virtio_blk`, `virtio_pci`, `virtio_net`, `vsock` and
  `vmw_vsock_virtio_transport` available; the first two are in the initrd.
- Go 1.27.3 (`GOTOOLCHAIN=local`), Node.js 24.22.2, `build-essential` (gcc,
  make), git, curl, ca-certificates, jq, Podman 4.9.x with uidmap,
  slirp4netns, fuse-overlayfs and passt for rootless operation, OpenSSH server
  and client with sftp-server, and `cloud-guest-utils` for growpart.
- `bbraft-toolchain-check` runs the in-guest acceptance checks below.
- First boot is clean: `/etc/machine-id` empty, `/var/lib/dbus/machine-id`
  symlinked, systemd random seed removed, hostname `bbraft-guest`, timezone
  UTC, no SSH host key, and no `authorized_keys`. No coding agents and no
  credentials are preinstalled.

The in-guest check, `bbraft-toolchain-check`, verifies uid/gid/HOME,
passwordless sudo, Go/Node/gcc/make/git/curl/jq versions, the five modules,
a DHCP address (bounded 60 s retry) and DNS resolution, that ext4 fills the
block device within 5% (ext4 inode/group metadata), sshd's enabled/active
units, the per-boot ed25519 host key (0600 root, no `/etc` key, effective
`sshd -T` policy, loopback `ssh-keyscan` fingerprint equal to the key file,
key stable across an sshd restart), `growpart` availability, rootless Podman,
and an offline `podman import` + `podman run --network=none` using the image's
static busybox. Geometry is read as uid 1000 via `statfs` and
`/sys/class/block/<dev>/size`, not root-only `blockdev`/`dumpe2fs`. It prints
versions and paths only, never the caller's environment or a private key; the
host key fingerprint it prints is public and is what the node publishes.

## Smoke test (`images/smoke.sh`)

Runs unprivileged and starts QEMU directly (no libvirt):

```sh
make -C images smoke
images/smoke.sh --image-dir images/out/ubuntu-24.04-v1 --disk-gib 12 --cid 42 --keep
make -C images smoke-unpinned-test  # explicit disposable test image selection
```

The default selection is always the pinned `images/out/<version>` image; the
test target passes `--image-dir images/out-unpinned-test/<version>-unpinned-test`
explicitly. It verifies the manifest first, creates a 12 GiB qcow2 overlay on
`root.qcow2`, then boots that same overlay **twice** with `q35,accel=kvm`,
`-cpu Haswell-noTSX[,+vmx],enforce`, `-nodefaults`, 2 vCPU / 2 GiB,
virtio-blk, virtio-net (QEMU user networking), virtio-rng, a unique
`vhost-vsock-pci` CID, serial to a file, and `-sandbox on`. `--single-boot`
skips the second boot for quick manual runs; the acceptance run uses both.
The Go client at `images/smoke/` connects over AF_VSOCK
(`github.com/mdlayher/vsock` v1.4.0) and exercises Ping, Exec echo and
uid/HOME, cwd/env override, PutFile + Exec read + ownership + sha256, timeout
and explicit RPC cancellation (the setup must write an exact child-ready
marker and a canonical numeric PID; a spawned background child must die with
the group and must never write its completion marker, and a missing/malformed
setup is a failure, never a pass; a separate Exec checks this), and the
in-guest toolchain/rootless-Podman/root-growth/SSH script. Each boot ends with
`sync` and a clean guest `systemctl poweroff`; both must exit 0, and only
transport loss while the VM is actually going down is tolerated (the harness
still waits for QEMU to exit with status 0). SIGTERM/SIGKILL are only the
failure-path cleanup, so the next boot starts from a consistent disk. Boot
labels 1 and 2 gate the cross-boot checks: boot 1 seeds a test
`authorized_keys` as the sandbox user, the harness grows the stopped overlay
from 12 to 14 GiB, boot 2 requires the key to persist unchanged with
`user 0600`, the root device/filesystem to have grown, and then removes the
test key; the two boots must print different ed25519 host key fingerprints.
The Ping line
includes `boot_ready_ms` measured from QEMU start for milestone evidence
(observational only). The cancel path
waits for the exact `bbraft-smoke-child-ready` line, not arbitrary stdout.
The client strictly enforces the nodev1 frame contract: one started(pid>0),
stdout/stderr only between started and exit, one final exit, no unknown
frames, and never treats a nil exit code as success. `images/smoke/stream_test.go`
has scripted-stream regressions for each violation and cancels the
per-command context on early stream errors. The client uses
`guestv1.NewGuestClient`; its module additions are coordinated by the lead
with the guest package.

Cleanup uses the exact QEMU PID it started for each boot: SIGTERM, ten
seconds, then SIGKILL. On failure it prints the last 200 serial lines and
QEMU stderr for every boot and keeps the temporary directory; on success it
removes it unless `--keep` is given. Missing `/dev/kvm`, `/dev/vhost-vsock`
or their write permission is a hard failure, not a skipped pass.

The CPU policy is the node's, not an image-local one: `images/smoke.sh`
builds the smoke client and runs its `-resolve-cpu` mode, which calls the
shared `internal/node/qemu` helpers (`DetectHostCPU` + `ResolveCPU`,
`DefaultCPUModel = Haswell-noTSX`). The production model is fixed with
`enforce` always (QEMU's default policy would warn/filter an unsupported
feature and still boot), and the harness never falls back to `-cpu host`,
`-cpu max` or TCG. A test-only test override is explicit: the rebuilt base
exports `BBRAFT_TEST_CPU_MODEL=host`, each wrapper reads it once at the entry
boundary, and forwards `-cpu-model host` plus the deliberate internal
`-cpu-model-test-host` opt-in to the Go client. The client then calls
`qemu.ResolveTestCPU`, which accepts exactly `host` and otherwise shares the
policy; the override applies to both the early CPU-only probe and the real
smoke QEMU argv. `max` is always rejected, an unset variable keeps the
production default, and `host` without the opt-in is refused. With the test
host model the guest CPUID vendor and features follow the host, not the fixed
Intel model; the policy still appends no `+svm` on an AMD host. Nested mode is
separate: `auto|on|off`, default `auto` through
`--cpu-nested`/`BBRAFT_SMOKE_CPU_NESTED`. In phase 0.5 the fixed model is an
Intel model, so `auto` adds `+vmx` only on a GenuineIntel host where every
CPU advertises vmx; an AMD host gets no nested feature (no `+svm` — the
Intel CPUID vendor would make in-guest `kvm_amd` refuse nested
virtualization), `on` fails clearly on AMD or vmx-less Intel, and `off` never
adds one. `off` needs no host facts at all, and an `auto` detection failure
clears the partial facts and resolves with none (matching the node's
`qemu.Start`); the reason always goes to stderr so stdout stays
machine-parseable. The resolved feature is passed to the guest toolchain
check via `-expect-nested`, which proves it in `/proc/cpuinfo` and does not
hard-require vmx. Vsock CIDs must be
3..4294967294 (0xffffffff is the reserved `VMADDR_CID_ANY`) and ports must be
nonzero; both the shell wrapper and the Go client reject invalid values before
QEMU starts. The client waits for Ping with gRPC `WaitForReady` for the whole
timeout, and the in-guest check retries DHCP/DNS, so QEMU boot timing does not
race the assertions.

## CPU probe (`images/cpu_probe.sh`)

`make review` gates the expensive mkosi build behind a cheap, unprivileged,
CPU-only QEMU probe. The thin wrapper builds the smoke client and runs its
`-probe-cpu` mode, which:

1. resolves the CPU with the same shared helpers as smoke (above) and
   requires read-write access to `/dev/kvm` before launching anything;
2. starts `qemu-system-x86_64` with `-machine q35,accel=kvm`, the resolved
   enforced `-cpu`, `-S` (paused, nothing boots: no kernel, initrd or disk),
   `-nodefaults`, `-sandbox on`, and a private QMP unix socket, in its own
   process group;
3. proves real readiness through the shared `internal/node/qmp` client: a
   full greeting/capabilities handshake, a `SO_PEERCRED` check that the
   socket's peer PID is exactly the child it started, `query-status`
   accepting only `prelaunch` or `paused` with `running=false` and both
   fields present (QEMU 8.2.2 `qapi/run-state.json` semantics), and
   `query-cpus-fast` reporting exactly one fully-realized CPU (cpu-index,
   positive thread-id and qom-path all present) for the probe's `-smp 1`;
4. stops the child only through the verified peer: QMP `quit`, a bounded
   wait, then SIGTERM and SIGKILL to the child's process group, and proves
   the whole group is gone (`kill(-pgid, 0)` = `ESRCH`) — a reaped leader
   with a surviving descendant (one holding the inherited stderr past
   `cmd.WaitDelay`) is a cleanup failure, not a success. `quit` is never sent
   to a socket whose peer was not proven to be the child; a mismatched peer
   is only closed, and the probe fails.

The wrapper only parses arguments and builds the client into the gitignored
`images/.cache/cpu-probe/` cache, then `exec`s it, so the wrapper PID is the
Go process itself: `SIGINT`/`SIGTERM` reaches the client's
`signal.NotifyContext` directly (installed before QEMU starts), and the exit
status propagates unchanged. Cleanup uses its own context, so an interrupt
cancels readiness but still quit/kills/reaps the child; a signal that arrives
during cleanup is reported as an interruption, never as a success.

`PASS` is printed only after readiness is proven AND cleanup succeeded AND the
run was not interrupted. `images/cpu_probe_test.sh` exercises all of this with
a fake QMP server
(`images/fake_qmp.py`) and fake QEMU binaries: resolver matrix (Intel vmx,
AMD none-with-reason, on/off/degradation), argv shape, rejected states and
malformed replies, fatal child, alive-but-no-QMP timeout, wrong socket peer,
quit/TERM-ignoring escalation with reaping, the leader-exits-descendant
case, and SIGTERM during readiness and during cleanup (a Python driver starts
the wrapper in its own session and signals exactly that PID); the test's exit
trap independently kills exactly the recorded fake process groups. No real
QEMU is needed.

## Review targets and the KVM presence gate

`review-node` and `review-all` both build on `review-prereqs` (the repository
`make check`, the image-local `make check` and the CPU probe, each run
exactly once) and gate the expensive image build behind
`images/kvm_gate.sh`: every required top-level test name must appear as an
exact line in `go test -list` of `./internal/node` (whose exit status is
checked, so a compile failure also fails the gate) before `build.sh` runs —
a renamed or not-yet-published test fails the review instead of silently
matching nothing, because `go test -run` would otherwise exit 0 on an empty
match (and `go test -list` cannot match subtest patterns at all).
`kvm_gate_test.sh` verifies the gate itself with a fake `go` binary
(empty listing, ok-summary-only, partial match, similar-name prefix and
compile failure).

`review-node` (the A2 gate, no role tests) then builds the image, boots the
smoke, and runs the non-snapshot conformance subset plus the ownership and
reconnect regressions, verbosely. `review-all` (the full A3 gate) adds, all
before the image build: a freshly built `bbraft-node` binary and the
mandatory `bbraft_node` role review wrapper
(`infra/ansible/roles/bbraft_node/tests/controller.sh`), then after
build/smoke runs the full direct+link conformance including Snapshot and the
focused regressions (`TestKVMOwnershipAndDiskGrowth`, `TestKVMLinkReconnect`,
`TestKVMSnapshotIntegrityAndGrowth`, `TestKVMGuestAgentCrashDuringFreeze`,
`TestKVMNodeManagerRestartDuringFreeze`), verbosely. Exactly one root check
and one image build happen per invocation.

`review-node` (in `images/Makefile`) depends on `review-prereqs`, so the
image is built and smoke-tested once, then runs A2's real-KVM conformance and
ownership/reconnect tests in the same VM. Those tests use
`BBRAFT_TEST_KVM=1`, `BBRAFT_TEST_IMAGE_DIR=<images>/out`, and a short
large-disk temporary directory for both `TMPDIR` and `GOTMPDIR`
(`KVM_TMP`, default `$HOME/.bbk`) to stay clear of tmpfs and the 107-byte
Unix socket path limit even after `t.TempDir`'s per-test suffix and the VM
work directory. `review` remains usable on its own.

## Test-only unpinned archive mode

The orchestrator authorized a test-only build from the live Ubuntu archive
while the pinned snapshot is unavailable, to unblock Milestone 2 image/KVM
gates. It is for throwaway VM builds only and is never a release path: the
pinned version, snapshot, mkosi/Go/Node pins, checksums, signature/keyring
verification and `SOURCE_DATE_EPOCH` are unchanged, and only the Ubuntu apt
snapshot selection differs for that one build.

- **Opt-in.** `BBRAFT_IMAGE_TEST_UNPINNED=1`; only unset or exactly `1` is
  accepted. The value is in build.sh's `sudo --preserve-env` list, so the
  re-exec keeps the explicit choice, and there is no flag or implicit path
  into this mode.
- **Id.** The normal id plus `-unpinned-test`
  (`ubuntu-24.04-v1-unpinned-test`). Suffixing is idempotent across the sudo
  re-exec, so an explicit `BBRAFT_IMAGE_VERSION` that already carries the
  suffix is not doubled. A pinned build refuses an id with the marker, and a
  mid-string marker, a second marker before the suffix, or an id longer than
  64 characters fails before preflight and before any download.
- **Roots.** `images/out-unpinned-test` and
  `images/.cache-unpinned-test`, overridable with `BBRAFT_IMAGE_TEST_OUT` and
  `BBRAFT_IMAGE_TEST_CACHE`. build.sh canonicalizes existing symlink
  components (`readlink -m`) and rejects roots that equal or overlap either
  pinned root (or each other). The caches are isolated by mode, not only by
  image id, so an unpinned apt/mkosi cache can never steer a pinned build.
- **mkosi input.** The build passes `--snapshot ""` plus
  `--mirror http://archive.ubuntu.com/ubuntu`. In mkosi 27.1,
  `ConfigAction` records a CLI empty value by setting `<dest>_was_none`, and
  `ParseContext.finalize_value` then skips the value from `mkosi.conf` and
  treats the optional setting as explicitly reset; the resolved
  `Config.snapshot` is `None`. build.sh does not trust that by inspection:
  before `mkosi build` it runs `mkosi_config_check.py
  --expect-snapshot none --expect-mirror <mirror>` on the exact argv, which
  imports the pinned mkosi and asserts the resolved `Config` objects.
  `make mkosi-config-check` runs the same check for the pinned and test modes
  when the pinned mkosi is cached. The mirror is the primary archive mirror;
  mkosi keeps its normal `security.ubuntu.com` endpoint for security updates.
- **Manifest.** Schema version 1, `id`, `root` and the exact four-artifact set
  are unchanged. A test manifest adds
  `distribution.snapshot: "unpinned"` and `distribution.mirror`. The node
  image loader (`internal/node/storage`) decodes only `schemaVersion`, `id`,
  `root` and `artifacts`, so it installs the test image without a contract
  change; `images/unpinned_manifest_test.go` builds a genuine qcow2 with real
  artifact hashes and asserts the image actually appears in
  `storage.VerifyImages`.
- **Verification and release.** `images/verify.sh` accepts a test-marked image
  only when the id suffix and unpinned snapshot are both present and the
  mirror is a non-empty http(s) URL; ordinary/legacy manifests are unchanged.
  `images/publish.sh` refuses either reserved marker independently, with no
  environment bypass, and keeps publishing ordinary/legacy manifests.
- **Materialization.** `images/stage_test.sh` is the only mover for test
  images. It enforces the same safe id grammar and 64-byte bound as
  build/publish, requires the suffixed id and both markers, requires the
  destination parent to be exactly `IMAGE_TEST_OUT`, and performs a
  no-clobber same-filesystem rename under a `.lock`. Its canonical path
  policy is symmetric with build.sh: the test output must not equal or overlap
  the release output, the pinned cache or the test cache in either direction,
  and symlink aliases are resolved first; an ancestor of `images/out` can no
  longer receive a destination. There is no allow-unpinned option in
  `publish.sh`.
- **Smoke selection.** `make smoke` still boots `images/out/<pinned version>`;
  `make smoke-unpinned-test` sources pins.sh inside the recipe and passes the
  test directory as one shell variable, so spaces, quotes or shell-looking
  text in a legitimate override stay argv data rather than recipe code.

Exact repeatable VM commands (`VM_HOME` is the throwaway VM user's home;
nothing is retained on a real host):

```sh
VM_HOME=<throwaway-VM-user-home>
cd "$VM_HOME/bamboo-raft"
make -C images check
make -C images mkosi-config-check
BBRAFT_IMAGE_TEST_UNPINNED=1 make -C images image-unpinned-test
make -C images smoke-unpinned-test
BBRAFT_TEST_KVM=1 \
  BBRAFT_TEST_IMAGE_DIR="$PWD/images/out-unpinned-test" \
  TMPDIR="$VM_HOME/.bbk" GOTMPDIR="$VM_HOME/.bbk" \
  go test -race -v ./internal/node -run 'TestKVMConformance(Direct|Link)$' -timeout 60m
```

The test output is disposable: it is never uploaded, published or retained as
a shared base, and `make -C images clean`/removing `out-unpinned-test` and
`.cache-unpinned-test` removes it.

## Reproducibility and limits

- mkosi, Go, Node and the Ubuntu archive snapshot are pinned by version and
  sha256/ID. `Snapshot=20261108T182718Z` (also in the manifest and documented
  in the pins table) fixes repository contents; `packages.json` records the
  exact installed package versions.
- `SOURCE_DATE_EPOCH` fixes file timestamps inside the root tree, but the
  ext4 superblock/UUID and qcow2 metadata carry build time. Do not claim
  bit-for-bit reproducibility until two builds produce matching hashes.
- `Format=directory` deliberately avoids a partition table and bootloader;
  this image is for direct kernel boot only.
- Phase 0.5 has no jailer, image signing, object-store upload or inbound
  networking. The node verifies hashes locally; artifacts are published as a
  local directory.
- Rootless Podman on ext4 uses `fuse-overlayfs`; the acceptance check is
  offline by design. General outbound container networking still relies on
  slirp4netns/passt.

## Guest agent integration

The guest API is approved: service `bbraft.guest.v1.Guest`, with Ping, Exec
(streaming `nodev1.ExecEvent`), PutFile (streamed header/data, response
`nodev1.PutFileResponse`), and token-bound Freeze/Thaw. `/etc/bbraft/guest.env`
contains `BBRAFT_GUEST_ARGS`, empty because the approved defaults are exactly
what the image needs (AF_VSOCK port 10789, uid/gid 1000, HOME `/home/user`,
tmp `/tmp`, 2-minute freeze lease). The host smoke client in `images/smoke/`
uses `guestv1.NewGuestClient` over `github.com/mdlayher/vsock`; it compiles and
runs once the guest runner's generated `internal/proto/guestv1` stubs are
merged into this worktree. No other image change is needed for that merge.

The file API requires the sandbox user to be able to open both configured
roots (`/home/user` and `/tmp`) as directories. If a root is search-only (for
example mode `0111`), Exec keeps working for known paths — traversal needs only
`x` — but every file RPC fails with `INVALID_ARGUMENT` naming the logical root
and `permission denied`; the guest never runs file operations as root or falls
back to another inode. Keep both roots readable by the sandbox user (`0755`,
and `1777` for `/tmp`).
