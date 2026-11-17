# bbraft_node

Installs `bbraft-node`, the agent on one Debian 13 KVM host, from a
caller-built binary and a caller-supplied node token. The role installs QEMU,
the QEMU disk tools, `iproute2` and `nftables`, loads and persists the `tun`,
`vhost_net` and `vhost_vsock` modules (a module neither loaded nor built in
fails the play) and checks that `/dev/kvm`, `/dev/vhost-vsock`,
`/dev/vhost-net` and `/dev/net/tun` are character devices, prepares the root-owned directories of a **fresh v1
data root** and read-only image directories, installs verified guest image
versions, and renders a hardened systemd unit for the **root jail
supervisor**: every QEMU and `qemu-img` it starts runs jailed on its own uid
(docs/node.md, "Jailed QEMU").

The role is **not wired into `site.yml`** here: the shared playbook, inventory
and collection list belong to the lead. It never contacts a real host when run
through its tests.

Dependencies: `ansible-core` (2.22.0 tested) and `ansible.builtin` only. The
role-local filters use the Python standard library.

## Required variables

| Variable | Meaning |
|---|---|
| `bbraft_node_binary_src` **or** `bbraft_node_binary_url` | exactly one: a controller path to the built binary or a checksum-pinned HTTP(S) URL |
| `bbraft_node_binary_sha256` | required in both cases; 64 lowercase hex characters |
| `bbraft_node_token` **or** `bbraft_node_token_src` | exactly one: the raw token value or a controller file holding it |
| `bbraft_node_images` | nonempty list of controller paths to image version directories |
| `bbraft_node_network_prefix` | private guest IPv4 range (`/8` to `/30`), for example `10.42.0.0/16` |
| `bbraft_node_network_gateway` | the node's address inside that range; guests' default gateway |
| `bbraft_node_ssh_ingress_addr` | node address SSH ingress ports are published on (may be a tailnet address) |
| `bbraft_node_ssh_ingress_iface` | host interface ingress is bound to; must differ from the VM bridge |
| `bbraft_node_ssh_port_range` | inclusive SSH ingress port pool `min-max`, ports `1024` and above |
| `bbraft_node_jail_uid_range` | jail uid=gid pool `min-max`, `0 < min < max < 2^31`, used by no host account or group |
| `bbraft_node_jail_io_device` | `MAJ:MIN` of the whole disk holding the data and image dirs |
| one of `bbraft_node_jail_io_{read,write}_{bps,iops}` | at least one positive per-VM `io.max` rate; no hardware default is guessed |
| `hardening_vm_bridge_interfaces` (inventory) | the hardening role's forward allowance: must contain `bbraft_node_network_bridge` and must not contain `bbraft_node_ssh_ingress_iface` |

Optional jail inputs: `bbraft_node_jail_qemu_binary`
(`/usr/bin/qemu-system-x86_64`), `bbraft_node_jail_tool_binary`
(`/usr/bin/qemu-img`), `bbraft_node_jail_firmware` (Debian 13's
`/usr/share/seabios/bios-256k.bin`, `/usr/share/qemu/linuxboot_dma.bin`,
`/usr/share/qemu/kvmvapic.bin`) and `bbraft_node_jail_library_dirs` (`[]`,
the binary's Debian multiarch default).

There is no repository default for the binary, its checksum or the token. A
local source is checked before anything is copied, so a missing file or a
checksum mismatch leaves an installed binary untouched. An HTTP download uses
`get_url` with `checksum: sha256:<value>`. The installed copy is checked again
before the unit is written.

The token is written byte-for-byte (no added newline) to a regular file owned
by root, mode `0600` or `0400`, and at most 8192 UTF-8 bytes; its
task and assertions use `no_log`. `bbraft_node_token_src` is copied
byte-for-byte and must contain one nonempty token line after trimming. Both
secrets belong in a vault or the gitignored inventory, never in the repository
or the inventory example.

## Files and permissions

| Path (default) | Mode / owner | Content |
|---|---|---|
| `/usr/local/bin/bbraft-node` | `0755` root:root | caller-built binary |
| `/etc/bbraft-node-v1` | `0750` root:root | configuration |
| `/etc/bbraft-node-v1/node.token` | `0600` root:root | raw node token |
| `/var/lib/bbraft-node-v1` | `0700` root:root | lock, jail state, VM disks, QMP sockets, serial logs |
| `/var/lib/bbraft-node-v1/.bbraft-node-data-v1` | `0600` root:root | v1 data-root marker (`bbraft-node data root v1`) |
| `/var/lib/bbraft-node-v1/images` | `0555` root:root | published image versions |
| `/var/lib/bbraft-node-v1/images/<id>` | `0555` root:root | exactly the five files below, `0444` |
| `/etc/systemd/system/<service>.service` | `0644` root:root | hardened unit; `bbraft_node_unit_path` must be named `<bbraft_node_service>.service` |
| `/etc/udev/rules.d/60-bbraft-kvm.rules` | `0644` root:root | Debian's `kvm` group and `0660` on `/dev/kvm` and `/dev/vhost-vsock` (not relied on by the root supervisor) |
| `/etc/modules-load.d/bbraft-vhost-vsock.conf` | `0644` root:root | loads `tun`, `vhost_net` and `vhost_vsock` at boot |

An image version directory contains exactly `kernel`, `initrd`, `root.qcow2`,
`packages.json` and `manifest.json`. Each first install uses a unique hidden
`.staging-*` directory at the image root, outside the version directory; the
node ignores invalid image ids. Staging and binary candidates are removed in
an `always` block, and image publication uses `mv -nT` so a concurrent apply
can never replace a version that won first. Verification markers are never
written inside a version.

The role refuses weaker or class-shadowed modes: token `0600`/`0400`, config
`0750`, data `0700`, image directories `0555`, image files `0444`, the binary
`0755`, and unit/udev/module files `0644`. The service is not the image owner
or group, so exact world-readable/traversable masks are required rather than a
mode whose owner/group class shadows a missing world bit.

## Fresh v1 data root (Phase 0.5 upgrade)

Phase 1 never reuses a Phase 0.5 data dir. Before any task that changes the
host, `tasks/data-dir-check.yml` reads `bbraft_node_data_dir` without
following a symlink and accepts only:

- an absent path (created `0700` root:root, then the marker is written);
- the supervisor's own `0700` directory that is empty (the marker is
  written);
- the supervisor's own `0700` directory holding the exact marker
  `.bbraft-node-data-v1`: a regular, single-link, `0600` root:root file
  whose bytes are exactly `bbraft-node data root v1\n` (nothing changes).

The role-owned module `library/bbraft_node_data_root.py` does this on
descriptors without following a final symlink. `observe` (read-only) records
the state with the parent, root and marker identities; `acquire` acts only on
exactly that observation: an absent root is created by an exclusive `mkdirat`
(anything that appeared meanwhile is refused and never chowned or chmod'ed),
an empty root is marked by an exclusive `O_EXCL` no-follow create (a planted or
wrong marker is refused, never skipped), a current root is re-proven by root
and marker identity, and every path fsyncs the marker, the root and its
existing configured parent again so an earlier failed barrier is repaid.
`verify`, the last task before the service starts, re-proves the same
identities. An uncertain marker publication is preserved and refused; nothing
is ever removed.

Everything else is refused unchanged: a non-empty directory without the
marker (including root-owned legacy data), a foreign owner, group or mode, a
symlink, a non-directory, and a marker of any other bytes, type, mode, owner
or link count. The role never chowns, copies, migrates or deletes old data,
and it neither creates nor removes the old `bbraft-node` account. See
docs/node.md, "Upgrading from Phase 0.5".

On a real host (`bbraft_node_manage_service`) `tasks/host-checks.yml` also
reads, before the unit is written: systemd `>= 254` (`bbraft_node_systemd_min_version`
may raise that floor, never lower it); that no `getent` passwd or group id
lies inside the jail uid range; that the mount holding the data root (the
data dir itself when it exists, else its existing configured parent) is not
`nodev`, `nosuid` or `noexec`; and that the QEMU binary, `qemu-img` and
each firmware file are root-owned regular files only root can write. The
binary re-proves each at startup and fails closed.

## Unit and flags

`ExecStart` contains only the exact typed flags from
`cmd/bbraft-node/config.go`; every argv element is quoted by
`bbraft_node_systemd_exec_quote`. There is no `-node.id` line when
`bbraft_node_id` is empty: the binary uses the hostname, as A2.3 requires.

| Flag | Env fallback | Role default | Meaning |
|---|---|---|---|
| `-server.addr` | `BBRAFT_SERVER_ADDR` | `127.0.0.1:7701` | plaintext node port; `localhost`, loopback or private IP only |
| `-node.id` | `BBRAFT_NODE_ID` | empty (omitted; hostname) | node identity |
| `-node.token-file` | `BBRAFT_NODE_TOKEN_FILE` | `/etc/bbraft/node.token` | raw token file |
| `-node.data-dir` | `BBRAFT_NODE_DATA_DIR` | `/var/lib/bbraft` | writable state root |
| `-node.image-dir` | `BBRAFT_NODE_IMAGE_DIR` | `/var/lib/bbraft/images` | read-only image root |
| `-node.default-image` | `BBRAFT_NODE_DEFAULT_IMAGE` | empty (omitted) | first verified image when empty |
| `-node.cpu-model` | `BBRAFT_NODE_CPU_MODEL` | `Haswell-noTSX` | fixed named model; `host`/`max` refused |
| `-node.cpu-nested` | `BBRAFT_NODE_CPU_NESTED` | `auto` | `auto` adds `+vmx` only on Intel+vmx and is off on AMD (no `+svm` in phase 0.5); `on` on an unsupported host is a startup error; `off` disables it |
| `-node.boot-timeout` | `BBRAFT_NODE_BOOT_TIMEOUT` | `2m` | bound from create to guest Ping |
| `-node.capacity-vcpus` | `BBRAFT_NODE_CAPACITY_VCPUS` | `0` | `0` detects host CPUs scaled by the CPU overcommit ratio minus reserve |
| `-node.capacity-memory-mib` | `BBRAFT_NODE_CAPACITY_MEMORY_MIB` | `0` | `0` detects `MemTotal` scaled by the memory overcommit ratio minus reserve |
| `-node.capacity-disk-gib` | `BBRAFT_NODE_CAPACITY_DISK_GIB` | `0` | `0` detects free data-dir space scaled by the disk overcommit ratio minus reserve |
| `-node.cpu-overcommit` | `BBRAFT_NODE_CPU_OVERCOMMIT` | `1.0` | finite positive multiplier on detected host CPUs |
| `-node.memory-overcommit` | `BBRAFT_NODE_MEMORY_OVERCOMMIT` | `1.0` | finite positive multiplier on detected `MemTotal` |
| `-node.disk-overcommit` | `BBRAFT_NODE_DISK_OVERCOMMIT` | `1.0` | finite positive multiplier on the detected free space used by logical disk admission |
| `-node.reserve-vcpus` | `BBRAFT_NODE_RESERVE_VCPUS` | `1` | host vCPUs reserved |
| `-node.reserve-memory-mib` | `BBRAFT_NODE_RESERVE_MEMORY_MIB` | `1024` | host MiB reserved |
| `-node.reserve-disk-gib` | `BBRAFT_NODE_RESERVE_DISK_GIB` | `1` | host GiB reserved |
| `-node.network-mode` | `BBRAFT_NODE_NETWORK_MODE` | `tap` (validated; `user` refused) | production TAP networking |
| `-node.network-bridge` | `BBRAFT_NODE_NETWORK_BRIDGE` | `bbraft0` | per-node bridge carrying every VM TAP |
| `-node.network-prefix` | `BBRAFT_NODE_NETWORK_PREFIX` | required | private guest IPv4 range |
| `-node.network-gateway` | `BBRAFT_NODE_NETWORK_GATEWAY` | required | node address and guests' default gateway |
| `-node.network-resolvers` | `BBRAFT_NODE_NETWORK_RESOLVERS` | `1.1.1.1,9.9.9.9` | public IPv4 resolvers, at most two |
| `-node.network-management` | `BBRAFT_NODE_NETWORK_MANAGEMENT` | empty (omitted) | networks guests must never reach |
| `-node.ssh-ingress-addr` | `BBRAFT_NODE_SSH_INGRESS_ADDR` | required | address SSH ingress ports are published on |
| `-node.ssh-ingress-iface` | `BBRAFT_NODE_SSH_INGRESS_IFACE` | required | interface SSH ingress is bound to |
| `-node.ssh-port-range` | `BBRAFT_NODE_SSH_PORT_RANGE` | required | inclusive DNAT port pool `min-max` |
| `-node.jail-uid-range` | `BBRAFT_NODE_JAIL_UID_RANGE` | required | jail uid pool |
| `-node.jail-cgroup` | `BBRAFT_NODE_JAIL_CGROUP` | `/sys/fs/cgroup/system.slice/<service>.service/vms` (derived) | delegated jail cgroup |
| `-node.jail-qemu-binary` | `BBRAFT_NODE_JAIL_QEMU_BINARY` | `/usr/bin/qemu-system-x86_64` | trusted QEMU |
| `-node.jail-tool-binary` | `BBRAFT_NODE_JAIL_TOOL_BINARY` | `/usr/bin/qemu-img` | trusted qemu-img |
| `-node.jail-firmware` | `BBRAFT_NODE_JAIL_FIRMWARE` | the three Debian 13 files | trusted firmware |
| `-node.jail-library-dirs` | `BBRAFT_NODE_JAIL_LIBRARY_DIRS` | empty (omitted) | trusted library dirs |
| `-node.jail-io-device` | `BBRAFT_NODE_JAIL_IO_DEVICE` | required | whole disk `MAJ:MIN` |
| `-node.jail-io-{read,write}-{bps,iops}` | `BBRAFT_NODE_JAIL_IO_*` | `0` (omitted) | per-VM `io.max`; at least one positive |

`bbraft_node_cpu_features` (default `[]`) renders `-node.cpu-features` as a
comma-joined list of `+name`/`-name` QEMU feature toggles; `vmx`/`svm` are
refused (they belong to `bbraft_node_cpu_nested`). It is rarely needed: the
node already turns off nested VMX features the host lacks.

Quote `on` and `off` in YAML (`bbraft_node_cpu_nested: "on"`); unquoted YAML
1.1 booleans are rejected because the binary flag expects the literal strings.

`bbraft_node_server_addr` is validated exactly like `checkServerAddr`: wildcard,
public, link-local and non-literal hosts are rejected because the phase 0.5
link is plaintext (TLS is a later phase). Capacities are non-negative integers
no larger than the binary's resource types; `bbraft_node_boot_timeout` is
validated as a positive Go duration with int64 nanosecond range and then also
parsed by the real CLI in the tests.

Each overcommit ratio multiplies the detected host resource before capacity
is resolved: a capacity of `0` offers `floor(detected * ratio) - reserve`
and an explicit capacity must be `<= floor(detected * ratio)` instead of the
raw detection. `1.0` keeps strict no-overcommit behavior. At any ratio a
single VM's vCPUs never exceed the host CPU count, and the disk ratio scales
the logical-size bound live admission charges outstanding VM disks against.
Ratios must be finite positive numbers: booleans, `NaN`, infinities, zero,
negatives and non-numeric values are rejected rather than coerced. The
dev/test `-node.cpu-allow-host` flag is never emitted by this role.

The unit runs the root jail supervisor inside an exact boundary:

- No `User=`: root, with `CapabilityBoundingSet=` exactly `CAP_SYS_ADMIN
  CAP_SYS_CHROOT CAP_MKNOD CAP_SETUID CAP_SETGID CAP_SETPCAP CAP_CHOWN
  CAP_FOWNER CAP_DAC_OVERRIDE CAP_KILL CAP_SYS_PTRACE CAP_NET_ADMIN` (each
  tied to jail code; docs/node.md), empty `AmbientCapabilities=`,
  `NoNewPrivileges=true` and `SystemCallArchitectures=native`. QEMU and
  `qemu-img` inherit none of them: each jail drops all five sets.
- `Delegate=yes` and `DelegateSubgroup=supervisor` (systemd `>= 254`): the
  node lives in `<service>/supervisor` and owns `<service>/vms`.
  `ProtectControlGroups=false` with `ReadOnlyPaths=/sys/fs/cgroup` and
  `ReadWritePaths=<data dir> /sys/fs/cgroup/system.slice/<service>.service`
  make the delegated subtree the only writable cgroup path.
- `DevicePolicy=closed` with exactly `DeviceAllow=` `/dev/kvm rwm`,
  `/dev/vhost-vsock rw`, `/dev/vhost-net rw`, `/dev/net/tun rw`,
  `/dev/null rwm`, `/dev/urandom rwm` and `/dev/vsock r`. `/dev/vsock` is
  read-only for the node's one startup `VM_SOCKETS_GET_LOCAL_CID` ioctl,
  which keeps the host's own vsock CID out of guest CIDs (a missing device
  means nothing to exclude; any other probe error fails startup).
  `ProtectClock=true` makes systemd add a read-only `char-rtc r` of its own.
  The policy sits on the service cgroup, so it also bounds every jail; no
  jail's private `/dev` holds `/dev/vsock`.
- `KillMode=process` and `TimeoutStopSec=30` (longer than the node's own 20 s
  bounded shutdown) signal the node only. Jailed QEMUs survive a restart and
  are adopted by the next node process by their jail identity.
- No namespace restriction and no system-call filter: systemd's namespace
  filter answers `clone3` with `ENOSYS` (startup admission would fail), and
  QEMU would inherit a syscall filter on top of its own `-sandbox`.
  `ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, kernel/clock
  protections, `RestrictSUIDSGID`, `LockPersonality` and the
  `AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_VSOCK` family list remain.
- `TasksMax=infinity` allows the node and its QEMU threads/children.

A changed unit reloads systemd and restarts the node; a changed binary, token or
new image restarts it. Unchanged runs do neither.

`net.ipv4.ip_forward=1` is a host requirement the node verifies at startup and
fails closed on. With `bbraft_node_manage_service` the role persists it in
`/etc/sysctl.d/60-bbraft-node.conf` (`root:root`, `0644`) and applies it with
`sysctl --load=` of exactly that file, but only while the **live** ruleset
(`nft -j list ruleset`) proves the hardening role's forward filter. That
means exactly one `inet bbraft_filter` table whose only forward-hook chain is
`forward` (`type filter`, priority 0, `policy drop`), containing exactly one
`meta nfproto ipv4 iifname <bridge> accept` and one `oifname <bridge> accept`
per bridge in `hardening_vm_bridge_interfaces`, and nothing else. The list
must be nonempty, unique and valid, contain `bbraft_node_network_bridge` and
exclude `bbraft_node_ssh_ingress_iface`; it is never extended or repaired.
Input-chain rules, other tables and other families are not inspected.

The guard runs before the file is written, and again just before the apply.
A refusal (missing or different forward filter, extra or generic accept rule,
IPv6, wrong policy or priority, malformed or oversized nft output) fails the
run without touching the conf or calling `sysctl`; only a rule change between
the two reads can leave a conf written but unapplied. An unchanged conf is
reapplied when the runtime value is not `1` (drift), and the runtime value is
verified after every apply. Check mode reads and changes nothing.

## Images

`bbraft_node_images` lists controller directories. Before the first install the
role verifies the manifest (`schemaVersion` 1, id equal to the directory
basename, exactly the four artifacts, `qcow2`/`ext4` root metadata whose
`diskGiB` and `virtualSizeBytes` agree) and every artifact's size and sha256.
It copies into a unique hidden staging directory, re-hashes all five staged
files and compares the staged manifest to the intended version, makes the
files `0444` and the directory `0555`, and publishes with `mv -nT`. A failed
check, copy, target re-hash or concurrent winner leaves an existing version
intact.

Published versions are immutable. An existing id is reused only when its
manifest matches the supplied one and every published artifact still matches
its declared sha256 and size; same-size corruption stops the run without
rewriting anything. A different manifest stops the run instead of replacing a
version that VM disks may depend on. The role never deletes an image version.

## Example (private inventory, never committed)

```yaml
bbraft_node_binary_src: "{{ playbook_dir }}/../../bin/bbraft-node"
bbraft_node_binary_sha256: "<sha256 of bin/bbraft-node>"
bbraft_node_token: "<token from the vault>"
bbraft_node_server_addr: "127.0.0.1:7701"    # default; use a private address for a remote server
bbraft_node_images:
  - "{{ playbook_dir }}/../../images/out/ubuntu-24.04-v1"
# Required production networking; placeholders below, real values in the
# private inventory:
bbraft_node_network_prefix: 10.42.0.0/16
bbraft_node_network_gateway: 10.42.0.1
bbraft_node_ssh_ingress_addr: 203.0.113.10   # documentation address
bbraft_node_ssh_ingress_iface: example0
bbraft_node_ssh_port_range: 22000-22999
# Required jail inputs (placeholders):
bbraft_node_jail_uid_range: 900000-900999       # must not overlap host accounts
bbraft_node_jail_io_device: "MAJ:MIN"           # whole disk under data and image dirs
bbraft_node_jail_io_write_bps: 104857600        # at least one positive rate
hardening_vm_bridge_interfaces: ["{{ bbraft_node_network_bridge }}"]
# Optional:
# bbraft_node_default_image: ubuntu-24.04-v1
# bbraft_node_capacity_vcpus: 12
# bbraft_node_capacity_memory_mib: 49152
# bbraft_node_network_management: ["100.64.0.0/10"]
```

## Tests

Local only, localhost connection, temporary directories; no system package,
user, directory, unit or service is changed and no real host is touched:

```sh
cd infra/ansible
mise exec -- sh roles/bbraft_node/tests/run.sh
# A10 subset only (jail inputs, hardening pairing, root ownership policy,
# legacy/foreign data roots); run.sh includes it:
mise exec -- sh roles/bbraft_node/tests/run-a10.sh
```

`tests/test_forward_guard.py` unit-tests the forward guard filter and
`tests/ip-forward.sh` drives `tasks/ip-forward.yml` with fake `nft` and
`sysctl` programs first in `PATH` and a temporary conf (fresh, idempotent,
runtime drift, conf and mode drift, check mode, every refusal leaving a prior
conf byte-identical with no `sysctl` call, a changed second read, a failed
verify). It also renders the hardening role's real nftables template and loads
it with the real `nft` inside `unshare -rn` (an unprivileged user and network
namespace, never the host firewall), then runs the task against that live
ruleset; that part skips when such a namespace is unavailable unless
`BBRAFT_TEST_REQUIRE_ALL=1`. With root on a disposable VM the same task can
also use the real `sysctl`, whose `net.ipv4.ip_forward` is per network
namespace:

```sh
cd infra/ansible
mise exec -- ansible-playbook -i roles/bbraft_node/tests/inventory.yml \
    roles/bbraft_node/tests/ip-forward-render.yml -e out=/tmp/ipf.nft -e '{"bridges": ["bbraft0"]}'
sudo unshare -n sh -c 'nft -f /tmp/ipf.nft && sysctl -n net.ipv4.ip_forward \
    && '"$(mise exec -- which ansible-playbook)"' -i roles/bbraft_node/tests/inventory.yml \
        roles/bbraft_node/tests/ip-forward.yml -e ipf_conf=/tmp/ipf-test.conf \
        -e ipf_owner=root -e ipf_group=root && sysctl -n net.ipv4.ip_forward'
```

`tests/legacy-data.yml` builds each data-dir case in a fresh temporary
parent, runs the read-only recognition and the creation step, and compares
a no-follow snapshot of the whole parent (`tests/tree_snapshot.py`: inode,
uid, gid, mode, link count, size and byte or link-target sha256) before and
after: every refusal leaves it identical.

On a controller without Python >=3.13 or Ansible (for example a minimal
test VM), `tests/controller.sh <bbraft-node>` provisions a user-space pinned
uv 0.13.7, uv-managed CPython 3.13.12 and ansible-core 2.22.0 under
`~/.cache/bbraft-node-role/controller`, verifies each version, then runs
`run.sh` with `BBRAFT_NODE_BIN` set. It never uses apt/sudo, does not modify
the shared `infra/ansible/mise.toml`, and supports `--dry-run` for review.
`tests/controller-fake.sh` is an offline, bounded regression for that
provisioning flow and its relative-path/version handoff; `run.sh` includes it.

It runs the filter unit tests, `--syntax-check` for the full role and its
plays, a full render with exact flags/modes/hardening/image-install assertions,
`systemd-analyze verify`, idempotence and immutability checks, staged and
published corruption/checksum-swap regressions, the device policy task with
synthetic stat fixtures, no-clobber image publication with a concurrent-winner
fixture, and rejection cases asserting the intended failing stage
(public/link-local/wildcard addresses, aliased/`..`/`/` paths, conflicting
destinations, weak/class-shadowed modes, bad durations/capacities/manifests/
hashes, invalid overcommit ratios (boolean, NaN, infinity, zero, negative,
overflow, underflow, padded and non-numeric forms), token newline/byte
boundaries and duplicate image ids). Network checks cover a complete TAP
deployment, every missing required input, an attempted `user`-mode override,
non-private/narrow prefixes, dotted-netmask CIDR suffixes, gateways outside
or on prefix edges, too many/private/resolver-in-prefix addresses, management
overlap, loopback/unspecified/multicast/link-local/broadcast ingress
addresses, a bridge-equal ingress interface, invalid interface names, and
privileged/malformed/descending port ranges. Two release-guard negatives prove
that a disposable `-unpinned-test` image id and a `distribution.snapshot:
unpinned` manifest are each refused independently during validation, before
any image or unit target is touched; a pinned distribution block stays valid
and legacy manifests need no distribution field. It also checks
that verbose invalid-secret output never echoes a sentinel. When
`BBRAFT_NODE_BIN` points at a real built `bbraft-node`, the rendered
`ExecStart` argv is passed to that binary with `-h` at the end, so every emitted
flag name and value is parsed by the actual nodecli `flag` set rather than
compared with a handwritten list.

Not covered locally: installing packages on a real host, creating the system
user, applying udev rules to real devices and starting/restarting the service.
Those need a disposable Debian 13 host or test VM, run by the lead. Never run
this role in apply or check mode against a real inventory from here.
