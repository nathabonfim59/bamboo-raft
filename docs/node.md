# bbraft-node

`bbraft-node` is the agent on one KVM host. It owns QEMU VMs under
`-node.data-dir`, dials `bbraft-server` and serves `nodev1.Node` over that
connection. Nodes dial out: the server never opens connections to a node
and never holds node credentials. When the link drops
the node redials with backoff; running VMs are unaffected by reconnects.

A `PutFile` canceled after the client half-closes its upload may still commit;
callers that need certainty should re-stat the path.

## Requirements

Production (`-node.network-mode=tap`, what the Ansible role deploys) runs
the node as the **root jail supervisor**: every QEMU and every `qemu-img`
run happens in a jail on its own uid (see [Jailed QEMU](#jailed-qemu)). The
host needs:

- Linux x86-64 with KVM and these device nodes: `/dev/kvm`,
  `/dev/vhost-vsock`, `/dev/vhost-net` and `/dev/net/tun`.
- systemd 254 or newer (`DelegateSubgroup=`), cgroup v2 mounted unified at
  `/sys/fs/cgroup`, and no cgroup namespace around the node. The node proves
  at startup that it runs in `<service cgroup>/supervisor` of a
  systemd-delegated service cgroup and owns `<service cgroup>/vms`.
- `qemu-system-x86_64` and `qemu-img` (Debian `qemu-system-x86` and
  `qemu-utils`), root-owned and writable only by root, with the firmware
  files named by `-node.jail-firmware`.
- A jail uid range (`-node.jail-uid-range`) that no host account or group
  uses. The node parses `/etc/passwd` and `/etc/group` and refuses any
  overlap; accounts served only by other NSS sources are not seen, so keep
  the range outside every id your directory services hand out.
- The data dir on a filesystem mounted without `nodev`, `nosuid` or
  `noexec` (the jailer stages device nodes and the QEMU runtime under it),
  and the data and image dirs on **one whole disk**: `-node.jail-io-device`
  is that disk, bounded per VM by `io.max`. Multi-disk layouts are not
  supported in Phase 1.
- A CPU that supports the fixed `Haswell-noTSX` model; another fixed named
  model may be configured, but `host` and `max` are rejected because they
  tie a VM to one machine. Nested virtualization is exposed to guests on
  Intel hosts only; see `-node.cpu-nested`.
- `net.ipv4.ip_forward=1`: the node creates the bridge and TAPs, owns its
  nftables tables and NATs guest egress. It verifies IPv4 forwarding at
  startup and fails closed. The Ansible role persists it in
  `/etc/sysctl.d/60-bbraft-node.conf` and applies it only after the live
  ruleset proves the hardening role's drop-by-default forward filter for the
  configured bridges; without that filter it refuses before writing the file
  or applying the setting.
- Enough headroom for the configured reserve (see Capacity).

At startup, before it touches the network or offers capacity, the node runs
a fixed sequence and fails on any step: the read-only host proofs (root
supervisor, delegation, account range, IO device), the jail cgroup
preparation, **functional admission** (a real `clone3` with
`CLONE_INTO_CGROUP` and `CLONE_PIDFD` into an attempt-owned cgroup, and the
detached mount APIs in a fresh namespace; a kernel version string is not
evidence), and a **jailed CPU probe**: a bounded CPU-only QEMU in a real
jail proves the resolved `-cpu` option. The probe boots no guest and fails
startup with QEMU's own stderr, for example a missing
`CPUID.01H:ECX.pcid [bit 17]`. Root never runs an unjailed QEMU; the
unjailed host check and probe exist only in the dev/test user mode.

The dev/test user mode (`-node.network-mode=user`) still runs unprivileged
with QEMU unjailed; it refuses every jail flag (see
[Dev/test user mode](#devtest-user-mode)).

## Install with Ansible

The [bbraft_node role](../infra/ansible/roles/bbraft_node/README.md) provisions
Debian 13 for the root jail supervisor: a fresh v1 data root, the token,
verified image versions and a hardened systemd unit with exactly the
supervisor's capabilities, `Delegate=yes`, `DelegateSubgroup=supervisor` and
a closed device policy. Supply a built node binary and its SHA-256, a raw
node token, verified image version directories, the jail uid range, the IO
device and at least one IO rate. The role publishes images without
replacing an existing version. It is not wired into `site.yml`; the role
README lists its variables. Upgrading a Phase 0.5 node: see
[Upgrading from Phase 0.5](#upgrading-from-phase-05).

## Flags and environment

Every flag has a `BBRAFT_*` fallback: dots and dashes become underscores,
upper-cased (`-node.data-dir` ⇄ `BBRAFT_NODE_DATA_DIR`). An explicit flag
always wins; a malformed numeric or duration env value is a startup error
unless the matching flag is set.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-server.addr` | `BBRAFT_SERVER_ADDR` | `127.0.0.1:7701` | Control-plane node port, `host:port`. Plaintext, so the host must be `localhost`, a loopback IP or a private IP (see Transport). |
| `-node.id` | `BBRAFT_NODE_ID` | hostname | Node identity reported to the server. |
| `-node.token-file` | `BBRAFT_NODE_TOKEN_FILE` | empty | File holding the raw token; see Authentication. |
| `-node.data-dir` | `BBRAFT_NODE_DATA_DIR` | `/var/lib/bbraft` | Writable state root: lock, VM disks, sockets. |
| `-node.image-dir` | `BBRAFT_NODE_IMAGE_DIR` | `/var/lib/bbraft/images` | Read-only root of verified image versions. |
| `-node.default-image` | `BBRAFT_NODE_DEFAULT_IMAGE` | first verified image | Image id used when a create request names none. |
| `-node.cpu-model` | `BBRAFT_NODE_CPU_MODEL` | `Haswell-noTSX` | Fixed named QEMU CPU model, reported in `Info.cpu_model`. `max` refused; `host` only with `-node.cpu-allow-host`. |
| `-node.cpu-features` | `BBRAFT_NODE_CPU_FEATURES` | empty | Extra QEMU CPU feature toggles, comma-separated `+name`/`-name` (for example `-vmx-apicv-vid`), appended before `enforce`. At most 32; `vmx`/`svm` are refused (see `-node.cpu-nested`). |
| `-node.cpu-nested` | `BBRAFT_NODE_CPU_NESTED` | `auto` | Nested virtualization exposed to guests. `auto`: `vmx` only on GenuineIntel hosts exposing the required feature set, off otherwise (reason logged). `on`: requires it; a startup error when the host lacks it or is AMD. `off`: suppresses it. |
| `-node.cpu-allow-host` | `BBRAFT_NODE_CPU_ALLOW_HOST` | `false` | Dev/test-only opt-in: permits `-node.cpu-model=host` exactly; `max` and other casings stay refused. See below. |
| `-node.boot-timeout` | `BBRAFT_NODE_BOOT_TIMEOUT` | `2m` | Bound on one boot until the guest answers Ping. |
| `-node.capacity-vcpus` | `BBRAFT_NODE_CAPACITY_VCPUS` | `0` | Offered vCPUs; `0` detects host CPUs minus the reserve. |
| `-node.capacity-memory-mib` | `BBRAFT_NODE_CAPACITY_MEMORY_MIB` | `0` | Offered MiB; `0` detects `MemTotal` minus the reserve. |
| `-node.capacity-disk-gib` | `BBRAFT_NODE_CAPACITY_DISK_GIB` | `0` | Offered GiB; `0` measures free space under the data dir minus the reserve. |
| `-node.reserve-vcpus` | `BBRAFT_NODE_RESERVE_VCPUS` | `1` | vCPUs kept for the host. |
| `-node.reserve-memory-mib` | `BBRAFT_NODE_RESERVE_MEMORY_MIB` | `1024` | MiB kept for the host. |
| `-node.reserve-disk-gib` | `BBRAFT_NODE_RESERVE_DISK_GIB` | `1` | GiB kept for the host. |
| `-node.cpu-overcommit` | `BBRAFT_NODE_CPU_OVERCOMMIT` | `1` | Multiplier for detected host vCPUs before the reserve. Finite and greater than zero. |
| `-node.memory-overcommit` | `BBRAFT_NODE_MEMORY_OVERCOMMIT` | `1` | Multiplier for detected host memory before the reserve. Finite and greater than zero. |
| `-node.disk-overcommit` | `BBRAFT_NODE_DISK_OVERCOMMIT` | `1` | Multiplier for detected free disk and the live disk promise. Finite and greater than zero. |
| `-node.network-mode` | `BBRAFT_NODE_NETWORK_MODE` | `tap` | VM networking. `tap` is production and requires the complete configuration below. `user` is the explicit dev/test fallback and refuses every operator-supplied TAP or SSH setting. |
| `-node.network-bridge` | `BBRAFT_NODE_NETWORK_BRIDGE` | `bbraft0` | Per-node bridge that carries every VM TAP; created when absent. |
| `-node.network-prefix` | `BBRAFT_NODE_NETWORK_PREFIX` | empty (required in tap) | Private IPv4 range guests are allocated from, `/8` to `/30`. |
| `-node.network-gateway` | `BBRAFT_NODE_NETWORK_GATEWAY` | empty (required in tap) | The node's address inside the prefix and the guests' default gateway. |
| `-node.network-resolvers` | `BBRAFT_NODE_NETWORK_RESOLVERS` | `1.1.1.1,9.9.9.9` | Public unicast IPv4 DNS resolvers for guests, comma-separated, at most two. |
| `-node.network-management` | `BBRAFT_NODE_NETWORK_MANAGEMENT` | empty | Networks guests must never reach, comma-separated IPv4 prefixes (the host's management or tailnet network). |
| `-node.ssh-ingress-addr` | `BBRAFT_NODE_SSH_INGRESS_ADDR` | empty (required in tap) | Node address SSH ingress ports are published on. |
| `-node.ssh-ingress-iface` | `BBRAFT_NODE_SSH_INGRESS_IFACE` | empty (the production role requires it) | Host interface SSH ingress is bound to; must differ from the VM bridge. |
| `-node.ssh-port-range` | `BBRAFT_NODE_SSH_PORT_RANGE` | empty (required in tap) | Inclusive SSH ingress port pool, `min-max`, for example `22000-22999`; ports start at 1024. |
| `-node.jail-uid-range` | `BBRAFT_NODE_JAIL_UID_RANGE` | empty (required in tap) | Inclusive host uid=gid pool `min-max`, `0 < min < max < 2^31`: one uid per jailed VM, CPU probe or `qemu-img` attempt. No host account or group may use an id in it. |
| `-node.jail-cgroup` | `BBRAFT_NODE_JAIL_CGROUP` | empty (required in tap) | `<service cgroup>/vms`, for example `/sys/fs/cgroup/system.slice/bbraft-node.service/vms`. |
| `-node.jail-qemu-binary` | `BBRAFT_NODE_JAIL_QEMU_BINARY` | `/usr/bin/qemu-system-x86_64` | Trusted root-owned QEMU staged into every VM jail. |
| `-node.jail-tool-binary` | `BBRAFT_NODE_JAIL_TOOL_BINARY` | `/usr/bin/qemu-img` | Trusted root-owned `qemu-img` staged into every image-tool jail. |
| `-node.jail-firmware` | `BBRAFT_NODE_JAIL_FIRMWARE` | empty | Comma-separated trusted firmware files staged read-only into every jail. The role uses Debian 13's `bios-256k.bin` (seabios), `linuxboot_dma.bin` and `kvmvapic.bin`; no option ROM (`romfile=`). |
| `-node.jail-library-dirs` | `BBRAFT_NODE_JAIL_LIBRARY_DIRS` | empty (Debian multiarch) | Comma-separated trusted directories searched for QEMU's ELF libraries. |
| `-node.jail-io-device` | `BBRAFT_NODE_JAIL_IO_DEVICE` | empty (required in tap) | `MAJ:MIN` of the whole disk holding the data and image dirs. |
| `-node.jail-io-read-bps`, `-node.jail-io-write-bps`, `-node.jail-io-read-iops`, `-node.jail-io-write-iops` | `BBRAFT_NODE_JAIL_IO_READ_BPS` and so on | `0` (omitted) | Per-VM `io.max` limits. At least one must be positive in tap mode; no hardware default is assumed. |

Every jail flag is required or validated in tap mode and **refused** in the
dev/test user mode, by flag or variable, even when equal to its default.

Nested virtualization is Intel-only in phase 0.5. Adding `+svm` to
`Haswell-noTSX` leaves the guest CPUID vendor as GenuineIntel, which Linux
`kvm_amd` rejects. This release therefore exposes no AMD nested feature.
`auto` resolves to off on AMD hosts and logs the reason.
`-node.cpu-nested=on` is a startup error on an AMD host or any host lacking
the required feature set. `-node.cpu-model` explicit
values are honored as given and reported verbatim in `Info.cpu_model`.
The resolved `-cpu` option always carries `enforce` (for example
`Haswell-noTSX,+vmx,enforce`), so a VM fails to boot on a host that
cannot provide the fixed model or the requested nesting instead of
silently dropping features.

**Nested VMX fallback.** Some Intel hosts have VMX but not every VMX
capability the model asks for under `+vmx`; many desktop CPUs
lack APICv and posted interrupts. When the startup probe is refused
and every feature QEMU names as missing is a VMX capability (`vmx-...`) the
operator did not request explicitly, the node appends `-<name>` for each,
logs a `WARN` naming them (`dropped=vmx-apicv-register,...`) and probes
once more. Guests keep nested virtualization, only without those
accelerations. A missing feature of the fixed model itself (for example
`pcid`) is never dropped: the node stops, as before. The resulting toggles
are used for every VM this node process launches.

`-node.cpu-allow-host` is a dev/test-only opt-in for test runs that must
match the host CPU feature set (for example covering the varied CPU
support of VM test hosts). When set it lifts the `host`/`max` refusal for
exactly `-node.cpu-model=host` — `max` and every other casing or spelling
stay refused, and the node logs a startup warning whenever the opt-in
is enabled. With `host`, the node resolves through the same
shared test policy as the `BBRAFT_TEST_CPU_MODEL=host` KVM harness
(`host,enforce`, `+vmx` on Intel under `cpu-nested=auto`, nothing extra
on AMD) and `Info.cpu_model` reports `host`. Production configuration is
unchanged: the flag defaults to false and the Ansible role never sets it.

### Capacity

A capacity flag of `0` autodetects that resource, scales it by the matching
overcommit ratio, floors the exact decimal product, and deducts its reserve:

- vCPUs: `floor(runtime.NumCPU() * -node.cpu-overcommit)` minus
  `-node.reserve-vcpus`.
- Memory: `floor(MemTotal * -node.memory-overcommit)` from `/proc/meminfo`
  minus `-node.reserve-memory-mib`.
- Disk: `floor(available space * -node.disk-overcommit)` using `statfs`
  `Bavail` (what unprivileged processes may use) on the filesystem holding
  the data dir (or its deepest existing ancestor when the dir does not exist
  yet), minus `-node.reserve-disk-gib`.

A non-zero capacity flag is the operator's offer: no reserve is deducted,
but it must fit the same scaled detected promise. Ratios are decimal
shortest-round-trip values parsed as exact rationals, so `1.2` means 12/10,
not its binary approximation. Ratios must be finite and greater than zero;
zero, negative, NaN and infinite values are startup errors. The node logs
the detected facts, ratios and effective offer at startup.

Overcommit is soft for memory and disk, not for a single VM's CPU: every
create request still requires `vcpus <= detected host CPUs` even when the
aggregate offered vCPU count is larger, otherwise it is refused with
`RESOURCE_EXHAUSTED`. Memory and disk are logical promises. Disk admission
and `Info` share the same checked promise `floor(live free bytes * ratio)`
minus outstanding full logical reservations; `ratio=1` is the previous
behavior. Physical `statfs` errors and arithmetic overflow remain hard
failures, and sparse guest usage never weakens the reservation accounting.

### Sizing a node

Every sandbox type asks the node for a fixed reservation
(`internal/machinetype`):

| Type | vCPUs | Memory | Disk (logical) |
|---|---|---|---|
| `small` | 2 | 4 GiB | 51 GiB |
| `default` | 4 | 8 GiB | 51 GiB |
| `large` | 8 | 16 GiB | 111 GiB |
| `xlarge` | 16 | 32 GiB | 251 GiB |

A create is admitted only if **all three** fit what the node still offers
(see Capacity above), so the scarcest resource sets how many sandboxes run
at once. On most hosts that is **memory**. Worked example, a node with 20
host CPUs, 20 GiB of memory and 192 GiB free on the data dir, ratios
`cpu 2`, `memory 1.5`, `disk 4` (the startup line
`capacity resolved ... vcpus= memory_mib= disk_gib=` shows the real offer):

| Offer | `small` | `default` | `large` |
|---|---|---|---|
| vCPUs 39 | 19 | 9 | 4 |
| Memory ~28 GiB | **7** | **3** | **1** |
| Disk 767 GiB | 15 | 15 | 6 |

That node runs about 7 `small` or 3 `default` sandboxes at a time.

- **Disks are thin.** A sandbox disk is a sparse qcow2 overlay on the
  shared base image and takes on the host only what the guest writes; a
  fresh sandbox uses a few hundred MiB, not 51 GiB. Admission still charges
  the full logical size, so `disk_overcommit` is what lets more disks be
  promised than physically fit.
- **Stopped sandboxes** free their vCPUs and memory and keep only their disk
  reservation, so far more sandboxes can exist than run at once.
- **Memory overcommit is the risky ratio.** Above `1`, the node promises
  more RAM than the host has. That holds while guests stay below their
  size; when several fill their memory together, the host swaps and then
  the kernel's OOM killer picks a QEMU. Keep `memory_overcommit` at `1` where
  sandboxes run memory-heavy jobs (builds, tests with databases), or give the
  host more RAM.
- **Disk overcommit bets on usage.** With ratio `4`, the data filesystem
  fills before admission refuses anything if guests write more than about a
  quarter of their logical size on average (large builds, container images,
  caches). Watch `df` on the data dir; a full filesystem fails writes in
  every guest, not only the one that filled it.
- **CPU overcommit is the safe one**: vCPUs are time-shared, so an
  oversubscribed node gets slower, not unstable. A single sandbox never
  gets more vCPUs than the host has.

### Authentication

The node authenticates with one token per node id; the server stores only
its sha256.

- `-node.token-file` / `BBRAFT_NODE_TOKEN_FILE`: a regular file (symlinks
  are refused) with mode `0600` or stricter, at most 8192 bytes, contents
  trimmed before use. An explicitly configured file that is missing,
  malformed or permissive is a startup error; there is no silent
  fallback.
- `BBRAFT_NODE_TOKEN`: the raw token as a fallback when no token file is
  configured. Set-but-empty or over 8192 bytes is an error.

The token never appears in flags, logs or error messages.

## Transport

Phase 0.5 uses plain TCP (`DialConfig.TLS` is nil). To keep the token and
RPC traffic off the open network, `-server.addr` is restricted to:

- `localhost`, or
- a literal loopback IP (`127.0.0.0/8`, `::1`), or
- a literal private IP (RFC 1918 / ULA).

Wildcard (`0.0.0.0`, `::`), public, link-local and 4-in-6-mapped public
addresses and non-`localhost` hostnames are rejected with a config error.
TLS for the link is a later phase (mTLS is planned); until then a
node and server on different hosts need a private network between them.

## VM networking

Production uses **TAP mode** (`-node.network-mode=tap`, the default). Each VM
gets one TAP on the node's bridge (`-node.network-bridge`, default `bbraft0`)
with vhost-net, a static IPv4 address from `-node.network-prefix` and
`-node.network-gateway` (passed to the kernel with QEMU's direct-boot `ip=`),
and NAT for IPv4 egress. The node owns isolation in its own nftables tables:

- no guest → host traffic, including loopback services and the tailnet,
- no guest → guest traffic, and no spoofed source address or MAC,
- no guest → management networks (`-node.network-management`) or to private,
  link-local, loopback, CGNAT and multicast destinations,
- IPv6 from a guest is dropped,
- DNS only to the configured resolvers.

Tap mode fails closed before the manager exists. The node requires the
operator's `-node.network-prefix`, `-node.network-gateway`,
`-node.ssh-ingress-addr` and `-node.ssh-port-range`, then validates the whole
configuration, including the allocation state directory below the node data
dir (`<node.data-dir>/net`). Values that cannot be used safely are a startup
error: a public or too-narrow guest prefix, a gateway outside the prefix or
on its network/broadcast address, a resolver that is not public unicast, a
resolver or management network inside the guest prefix, an ingress address
inside the guest prefix, an ingress interface equal to the bridge, and ports
below 1024. `-node.network-resolvers` takes at most **two** addresses because
QEMU's direct-boot `ip=` parameter carries two nameservers; the defaults are
the public resolvers `1.1.1.1` and `9.9.9.9`, never the host's own resolvers.

### SSH ingress

Each READY sandbox's SSH is reachable at the configured ingress address and an
allocated port from `-node.ssh-port-range`: the node DNATs that port to the
guest's port 22 and reports `SSHAccess{host, port}` to the server. The pool is
bounded, and exhaustion is a clean failure, not a fallback. Setting
`-node.ssh-ingress-iface` binds the DNAT and its inbound accept to that
interface, so a packet addressed to the ingress address that arrives on
another interface cannot reach a guest; the production role requires the
interface. The ingress address may be private (a tailnet address is the
normal first deployment) and must be outside the guest prefix.

Enable strict reverse-path filtering on the node's public interface
(`net.ipv4.conf.<iface>.rp_filter=1`), whether or not SSH ingress is published
there: with a private or tunnel ingress interface, the public interface still
carries guest egress and unrelated traffic. Strict `rp_filter` drops traffic
whose return route would leave through a different interface; it is not a
general defence against spoofed sources that route back the same way, and
isolation does not depend on it.

### Dev/test user mode

`-node.network-mode=user` (`BBRAFT_NODE_NETWORK_MODE=user`) selects the
Phase 0.5 QEMU user-mode network for unprivileged KVM tests. It is explicit,
never a silent fallback, and refuses any operator-supplied TAP or SSH
setting: bridge, prefix, gateway, resolvers, management networks, ingress
address, ingress interface and port range. The built-in bridge and resolver
defaults do not count as operator input. The Ansible role validates
`bbraft_node_network_mode` as `tap` and renders the complete production
configuration, so a deployed node can never select user mode.

### Allocation and restart

The node persists one allocation record per sandbox under
`<node.data-dir>/net`, holding the guest address, MAC, TAP name, ingress
address/interface and SSH port. Reconciliation adopts live allocations and
re-renders its rules; a record that disagrees with the configuration is a
hard error rather than a silent re-addressing of a running guest. Network
resources follow the VM lifecycle: stopping a sandbox releases its TAP and
published SSH port, and resuming it allocates an address and port again. The
allocator may reuse the same numeric endpoint, so clients must use the
endpoint the API returns for the resumed sandbox; it may be unchanged. The
durable record is the restart identity for a running VM.

## Storage layout

```
<data>/                        # -node.data-dir; the role uses /var/lib/bbraft-node-v1
  .bbraft-node-data-v1         # v1 data-root marker written by the role
  node.lock                    # exclusive manager lock (flock)
  net/                         # network allocation records, one per sandbox
  jailer/                      # durable jail records, admission and staged runtime
  toolcheck/                   # qemu-img self-check fixtures (empty between runs)
  vms/<sandbox-id>/            # one dir per VM, mode 0700
    vm.json                    # durable manifest, mode 0600 (holds the VM env)
    disk.qcow2                 # writable overlay on the verified base image
    control/qmp.sock           # QEMU control socket, /control/qmp.sock in the jail
    serial.log                 # guest serial console, /serial.log in the jail
  orphans/<name>-<unix>-<random>/  # quarantined VM dirs; never touched again
<data>/images/
  <image-id>/                  # id: [a-z0-9][a-z0-9._-]{0,63}
    kernel                     # direct-boot kernel
    initrd                     # matching initramfs
    root.qcow2                 # immutable standalone base disk
    packages.json              # installed package record
    manifest.json              # schemaVersion/id/root/artifacts{sha256,size}
```

Every image version directory is verified against its manifest before use:
the entry set is exactly the files above, `root.qcow2` must be a standalone
qcow2 of the manifest's virtual size, and every artifact's sha256 and size
must match. The manifest names fixed filenames, never host paths, and image
ids never become unchecked path components. `-node.default-image` must name
an installed image id; when empty the first verified image id (sorted) is
the default and is reported as `Info.default_image`.

## Lifecycle

- `CreateVM` is idempotent on sandbox id: a call for an id that already
  exists joins the same boot and returns that VM whatever its other fields
  say. Capacity (vCPUs, memory, disk) and a unique vsock CID are reserved
  before the record is written. A reservation is released only after the
  VM's runtime absence and required cleanup are proven; a failed stop or
  a failed disk removal keeps the record and its reservation protected.
- Disk accounting is conservative. VM disks are sparse qcow2 overlays, but
  the full requested logical size of every outstanding reservation, the
  new VM included, is charged against live free space on the data-dir
  filesystem. Sparse files may look small today, yet promising the same
  free bytes to two disks is overcommit, so the check refuses with
  `RESOURCE_EXHAUSTED`.
- A boot outlives its caller. Only the last caller still waiting on a boot
  cancels it; once it does, the VM ends in a visible READY or FAILED
  record with its private disk preserved, or it is fully removed. A
  half-built disk is never left behind and a canceled boot can never
  resurrect a VM.
- `DeleteVM` cancels an in-flight boot and waits for the boot worker's
  cleanup before the process is stopped and the disk removed. Duplicate
  deletes join the single cleanup owner of that VM generation and report
  its outcome. A `CreateVM` for the same id during a delete is refused
  with ABORTED, so a recreate can start only after the previous generation
  is fully gone; earlier boot waiters observe ABORTED, never a resurrected
  VM. A VM whose process ownership could not be proven keeps its disk: the
  delete resolves it first or fails.
- On SIGINT/SIGTERM the link loop stops and the manager shuts down on an
  independent bounded (20 s) context: boot workers stop, guest connections
  close, the lock is released. Running VMs are left running so the next
  node process can adopt them; a VM that never reached READY is cleaned
  up.

## Restart

- The node takes an exclusive lock (`node.lock`, flock) on the data dir; a
  second node process on the same dir fails.
- The unit uses `KillMode=process`: stopping or restarting the service
  signals only the node, and jailed QEMUs keep running. On startup a jailed
  VM is adopted only when its durable jail record matches the live process:
  boot id and start ticks, its pool uid, its owned cgroup, the staged
  executable and the exact argv digest. A STOPPED VM stays stopped. A
  record of another attempt, or one no manifest claims, is never released
  or adopted; that VM stays FAILED until resolved.
- On startup it reconciles only directories named like sandbox ids. A VM
  whose process is provably still alive, meaning pidfd-pinned identity,
  the exact owned argv, the QMP peer pid and the guest name, is adopted
  and appears in `ListVMs` again. The pidfd pins one process incarnation,
  so pid reuse cannot trick recovery into adopting or killing the wrong
  process. Death is accepted only when a pidfd poll reports the exit or an
  explicit ESRCH is returned; unknown poll results and other errors never
  prove death.
- A record written before QEMU launch intent is a safe owned orphan and is
  removed. Anything else readable becomes a FAILED record with its private
  disk preserved until `DeleteVM`; a stale PID alone never justifies a
  kill, and a VM whose process cannot be proven owned or dead is kept
  unresolved, never killed or deleted. Foreign names are left in place
  untouched.
- A corrupt manifest is handled conservatively: before the record is set
  aside, the node proves that no live QEMU still uses the VM's paths. If a
  live owner is found or ownership cannot be disproven, startup fails
  rather than offer a running guest's CID or disk to new VMs. Only when no
  live owner is found is the directory moved to `orphans/` under a unique
  `<name>-<unix>-<random>` name, where the node never touches it again.
- Before an adopted VM becomes READY, the node resumes paused execution
  and discovers and thaws any surviving guest freeze lease. Ping alone does
  not prove that the filesystem is writable. Failed recovery leaves a FAILED
  record; process ownership and cleanup must be resolved before its resources
  can be reused.

## Snapshots

`SaveSnapshot` captures a READY VM under an opaque id chosen by the server.
A duplicate id returns AlreadyExists. Snapshot ids are hashed for directory
names and never become unchecked paths. Committed snapshots survive node
restarts and appear in `Info.snapshots`. Startup verifies their manifests,
disk checksums, qcow2 structure and verified immutable image backing.

The node freezes the guest with a bounded lease, stops QEMU, copies and
validates the stopped disk, resumes QEMU and thaws the guest before publishing
the snapshot. It uses the guest's remaining lease to bound the copy and
publication; retries never extend that deadline. Cancellation triggers
independent, bounded resume and thaw cleanup. If cleanup cannot prove that
the VM is running with an unfrozen filesystem, the VM becomes FAILED and
unavailable to Exec. Its process, disk and reservations remain protected
until cleanup proves that they can be released.

`CreateVM` with `from_snapshot` makes a private copy before boot. Its disk
must be at least as large as the saved disk. A larger request grows both the
private qcow2 disk and the guest root filesystem; shrinking is rejected.
`DeleteSnapshot` is idempotent and waits for a copy using the same snapshot.
Deleting the source VM or the saved snapshot cannot break an existing child.
Saves and VM deletion also serialize, so deletion waits for snapshot cleanup
and still fences earlier creates.

## KVM test CPU selection

On test VMs, set `BBRAFT_TEST_CPU_MODEL=host` for the KVM test harness and
image smoke/probe. Test-VM hosts expose different CPU features. This explicit
test override uses `host,enforce` on AMD and reports `host` in
`Info.cpu_model`; the shared nested policy may add `vmx` on Intel.
The normal node CLI and Ansible role do not enable this override. With the
variable unset, tests use the production `Haswell-noTSX,enforce` default.
The same shared policy is reachable on the node itself via the dev/test
`-node.cpu-allow-host` opt-in (see Flags and environment); the role never
sets that either.
A focused process test retains the default-model refusal error and verifies
that a rejected QEMU child is reaped. See [the image review commands](guest-image.md).

## Jailed QEMU

Phase 1 isolation. The node is a root supervisor with only
these capabilities, each tied to jail code: `CAP_SYS_ADMIN` (clone3,
namespaces, detached mounts), `CAP_SYS_CHROOT` (the jail chroot),
`CAP_MKNOD` (the jail's private device nodes), `CAP_SETUID`, `CAP_SETGID`
and `CAP_SETPCAP` (the per-attempt credential and bounding-set drop),
`CAP_CHOWN`, `CAP_FOWNER` and `CAP_DAC_OVERRIDE` (per-attempt ownership of
disks, the control directory and the QMP socket), `CAP_KILL` and
`CAP_SYS_PTRACE` (signalling and proving a different-uid QEMU) and
`CAP_NET_ADMIN` (bridge, TAPs, nftables). No ambient capabilities.

The unit's closed device policy (on the service cgroup, so it bounds every
jail too) allows exactly `/dev/kvm rwm`, `/dev/vhost-vsock rw`,
`/dev/vhost-net rw`, `/dev/net/tun rw`, `/dev/null rwm`, `/dev/urandom rwm`
and `/dev/vsock r`, plus the `char-rtc r` that systemd adds for
`ProtectClock=true`. `/dev/vsock` is read-only and only for the node's one
startup `VM_SOCKETS_GET_LOCAL_CID` ioctl: when the host's own vsock CID
(on a nested host, its guest CID) falls in the allocation range it is
logged and never given to a VM. A missing `/dev/vsock` means nothing to
exclude; every other probe error fails startup. No jail's private `/dev`
holds `/dev/vsock`.

Each QEMU and each `qemu-img` attempt:

- runs on its own uid=gid from `-node.jail-uid-range`, with no
  supplementary groups, all five capability sets empty and
  `NoNewPrivs=1`;
- lives in its own mount, PID and IPC namespaces with a chroot holding
  only the staged trusted binary, its libraries, the firmware files and a
  private `/dev` (`null`, `urandom`, and `kvm` for VMs);
- is born into its own cgroup under `-node.jail-cgroup`
  (`CLONE_INTO_CGROUP`) with `cpu.max`, `memory.max`, `pids.max` and
  `io.max`;
- reaches its disks only through descriptors: every disk graph names
  `/proc/self/fd/N`, the trusted base read-only and the overlay writable,
  and a qcow2's stored backing name is never opened. The QMP socket is
  `/control/qmp.sock` and the console `/serial.log`, both node-pinned.

The node is the only waiter of every jailed process. Its durable jail
records under `<data>/jailer` hold each attempt's uid, cgroup, boot id,
start ticks and staged executable, so a restarted node proves a live QEMU
before adopting it and proves a dead one before freeing its uid or disk.

### qemu-img self-check

After image verification and before snapshot recovery, the node runs the
installed `qemu-img` through the real jail on fixtures over the default
image's verified base: create an overlay, `info` (which must report the
stored backing name while opening no backing), `check` and `resize` with
the explicit base descriptor, and `info` of an overlay whose stored backing
name does not exist (it must succeed: an `info` that opened stored names
would fail). Any mismatch fails startup; there is no fallback. Tested
against qemu-img 8.2.2 (Ubuntu 24.04's package) and an upstream 10.0.13
build; Debian 13's own package has not been run yet: it will be checked at
deployment, when the node's startup self-check runs against it.

The fixtures live in `<data>/toolcheck/`, which the node owns. Every name
there is recorded by inode in an intent before it exists, and the node
removes only recorded inodes no tool still holds. A foreign, mismatched or
still-held entry is **preserved** and startup fails, naming it.

## Upgrading from Phase 0.5

Phase 1 starts from a **fresh data dir**. Nothing from a Phase 0.5 node is
reused, migrated or chowned:

- The role's default paths are new: data `/var/lib/bbraft-node-v1`, config
  `/etc/bbraft-node-v1`. The role refuses an existing data dir unless it is
  an empty root-owned 0700 directory or already holds the v1 marker
  (`.bbraft-node-data-v1`), and it refuses **before** changing anything, so
  a Phase 0.5 data dir (owned by `bbraft-node`, or root-owned without the
  marker) is left byte for byte as it was.
- Phase 0.5 VMs and snapshots do **not** carry over. The control plane's
  rows for them reach `vm_missing` once the new node reports its (empty)
  inventory; recreate what you need from images.
- The binary itself refuses to start while a live Phase 0.5 (unjailed) QEMU
  is recorded in its data dir, so point it at the fresh dir.

Deploying and draining a real node is an operator decision. After you have
verified that the old node is stopped and no old QEMU runs
(`pgrep -u bbraft-node qemu-system` prints nothing):

1. Inspect the old data dir (`/var/lib/bbraft-node`) and keep anything you
   need; it holds VM disks and manifests that the new node never reads.
2. Remove it by hand when you are sure (`rm -rf -- /var/lib/bbraft-node`).
3. The role no longer creates the `bbraft-node` account and never deletes
   it; remove it with `userdel bbraft-node` once nothing references it.
4. The old token under `/etc/bbraft-node` can be removed the same way.

## Operator cleanup

The node never deletes state it cannot prove it owns. Two kinds of
preserved state behave differently.

**Per-VM: startup continues.** One VM whose state cannot be proven is
recovered as a FAILED (unresolved) record, and the node still starts and
serves every other VM:

- **A retained jail reservation of the VM's own attempt** (`<data>/jailer`):
  its QEMU's death or ownership could not be proven. The VM stays FAILED
  with its disk, uid and reservation protected. Once no process of the
  named uid remains, a later restart or `DeleteVM` retries the release by
  proof.
- **A jail record of another or unclaimed attempt** (one the VM's manifest
  does not name): the VM stays FAILED and conflicted, and this is never
  released automatically, neither by a restart nor by `DeleteVM`. It needs
  operator inspection, with the node stopped: identify the named attempt,
  uid and cgroup; prove that no process of it remains and that its owned
  cgroup is empty; and finish the recorded resource and caller-file
  reclaim (return every file still owned by that uid, such as the overlay,
  control dir and console log, to root) before removing the record by
  hand. Deleting the record alone does not make the uid safe: a uid is
  never reused while files of the old attempt are still owned by it.
- **A FAILED VM whose process is gone** keeps its private disk until
  `DeleteVM`; see [Restart](#restart).

**Global: startup stops.** These fail startup with an error naming the
entry, and the entry is left as it was:

- **A live Phase 0.5 (unjailed) QEMU** recorded in the data dir: stop it
  with the old version, or use a fresh data dir.
- **A corrupt manifest whose paths a live QEMU may still use**: startup
  refuses rather than offer that VM's CID or disk again.
- **A retained CPU-probe record that may still be live**, and a **recorded
  admission cgroup that is not empty**: a startup probe that did not
  finish. Make sure no process remains in the named cgroup, then restart.
  An admission cgroup or scratch directory that no record names is never
  touched: it fails startup when it is non-empty and is only logged when
  it is an empty inert directory; remove it by hand once you are sure.
- **`<data>/toolcheck/`** entries that are foreign, swapped, hard-linked or
  still held by a tool (the qemu-img self-check fails): remove only the
  named entries after checking that no `qemu-img` of the jail uid range
  runs.

## Phase 1 limits

- Plaintext node ⇄ server link restricted as above; no mTLS yet.
- Snapshots are node-local qcow2 copies; no object-store publication, no
  continuous snapshots, no memory snapshots.
- Images are installed under `-node.image-dir` by the operator (or the
  Ansible role); the node verifies but does not yet pull them.
- The guest agent is reached over vhost-vsock only; the guest holds no
  platform credentials.
- One whole disk for the data and image dirs (`-node.jail-io-device`).
