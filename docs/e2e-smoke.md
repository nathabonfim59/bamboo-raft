# Compiled CLI/server end-to-end smoke

This document describes `internal/server/e2e_compiled_test.go` and
`internal/server/e2e_node_test.go` (package `server_test`). The test drives the
real `bbraft` CLI as an OS subprocess against a real `bbraft-server` process
over TCP. It is a PostgreSQL integration test: it skips unless
`BBRAFT_TEST_DATABASE_URL` is set, and `BBRAFT_TEST_REQUIRE_ALL=1` turns a
missing database or pinned spec into a failure. Both binaries are built only
after that gate.

## What the test proves

- `login --key-stdin` persists a 0600 `config.json` in a 0700 directory and
  never prints the key. Later CLI invocations run with `BBRAFT_API_URL` but
  **no** `BBRAFT_API_KEY`, so the persisted configuration is what
  authenticates them.
- `new --json` emits exactly one `ready` event whose `id` is parsed like
  an agent harness does, followed by `list`/`info` through the real API.
- `exec --stream` raw output (through a PTY), explicit `--json --stream`
  frames, the exact agent-harness argv form
  `exec ID --timeout 0 --stream --json -- bash -lc CMD`, a remote exit 7 and a
  remote timeout exit 124.
- `--timeout 0` discovers the server's actual `streamMaxSeconds` through one
  `/api/bbraft/info` request per command; no duplicate metadata requests.
- `push --to` of a directory larger than 5 MiB through a cross-origin signed
  upload URL, with no bearer or cookie on the raw PUT. The uploaded tar is
  inspected in the fake node's confined root for executable bits, a symbolic
  link and unusual filenames. Absolute and relative `--to` destinations are
  covered.
- Saved snapshot save/wait, same-name replacement with a new artifact id, the
  old generation retiring and being purged, and `new --from` with an omitted
  type (inherited), an explicit equal type, an explicit larger type, and a
  smaller type rejected with 400 `invalid_json`.
- Deleting the source sandbox only after its VM deletion job is durably done,
  then restoring from the saved snapshot and reading a file that was pushed
  before the save.
- `delete --yes --wait` sends `X-Ascii-Confirm-Delete`, returns a completed
  operation, and releases the sandbox row, node reservation and VM directory.
- `run --push .` uses the pushed directory as the default remote cwd, reports
  real remote output/exit and a truthful `deleted` summary, cleans up after a
  remote nonzero exit, and cleans up after SIGINT (exit 130) with the remote
  shell and child process group gone. A successful human-mode run with an
  explicit `--cwd`, raw stdout, an `exit 0` stderr summary and completed
  deletion is also covered.
- Unsupported `run` flags (`--personal`, `--environment`, `--no-env`) fail
  before any request or sandbox effect. On the legacy harness `run --keep` is
  valid syntax but exits 1 with `keep_unsupported` after its one metadata GET
  and before create, because that server has no TTL renewal.
- A second configured key logs in with its own isolated config and cannot see
  the first owner's sandbox.

The legacy compatibility harness deliberately removes `commands.detached` from
the info response before the CLI sees it. That keeps the old fixed run TTL,
legacy streamed command body, command counts and remote-exit assertions as
explicit old-server coverage; the compiled phase-exit test below reads the real
`commands.detached: true` response.

## Phase-exit compiled flow

`TestPhaseExitCompiledCLI` in `internal/server/phase_exit*_test.go` covers the
accepted Phase 1 surface through the same real binaries:

- the real info response advertises `commands.detached: true` and the
  unchanged 7200-second stream cap;
- create, rename and all three extend selectors (including a null TTL) through
  the CLI, with exact PATCH bodies;
- representative core file operations (folder, write, read, info, list,
  permissions, move, recursive delete) and a server-signed download fetched
  without a bearer credential;
- sync exec, the exact dogfood argv through one detached start plus attach,
  explicit detach/status/attach, idempotent kill, explicit attach cancellation
  with exit 130, and a controlled HTTP cut whose reconnect must reproduce the
  output byte-for-byte;
- stop, stop-status polling, resume on the same retained disk, and a terminal
  delete with released reservations and removed VM directory;
- `run` cleanup and `--keep` through the real CLI: create TTL 900, an immediate
  PATCH, one preconfirmed detached timeout-0 start, default deletion, and a
  final keep PATCH with an explicit delete afterwards;
- the exact dogfood argv against a shim that omits `detached` and against one
  that answers 404, proving the legacy stream path sends the returned cap or
  the 600-second fallback and never re-POSTs.

Every captured success, error and NDJSON frame validates against the pinned
document, the subset document and `api/bbraft-ext.yaml`. Captured bodies,
tokens and logs are never printed on failure.

The test records request/response fields through two bounded reverse proxies.
Captured request and response bodies are swept for platform credentials,
database URLs, the master key and environment sentinels before any validator
or diagnostic runs. Responses are validated against the v1 API document
and the accepted subset, and extension responses against `api/bbraft-ext.yaml`.
Validation uses the normal `IncludeResponseStatus` path; there is no broad
exception. Failure messages are field-only: request bodies, environment values,
credentials, signed URLs and decoder text are never printed. A legitimate
upload-URL response may contain its signed URL; that token is tracked
separately and never printed by CLI output or logs.

## What nodefake cannot prove

The node is `nodefake` behind the real `nodelink` transport, not a KVM guest.
The test therefore does **not** prove:

- QEMU/KVM boot, guest kernel, vsock, real disk images or real disk shrink;
- guest-absolute path semantics or real filesystem isolation;
- a real guest agent's tar extraction (the test wrapper emulates only the exact
  internal push extraction/cleanup script with a confined `os.Root`);
- real SSH reachability or a real guest sshd; the SSH/scp CLI tests use a fake
  API and a local pseudo-terminal, so they prove key management, host-key
  pinning and argv/terminal handling only;
- real network isolation, real node capacity accounting or real snapshot
  artifact storage.

Normal user commands delegate to nodefake through an explicit allowlist of the
fixture commands and keep their real remote exit status. The wrapper never
executes a CLI-generated guest-absolute script on the host: an internal push
script that does not match its exact validated form is refused, not delegated.

## Running it

Focused local compile/skip (no database, no binaries built):

```sh
go test ./internal/server -run 'TestE2ECompiledCLI|TestPhaseExitCompiledCLI' -count=1 -v
```

Full run on a test VM with PostgreSQL and KVM not
required for this fake-node test:

```sh
BBRAFT_TEST_DATABASE_URL=postgres://... \
BBRAFT_TEST_REQUIRE_ALL=1 \
go test ./internal/server -run 'TestE2ECompiledCLI|TestPhaseExitCompiledCLI' -count=1 -v
```

The test creates one throwaway
PostgreSQL schema and drops it on cleanup. It builds `cmd/bbraft-server` and
`cmd/bbraft` into temporary paths; it never installs or deploys anything.

## Repeatable KVM smoke for orchestrator integration

This is a procedure only; it is not permission to deploy, and it never uses
real inventory or host details.

1. Start the control plane roles on the target host with the documented
   `bbraft-server` flags and `BBRAFT_*` secrets from files. Wait for
   `/healthz` and a fresh node inventory row.
2. Configure one real `bbraft-node` with its token file and confirm the node
   appears in the inventory. Do not put platform API keys or object-store keys
   in the node or guest.
3. From a client with an isolated `XDG_CONFIG_HOME`, run:
   - `bbraft login --key-stdin` (no `BBRAFT_API_KEY` afterwards);
   - `bbraft new --json --type large` and parse the `ready` id;
   - `bbraft push ID . --to /home/user/target`;
   - `bbraft exec --timeout 0 --stream --json ID 'cat target/<file>'`;
   - `bbraft snapshot ID base`;
   - `bbraft new --from base --type large`;
   - `bbraft delete ID --yes --wait` and confirm the operation completes and
     the VM directory is gone;
   - `bbraft run --push . --json -- 'make check'`.
4. Confirm the durable deletion jobs, released node reservations and removed
   VM/snapshot artifacts before treating the run as clean.
5. Run the same `bbraft run --push . -- make check` for bamboo-raft and for
   parkana once the node is accepted.

The fake-node test above covers the CLI/control-plane contract. The KVM smoke
is the remaining acceptance step for real guest execution, real snapshots and
real capacity.

## Two-node end-to-end with the object store

`deploy/dev/e2e-p2.sh` is the real-node counterpart for snapshots in the
object store, node loss and moving sandboxes between nodes. Run it on one
large throwaway KVM VM (`/dev/kvm`, `/dev/vhost-vsock`, `sudo -n`, QEMU,
Postgres, `openssh-client`, the pinned `garage` binary in `PATH`) from the
repository root, on a branch whose node has object-store support, the
`bbraft_node` role's S3 variables and the `-node.vsock-cid-range` flag:

```sh
BBRAFT_TEST_DATABASE_URL=postgres://... bash deploy/dev/e2e-p2.sh
```

It builds everything and the pinned image, starts Garage (bucket and key
created by the script, the secret only in `0600` files), a fresh Postgres
database and `bbraft-server -roles all` with the object store and short
lifecycle intervals (snapshot interval 10 s, node-lost grace 60 s, reaper
5 s), then two nodes:

- **node a** is the production path of `e2e-p1.sh`: the root supervisor under
  the unit the `bbraft_node` role renders, jailed QEMU, TAP networking and SSH
  ingress. It offers one running small sandbox and two small disks, so
  capacity decides every placement in the script.
- **node b** is a plain process of the calling user in
  `-node.network-mode=user` (QEMU unjailed, no guest networking). It offers
  ten small sandboxes' worth (20 vCPU, 40960 MiB, 510 GiB), so `count 10`
  puts nine of them on b. The script kills it with `SIGKILL` and restarts it.

Node b is user-mode because two real tap nodes on one host collide on the
nft tables, the bridge, the guest subnet and the SSH ingress (per-node network
naming is a later item). Guest networking is therefore checked on node a (ssh
through the SSH ingress, and an address inside the node a guest prefix) whenever a sandbox
lands there; on node b only the data is checked. Both nodes share the host's
vsock CID space, so node b gets its own range through
`E2E_NODEB_EXTRA_FLAGS` (default `-node.vsock-cid-range 1000000-1999999`); the
build stage fails with a clear message when the node binary does not know the
flag.

Node b is sized from the host at run time: `nproc`, `MemTotal` and the free
disk, minus node a's offer (2 vCPU, 4096 MiB) and 1 GiB of memory for the
host. The node refuses an explicit capacity above the detected host times its
overcommit factor, so b gets its own `-node.cpu-overcommit`,
`-node.memory-overcommit` and `-node.disk-overcommit`, computed so the offer
fits with a 25% margin (disk: 100%, since free space shrinks as guests write)
and printed by the preflight. Node a keeps its 110 GiB offer and gets only a
disk overcommit, through the role variable `bbraft_node_disk_overcommit`,
computed from the free space under `/var/lib`. On an 8 vCPU / 16 GiB VM with
about 97 GiB free on one filesystem this gives b 4.17 / 4.71 / 10.52 and a
2.27 for disk. Offers are promises; what must be real is free space, so the
preflight refuses a host with less than 30 GiB actually free under either data
directory (a shared filesystem counts once), and also when even the ceilings
(cpu 8, memory 8, disk 20) cannot fit ten small sandboxes across both nodes,
for example on a 2 vCPU host. Overcommit is fine for this test: the guests
idle, a small idle guest touches a few hundred MiB of the 4 GiB it reserves,
and the disks are thin.

The guest prefix of node a is `10.88.0.0/16` (gateway `10.88.0.1`), set once
in `e2e-p1-node.sh` and exported as `E2E_NODE_GUEST_PREFIX` /
`E2E_NODE_GUEST_GATEWAY`; the tap check derives its expected address pattern
from it. The preflight refuses a host that holds an address inside the prefix
(the disposable VM may itself be a sandbox on a private prefix: a guest would
be handed the host's own address and SSH would reach the host), naming the
address.

Every `/api/v1` response goes through a recording proxy and is validated
against `api/bbraft-v1.yaml` by `internal/compat`, as in `e2e-p1.sh`. One
`PASS`/`FAIL` line per stage; logs are dumped redacted on failure; the
teardown deletes every sandbox, stops both nodes and Garage, drops the
database, proves nothing leaked and restores `net.ipv4.ip_forward`; `E2E PASS`
is printed only after that.

| Stage | What it proves |
|---|---|
| `write-snapshot` | a sandbox created while only node b exists (its disk is written on b) takes a file; a snapshot generation captured after the write appears within the interval |
| `node-a` | node a starts through the role with the object store and registers |
| `node-loss` | `kill -9` of node b: within the grace (plus inventory polls) the sandbox is ready on node a with the file, guest networking works there, at most one interval of writes is lost (a write right before the kill is reported, not required), the stale VM copy on b has an eviction row |
| `node-returns` | b restarts: its stale copy is deleted, no QEMU of it runs, the API shows one sandbox |
| `stop-resume-lost-disk-node` | a sandbox stopped on b whose node is then lost resumes on node a with its file (guest networking checked) |
| `stop-resume-a-to-b` | a sandbox stopped on a, with a made unable to run it (a filler sandbox takes its one running slot while b is stopped cleanly), resumes on node b with both files |
| `fork` | fork of a ready sandbox is ready, has the file, the source is unchanged and does not see the fork's writes |
| `saved-count` | the sandbox saved as `e2e-base`; creating 10 from it gives 10 ready sandboxes on both nodes, each with the file |

The image stage builds the pinned image when `images/out/<version>` is
absent. When it is present it is reused only if `images/verify.sh` passes
(manifest, sha256 and size of every artifact) and the manifest records the
pinned id, Ubuntu snapshot (no mirror override), Go and Node versions; any
other image there is refused with the reason (a published version is never
overwritten, so remove it by hand). `E2E_REUSE_IMAGE=1` forces reuse and never
builds.

Expected wall time: about 25 minutes with the image already present, about
35-45 minutes with the image build. The two node
losses dominate (each at least the 60 s grace plus up to two 30 s inventory
polls, then a restore), then ten sandboxes boot at the end.

Debugging a failed run: `E2E2_DIAG_ON_FAIL=1` makes a failing stage collect,
before the teardown, what tells where guest networking or the SSH path broke,
for the sandboxes the stage touched: `bbraft info` and the raw API view (with
`sshEndpoint`), node a's nft ruleset and the DNAT lines for the endpoint
port, its journal, the VM directory and serial console, probes run inside the
guest through exec (not ssh: kernel command line, addresses and routes,
netplan, `networkctl`, the network units, cloud-init status, authorized keys),
one `ssh -vvv` attempt with the CLI's own endpoint and key, and a fresh control
sandbox on node a when its one running slot is free. The text is printed and
saved under `$E2E_OUT/diag-<run>/`. Nothing secret is collected: every text
goes through the same redaction as the rest of the script, and cloud-init data
is only listed, never read. Without the variable nothing changes.

Self-test, no VM, server, node or network (stage order, teardown after a
failing stage, redaction, distinct names for the two node instances, node b's
capacity from the host, the pinned-image check, the diagnostics hook with its no-secret check, the guest prefix):

```sh
E2E_SELFTEST=1 bash deploy/dev/e2e-p2.sh
```

The script sources `e2e-p1.sh` as a library (`E2E_HELPERS_ONLY=1`) and the
node driver `e2e-p1-node.sh`, which gained one optional input,
`E2E_NODE_EXTRA_VARS_FILE` (a JSON file of extra role variables, here the
object-store files and node a's capacity); unset, the driver behaves as
before.
