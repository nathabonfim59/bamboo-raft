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
