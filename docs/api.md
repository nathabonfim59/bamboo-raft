# API v1

bamboo-raft serves its sandbox API under `/api/v1`. The OpenAPI document is
[`api/bbraft-v1.yaml`](../api/bbraft-v1.yaml); server-specific extensions live
under `/api/bbraft` and are described in
[`api/bbraft-ext.yaml`](../api/bbraft-ext.yaml). This page records how the
operations behave beyond what the schema can say.

## Verification

`internal/compat` is test support, imported only by tests. For every response it
checks the **actual status, media type and body** against `api/bbraft-v1.yaml`
(or `api/bbraft-ext.yaml` for extensions), including required members,
`const`, `enum` and nullability. Streamed commands are checked one NDJSON
`CommandStreamFrame` line at a time. On top of the schema, a per-operation
error-code matrix pins the exact codes each error status may carry (for
example `502` is always `sandbox_direct_failed`, and a `503` must carry a
positive integer `error.details.retryAfterSeconds`). Negative self-tests mutate
required fields, constants, enums, nullability, status codes, media types and
stream frames, and fail if the validator accepts them.

The harness uses `github.com/getkin/kin-openapi` and its OpenAPI 3.1 / JSON
Schema 2020-12 support.

## Operation matrix

Operations implemented in 0.1.0:

| Operation | Method and path | Notes |
|---|---|---|
| `me` | `GET /me` | Same envelope; account fields are a subset. |
| `sandboxes` | `GET /sandboxes` | Cursor pagination, `state`, `name`, `namePrefix`, `label`. |
| `create` | `POST /sandboxes` | Starts provisioning from the node base image; with `from`, restores a saved snapshot on its artifact node; status 202. |
| `get` | `GET /sandboxes/{sandboxId}` | Poll after create, stop, resume or delete. Exports `subdomain`, `sshEndpoint`, `stoppedAt`, `archiveAfter`, the configured `ttlSeconds` and, after any stop, its `stop` metadata. |
| `update` | `PATCH /sandboxes/{sandboxId}` | Display name, tri-state `ttlSeconds` and validated globally unique `subdomain`. Empty names are rejected and over-long names are truncated to 120 characters. |
| `deleteSandbox` | `DELETE /sandboxes/{sandboxId}` | Requires `X-Ascii-Confirm-Delete`; status 202 with a deletion operation. |
| `getDeletionOperation` | `GET /deletion-operations/{operationId}` | Owner-scoped poll; `Cache-Control: no-store`. |
| `stop` | `POST /sandboxes/{sandboxId}/stop` | 202 while archiving with the canonical `operation` and `statusUrl`; 200 with the last completed stop when already archived. `force` escalates a pending stop. |
| `getStop` | `GET /sandboxes/{sandboxId}/stops/{stopId}` | Owner, sandbox and stop id are all bound; old stops stay readable after resume or a later stop. |
| `resume` | `POST /sandboxes/{sandboxId}/resume` | Same-node resume of the retained disk, with optional `type`, `env`, `ttlSeconds` and `failFast`. Never falls back to another node or an empty disk. |
| `command` | `POST /sandboxes/{sandboxId}/commands` | Synchronous, `stream: true` and `detached: true`; sync/stream keep the 7200-second stream cap. |
| `commandStatus` | `GET /sandboxes/{sandboxId}/commands/{processId}` | Durable detached status and log tails or offset windows; `LOST` and tenant scoping. |
| `attachCommand` | `GET /sandboxes/{sandboxId}/commands/{processId}/stream` | NDJSON `attached`/`stdout`/`stderr`/`heartbeat`/`exit`/`reconnect`/`error`; raw byte offsets, bounded frames and 10-minute reconnect. |
| `stopCommand` | `DELETE /sandboxes/{sandboxId}/commands/{processId}` | SIGTERM then SIGKILL through the node, with the final command record; an already ended process returns 200 with `signaled: false`. |
| `createUploadUrl` | `POST /sandboxes/{sandboxId}/files/upload-url` | Server-served URL; token-bound and streamed. |
| `readFile` | `GET /sandboxes/{sandboxId}/files` | UTF-8 or base64, 5 MiB inline limit; a starting sandbox is waited for. |
| `writeFile` | `PUT /sandboxes/{sandboxId}/files` | UTF-8 or base64, 5 MiB after decoding; parent directories are created. |
| `deleteFile` | `DELETE /sandboxes/{sandboxId}/files` | `recursive` for non-empty directories; a symlink is removed itself and the roots are refused. |
| `listFiles` | `GET /sandboxes/{sandboxId}/files/list` | One level, sorted, at most 10,000 entries with `truncated`. |
| `getFileInfo` | `GET /sandboxes/{sandboxId}/files/info` | Size, type, symlink flag, four-digit octal mode and modified time. |
| `createFolder` | `POST /sandboxes/{sandboxId}/files/folder` | Missing parents created; an existing directory keeps its mode. |
| `moveFile` | `POST /sandboxes/{sandboxId}/files/move` | `overwrite` replaces a file or empty directory; paths may cross `/home/user` and `/tmp`. |
| `setFilePermissions` | `POST /sandboxes/{sandboxId}/files/permissions` | Permission bits up to `0o7777`, including setuid, setgid and sticky; owner and group do not change. |
| `createDownloadUrl` | `POST /sandboxes/{sandboxId}/files/download-url` | Server-served signed URL; the public receiver streams raw bytes without a bearer header. |
| `sshKey` | `POST /sandboxes/{sandboxId}/sshkey` | One OpenSSH public key through `AddSSHKey`; returns `machineIp`, `sshUser`, `sshEndpoint` and the public `hostKey`. The endpoint is durable only for the current boot. |
| `listSavedSnapshots` | `GET /saved-snapshots` | Local snapshots, newest first, with `name` and `namePrefix` filters. |
| `saveSavedSnapshot` | `POST /saved-snapshots` | Status 202, `saving` until ready or failed. |
| `getSavedSnapshot` | `GET /saved-snapshots/{name}` | Poll a save. |
| `deleteSavedSnapshot` | `DELETE /saved-snapshots/{name}` | Source sandbox deletion does not remove the artifact. |

Not implemented yet: fork and recover; command history and process
lists (the detached records are exposed through command status/stop but not
`command-history`); `searchFiles`, `findInFiles` and `replaceInFiles`;
snapshot `tree`, `files` and `download`; `host` and `desktop`;
prompts, conversations and events; environments, repos and secrets;
organizations, teams, billing and `GET /limits`; webhooks; API keys; CI. They
are not emulated with fake success.

## Stop and resume

Stop and resume are local-disk operations. A normal stop keeps the VM disk on
its node, releases compute and archives the sandbox; resume starts that same
disk on the same node. A forced stop persists the escalation on the canonical
stop operation; the worker allows the current stop attempt a one-second grace
before forcing it, so the stop stays asynchronous and queue-backed and keeps
the disk as it is. There is no cloud snapshot chain, so force is not a
rollback to an older snapshot and neither stop form restores an older point in
time.

A stop is a durable operation: a new or pending stop returns `202` with the
canonical `operation` and `statusUrl`, an already archived sandbox returns
`200` with its last completed stop, and a repeat while pending returns the same
id. `force` escalates a pending stop. Stop metadata is exported on `get`/list;
`requestedAt` is null there once the stop ended, while `getStop` keeps it. A
stop whose VM and disk are confirmed missing after the initial boot settles
ends in the fixed `vm_missing` failure (sandbox `error`, stop `failing` with
that error) with no automatic retry; the row remains readable and a normal
`DELETE` still works. Resuming such an error sandbox answers `409`
`sandbox_not_ready` with `retryable: false` and does not admit a new machine.

Resume keeps the retained disk and never falls back to another node or an
empty replacement. `type` may keep or grow the disk but a shrink below it is
`409 type_too_small`; `env` replaces the per-sandbox environment; only
`environment: base` is accepted (other names are `400 unknown_environment`) and
`noEnv: true` is refused rather than ignored; `ttlSeconds` is tri-state
(omitted keeps it, null disables auto-stop, a value resets the deadline);
`failFast` answers `503 no_ready_machine` when no machine is ready.
Changing type, requesting an environment change or requesting `noEnv` on a
running sandbox is `409 sandbox_running_stop_first`; a resume that cannot take
over an in-flight stop is `409 sandbox_stopping` with its `statusUrl`.
`get`/list report the configured `ttlSeconds` (not the remaining time),
`archiveAfter`, `stoppedAt`, `subdomain` and the latest `stop` object.

## Error codes by status

Besides the statuses each operation documents for its own requests, every
operation may answer:

- `500 http_500` for an internal control-plane fault.
- `502 sandbox_direct_failed` when the node or guest did not complete the call
  (commands, detached command status/attach/stop, the eight file operations
  and `sshKey`). For writes and other mutations the call may or may not have
  taken effect, so `retryable` says whether a retry is safe.
- `503 out_of_capacity` (synchronous command only) and
  `503 ssh_host_key_unavailable` (`sshKey`), both with
  `error.details.retryAfterSeconds`.
- For file operations: `401 unauthorized`, `404 not_found`, and `409` with one
  of `machine_not_running`, `sandbox_not_ready` or `sandbox_starting` (the
  latter after a 60-second wait for a starting sandbox).

`create` additionally answers `400` with `invalid_json`, `invalid_env`,
`invalid_name`, `invalid_query` or `unknown_environment`, and `404
snapshot_not_found` for a missing, foreign or not-ready saved snapshot.
`resume` answers `400` with `invalid_json`, `invalid_env` or
`unknown_environment`. A starting sandbox is reported as `409
sandbox_starting`.

## Files and signed downloads

Inline reads stop at 5 MiB and writes accept at most 5 MiB after decoding;
larger files use the signed URLs. `readFile` reports `413 file_too_large` with
`maxBytes` and, when the node stated the size, `sizeBytes`; `writeFile` reports
`413 file_too_large` without inventing a size. A non-empty directory needs
`recursive=true`, a symlink is removed or moved itself, and `/home/user` and
`/tmp` themselves are refused. Listing sends a 10,000-entry limit and reports
`truncated` when the directory holds more.

`createFolder` with an explicit `mode: "0000"` follows this protocol:
an existing path is stat'ed and its mode is preserved, while a missing path is
created with the node default and then chmod'ed to `0`. A failure of either RPC
is surfaced and never cleaned up, because a concurrent creator may own the
path. A concurrent creator between the missing stat and MakeDir can therefore
leave the directory at mode `0000`; that race is accepted.

Known guest limitation: file operations return `400 invalid_path` when the
user removes read permission from their own `/home/user` or `/tmp` (for
example with mode `0111`). Commands still work.

The signed download URL is bound to one owner, sandbox, canonical path and
expiry, and carries a version, purpose, nonce and HMAC-SHA256 signature over
the download domain. Its key is derived from the installation master secret
with a download-specific HKDF context, so an upload token can never verify as
a download token or vice versa. The receiver is public: the token is the only
credential and is never logged. The first node message is validated before any
HTTP header is written, so pre-header failures are normal error envelopes;
after the headers, a node failure aborts the connection so a truncated body is
never mistaken for a complete file. The response is `application/octet-stream`
with an `attachment` filename and `X-Content-Type-Options: nosniff`.

The public receiver is registered for `GET` only. A wrong method is answered by
the server's ServeMux/safe-defaults layer as `405 method_not_allowed` (the same
behavior as the upload receiver), so a token is never usable through another
method. The handlers also keep a direct-dispatch `405 not_found` guard, which
is covered by the core handler tests.

## SSH access

`POST /sandboxes/{sandboxId}/sshkey` installs one OpenSSH public key through
the node's `AddSSHKey` and returns the `ssh_key.configured` envelope:
`machineIp` (native, unbracketed host), `sshUser` (`user`), `sshEndpoint`
(`host:port`, with IPv6 hosts bracketed by `net.JoinHostPort`) and the public
`hostKey`. The guest installs the user's public key in `authorized_keys`;
the control plane forwards it and does not persist user public or private
keys. Private keys, key options and multi-line values are refused with the
declared `400 invalid_json`. Key material never reaches a log or an error
message.

The endpoint and host key are durable metadata of one boot. They are stored
only after `AddSSHKey` succeeded and the node's answer passed structural host
key validation, and only while the owner-scoped row still has the same node
and lifecycle generation. `get` and list export `sshEndpoint` as a
nullable field only in `ready`, `idle` or `running`; it is an explicit `null`
before the first successful call. A stop, resume or terminal error clears the
pair in the same fenced state change, and a late response from the previous
boot cannot restore it. After a resume the endpoint stays null until the next
successful `sshKey` call refreshes it. `DELETE` does not clear the metadata;
it makes the whole owner-scoped row invisible, so later SSH calls and `get`
answer the normal `404`.

A stopped sandbox or a stale boot answers `400 machine_not_running`,
never a `409`. An unavailable host key answers `503
ssh_host_key_unavailable` with `retryAfterSeconds: 5` and stores nothing. A
node failure answers `502 sandbox_direct_failed`, non-retryable
for `RESOURCE_EXHAUSTED` and retryable for idempotent causes.

Nodefake returns fixed reference metadata; the test suite proves the API and
metadata contract, never real SSH reachability.

## bbraft extensions

Extensions live under `/api/bbraft` and never add fields to `/api/v1`.
`GET /api/bbraft/info` is authenticated with the same bearer key and returns
the build version plus the command limits this server actually enforces
(`streamMaxSeconds`, `syncMaxSeconds` 600). It exposes no credential, node,
host or account data. The document is `api/bbraft-ext.yaml`. The CLI reads it
for `--timeout 0`, for a positive timeout above 600 seconds (to decide whether
detached execution is available), and for `run` before create (to choose
rolling or legacy TTL). On a legacy server `--timeout 0` sends the returned
`streamMaxSeconds` exactly, never zero. A `404` falls back to 600 with a note
on stderr (a server without the extensions); any other failure or malformed response is an
error, never a silent fallback.

This server also sets `commands.detached: true`, advertising the
detached command start, status, attach and stop operations. The member is
optional: an old server omits it or sends `false`, and a client must then keep
the legacy synchronous/streamed path. Only an explicit JSON boolean `true`
selects detached execution; an absent member and an explicit `false` mean
legacy, while any other JSON kind is a malformed response and is rejected.
With `true` and `--timeout 0`, the command is unlimited: the detached start
omits a timeout entirely rather than sending zero. The 7200-second streamed
cap is unchanged.

`PUT /api/bbraft/uploads/{token}` and `GET /api/bbraft/downloads/{token}` are
the two public signed-URL receivers. Each token is the only credential; the
routes never use bearer authentication, never consult the request `Host` or
proxy headers and answer every token failure with the same `404`. Both are
documented in `api/bbraft-ext.yaml`.

## CLI behavior

The CLI covers sandbox create, list, info, rename, extend, stop, resume, exec,
run, push, snapshot, ssh, scp and delete. The tests validate every captured
success and error response against `api/bbraft-v1.yaml`, and against
`api/bbraft-ext.yaml` for the extension responses; production handlers are not wrapped in a full-spec
validator at runtime.

The scripted invocation agent harnesses use keeps working:

```sh
bbraft exec <id> --timeout 0 --stream --json -- bash -lc '<command>'
```

It emits the legacy JSON Lines shapes `{type:"stdout"|"stderr",data}` and
`{type:"exit",exitCode}` and returns the remote process status. A positive
timeout at or below 600 seconds is synchronous unless `--stream` is set; `run`
always requests streamed output. The CLI reads the metadata for `--timeout 0`,
for a positive timeout above 600 seconds and once before a `run` create. On a
server that advertises `commands.detached`, `--timeout 0` is unlimited and the
detached start omits a timeout entirely; above 600 seconds the command uses a
detached start with that local timeout. Either way it is started exactly once
and followed through attach. On a legacy server `--timeout 0` sends the
returned `streamMaxSeconds` exactly. The CLI never re-POSTs a command after an
ambiguous start. A server without the info endpoint falls back to the legacy
600-second stream with one stderr note; a valid response without `detached`
(or with `false`) keeps the legacy streamed path with the returned cap. A
malformed metadata response is an error.

Cancelling a transparent long command stops the known remote process through
`stopCommand` before exiting 1. Cancelling an explicit `exec --attach` exits
130 and leaves the detached process running. A cancelled `run` also exits 130.
`exec --kill` is idempotent: an already ended process answers 200 with
`signaled: false`.

`ssh` and `scp` manage `~/.ssh/bbraft_ed25519` with safe permissions and no
overwrite, install its public key through `sshKey`, pin the returned host key
for one invocation (never a shared `known_hosts`), and execute the local
OpenSSH client with an argument vector. Their CLI tests use a fake API and a
local pseudo-terminal; they prove key handling, host-key pinning, argv
quoting, terminal handoff and subprocess exit codes, but not real SSH
reachability. That remains a node and guest responsibility.

Known limits:

- No fork/recover, dashboard, command history, process list,
  file search or find/replace. Detached commands are reachable through status,
  attach and stop only.
- File operations return `400 invalid_path` when the sandbox user removes read
  permission from their own `/home/user` or `/tmp` (for example mode `0111`);
  commands still work. The `mode: "0000"` createFolder race is
  described under [Files and signed downloads](#files-and-signed-downloads).
- Stop and resume keep the retained disk on the same node. There is no
  cross-node or empty-disk fallback; a confirmed missing disk ends in the
  terminal `vm_missing` error.
- A saved-snapshot restore pin is held until the restore is ready, a confirmed
  outcome proves the retained disk is settled (a confirmed archived stop or a
  terminal missing-disk stop), or cleanup is confirmed. A cancelled or failed
  HTTP request does not release it early; a lease expiry eventually recovers
  it.
- Signed receiver tokens are bearer-equivalent secrets. The API role omits
  them from its logs; any access log in front of the API MUST redact the
  receiver token path segment.

## Behavior notes

1. **Streamed commands may exceed 600 seconds.** Synchronous commands keep the
   1–600 second range. `stream: true` accepts up to
   `commands.stream-max-seconds` (default 7200) so long jobs can run. On a
   server without detached commands `bbraft exec --timeout 0` uses that cap; on
   a server advertising `commands.detached`, `--timeout 0` runs detached with
   no timeout.
2. **Stopped sandboxes are deleted after a configurable retention.** An
   archived sandbox's retained disk is deleted with the sandbox (an ordinary
   explicit deletion through the normal deletion path) once it has been
   stopped longer than `-lifecycle.stopped-retention-seconds`, default seven
   days, zero never. A resume of a sandbox whose retained disk the node
   confirms missing ends in a terminal error; bamboo-raft never silently
   boots an empty replacement disk. Likewise, a stop whose target VM and disk
   are confirmed missing after the initial boot settled ends in the fixed
   `vm_missing` failure instead of retrying forever; the row and stop stay
   queryable and a normal `DELETE` still works.
3. **Reaching `ttlSeconds` archives the sandbox** (a durable stop with the disk
   kept), and `ttlSeconds: null` disables auto-stop.

## Streamed command UTF-8 boundaries

Node `Exec` events have a frozen frame order. The HTTP stream preserves each
output stream's own chunk order, but not a cross-stream byte order, because a
UTF-8 rune split across node chunks is completed before it can be emitted.
The boundary policy is:

- At most 3 pending bytes are carried per stream (the incomplete suffix of a
  rune). Complete runes are emitted as soon as their final byte arrives.
- `stdout` and `stderr` frames each preserve their own stream's order. A later
  other-stream frame may therefore precede a rune that is still incomplete;
  there is no cross-stream byte-order promise.
- At `exit` or `error`, any remaining invalid or incomplete tail is emitted in
  the original node-event receive order, with invalid bytes replaced by
  U+FFFD.
- The node `Exec` event ordering itself is unchanged; this is only how the
  HTTP decoder handles split UTF-8.

## Attach byte offsets and reconnect

The attach stream uses raw guest log byte offsets, never rune or character
counts. Cursors advance only when a frame has been fully written and flushed.
Until then, complete output already received from the node and any incomplete
trailing rune are replayed from the last flushed offset, so a reconnect or
retry never loses or repeats a byte.

- `stdout`/`stderr` `nextOffset` advances by all raw bytes consumed into that
  frame, including invalid bytes that become U+FFFD. A client must continue
  with the offset, not with the decoded string length.
- A chunk larger than the 1 MiB HTTP frame bound is split on rune boundaries
  into several frames; each frame advances `nextOffset` and only a fully
  flushed frame advances the reconnect offset.
- At a validated `exit`, the remaining incomplete tail is flushed with U+FFFD
  and the exit reports the raw end of both logs.
- At `reconnect`, `error`, or an EOF without the node's exit event, pending
  bytes are preserved and both offsets point at the last emitted byte. Opening
  the stream again at those offsets replays the pending bytes and completes a
  split rune exactly once.
- Heartbeats arrive after 15 seconds without another line. The stream ends
  with one `reconnect` after ten minutes even mid-chunk; a fresh write deadline
  is used for that frame, not an expired frame deadline.

## Current limits

These are omissions, not changes to the operations we do implement:

- **Detached commands and reconnect now ship.** `detached: true`,
  `commandStatus`, `attachCommand` and `stopCommand` are implemented above.
  The legacy synchronous/stream command behavior and its 7200-second stream
  cap are unchanged. `command-history` and process lists remain out of scope.
- **Saved snapshots are local.** It keeps a frozen disk on one node.
  A create with `from` is placed on the node that holds the artifact. A source
  sandbox can be deleted without deleting the saved artifact. Re-saving a
  completed name creates a new immutable artifact; a late save can never
  replace a newer one.
- **No billing or organizations.** `cost`, `deletedCost`, wallets, plans and
  `org` scoping are not implemented. The list parser refuses the
  `org`, `stoppedBefore`, `dataExpired` and `include` parameters with
  `400 invalid_query` instead of silently ignoring them.
- **Create accepts the full request shape but rejects unsupported features.**
  The parser recognizes every documented request field, including `environment`,
  `failFast`, `count`, `setupScript`, `visibility`, `hostedPorts`, `files`,
  `org`, `teamId` and `from`. Handlers reject the features bamboo-raft does not
  support with a documented error; they never ignore a requested behavior.
  Labels, env, name, type and TTL are supported; the other fields are accepted
  only where their effect is implemented.
- **Upload URLs are server-served.** The URL points at the control plane, is
  bound to owner, sandbox, path and expiry, and streams the body to the node in chunks with no
  application size limit. The URL token is secret-bearing and is redacted from
  logs.
- **Block snapshots, not file snapshots.** A snapshot copies the disk; file
  filtering and file-level snapshot reassembly are not implemented.

## Deletion behavior

`DELETE /sandboxes/{id}` requires `X-Ascii-Confirm-Delete` to exactly equal the
id, returns `202` with `type: "sandbox.deleting"` and a required `operation`;
polling uses `type: "deletion.operation"`. Only the statuses `pending`,
`processing`, `blocked` and `completed` are emitted. The record is owner-scoped
and the operation response is never cached. An unexpected ALREADY_EXISTS from
a node maps defensively to `409` with code `conflict`; that is a fallback only,
not the normal deletion path.

## Saved snapshot behavior

The save job checks the durable in-flight/done artifact row and verifies the
artifact in the node's `Info.snapshots`. A retry that finds a verified artifact
is a success. An unknown artifact collision fails the save as an internal
failure. A concurrent save of the same name returns `409 save_in_progress`.
After a name is saved, it can be saved again under a new immutable artifact.

The saved-snapshot list has no pagination: our handler walks every internal page and
only validates the two documented query parameters (`name`, `namePrefix`).
`POST`, `GET` and `DELETE` ignore query parameters, so they never invent
`400 invalid_query` where the API documents no such answer. Every row and lookup is
owner-scoped; a missing name and a foreign name are the same `404`.

A completed generation stays addressable while a replacement saves or fails.
Restoring by name resolves the newest ready generation, and re-saving a name
records a new immutable artifact. Only a ready or confirmed-cleanup restore can
release its durable artifact pin; a lease expiry or HTTP cancellation does not.
`DELETE /saved-snapshots/{name}` is independent of sandbox deletion: VMs already
booted from the artifact keep working.

`POST /sandboxes` with `from` first replays the owner's idempotency key, then
resolves the snapshot name. That order is required: a retry returns the
originally accepted generation even after the name was deleted or replaced, or
while its node is offline or full. An omitted `type` inherits the snapshot's
canonical type; an explicit type may keep or grow the disk but never shrink it.
Placement uses only the immutable artifact's recorded node and checks its live
`Info` capacity; there is no fallback node and no fresh-image fallback. The
guest boots with `image: ""` and `from_snapshot` set to the artifact, and env
comes only from the create request, never from the source sandbox.

## Run auto-stop behavior

`run` uses a rolling 900-second sandbox TTL. A capable
server (one that advertises `commands.detached: true`) creates the run sandbox
with `ttlSeconds: 900`, verifies the timer with an immediate PATCH before the
sandbox is ready and before any push or command, and renews that exact TTL
every five minutes while setup, push and command are still in progress. A
normal completed `--keep` run sends one final PATCH with `ttlSeconds: 900` and
leaves the sandbox; a default run deletes it through the normal deletion path.
Cancellation, setup failure or an unconfirmed command completion with a known
sandbox id still deletes the sandbox when the CLI can. An ambiguous create has
no known id, so the CLI cannot delete it and reports the ambiguity; only the
finite TTL remains. If `--keep` was requested and the final renewal PATCH
cannot be confirmed, the CLI reports the unconfirmed keep and leaves the
sandbox at its last finite expiry instead of deleting it. A command started
with `--timeout 0` on this server is unlimited and is started exactly once
through the preconfirmed detached path, with no second metadata request.

An old server (no `/api/bbraft/info`, or the capability absent/false) keeps the
legacy fixed TTL: `TTL = max(900, effective timeout + 3720)`, no PATCH renewal,
and the legacy streamed command. `run` still performs its one metadata read
before create, even with an explicit positive timeout at or below 600 seconds;
`exec` does not fetch metadata for such a positive timeout. A missing endpoint
falls back to 600 seconds with one stderr note; a valid response without
`detached` uses its `streamMaxSeconds`. On such a server `run --keep` fails
with exit status 1, code `keep_unsupported`, and no sandbox is created. If a
create response is lost, the CLI reports an ambiguous-create error and relies
on the finite TTL; it never guesses an id or retries automatically. The
7200-second streamed cap is unchanged.

## Pagination and filters

`limit` is an integer from 1 to 200, default 100. `cursor` is opaque and comes
from `pageInfo.nextCursor`; `pageInfo.nextCursor` is always present and is an
explicit `null` on the last page. `sort` is `asc` or `desc` (default `desc`).

Lists order by the `(created_at, id)` pair. `created_at` is stored at
PostgreSQL microsecond precision and `id` is the stable tie-break. A cursor is
exclusive in the requested direction: the next page starts strictly after the
pair. The canonical encoding rejects malformed and non-canonical cursors and
binds a cursor to its collection and sort; it is **not** a cryptographic MAC.
Tenant isolation comes from the owner filter on every query, never from the
cursor. A bad cursor or a repeated scalar parameter is `400 invalid_query`,
naming the parameter.

`state` is a comma-separated filter; `name` is exact; `namePrefix` is a literal
prefix; `label` is `key=value` and repeated to require several labels. Unknown
or unsupported query parameters are refused by name, never ignored.

## Errors

Errors use one envelope:

```json
{
  "ok": false,
  "type": "sandbox.error",
  "status": 400,
  "code": "invalid_query",
  "message": "Unknown query parameter: org",
  "error": {"code": "invalid_query", "message": "...", "status": 400},
  "requestId": "req_..."
}
```

`requestId` is present on every error and safe to quote in support requests.
`error.details.retryable` is set where retrying is safe. Error messages never
contain secrets, API keys, node tokens or upload tokens.

## Test configuration

- `BBRAFT_TEST_REQUIRE_ALL=1`: missing required services (PostgreSQL, KVM) are
  failures instead of skips.
- `BBRAFT_TEST_DATABASE_URL=postgres://...`: run PostgreSQL integration tests;
  each test creates and drops its own schema.
