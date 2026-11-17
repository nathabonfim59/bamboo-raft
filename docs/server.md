# bbraft-server

`bbraft-server` is the bamboo-raft control plane. One binary runs the public
v1 API, the durable lifecycle workers and the node hub. Roles are
separable (`-roles`), and every role opens its own PostgreSQL pool and network
clients; roles share only PostgreSQL, the object store and the network.

## Roles

| Role | Serves |
|---|---|
| `api` | `/api/v1` (core sandbox CRUD, commands, files, SSH key, deletion operations) and `/api/bbraft/info` |
| `lifecycle` | PostgreSQL job workers, placement provisioning/deletion, TTL reaper and VM reconciliation |
| `nodehub` | node dial-in listener (`nodelink`), authenticated internal node gateway, inventory poller |

`-roles all` (the default) runs all three in one process with the same
boundaries as separate processes. To split them, run one process per role:

```sh
bbraft-server -roles nodehub  -database.url-file /etc/bbraft/database-url ...
bbraft-server -roles api      -database.url-file /etc/bbraft/database-url ...
bbraft-server -roles lifecycle -database.url-file /etc/bbraft/database-url ...
```

Startup converges safely in any order: every role migrates the schema under
the accepted PostgreSQL locks, and the API/lifecycle roles dial the gateway
lazily. A node that reconnects replaces its old link.

## Required inputs

| Input | Flag | Secret source |
|---|---|---|
| PostgreSQL URL | `-database.url-file` | file contents or `BBRAFT_DATABASE_URL` |
| internal gateway credential | `-node.gateway-token-file` | file contents or `BBRAFT_NODE_GATEWAY_TOKEN` |
| installation master key | `-secrets.master-key-file` | exactly 32 raw bytes, or 64 hex characters in `BBRAFT_SECRETS_MASTER_KEY` |
| API keys (`api`) | `-auth.keys-file` | `{"keys":[{"id":"main","owner":"<account>","key":"bmb_..."}]}` |
| node token digests (`nodehub`) | `-node.tokens-file` | `{"nodes":[{"id":"node-example","tokenSha256":"<64 lowercase hex>"}]}` |

Raw keys are hashed when the file is loaded and never stored in `auth.Keys`.
Each configured raw key must start with `-auth.key-prefix` (default `bmb_`);
a configured or presented token without the public prefix is refused before a
digest comparison. A configured bad prefix fails startup with a fixed
record-index error that never echoes the key. `-secrets.allow-ephemeral`
generates a throwaway master key and is only for local development and tests;
such a key decrypts nothing after a restart.

## Listeners

| Flag | Default | Env |
|---|---|---|
| `-server.listen` | `127.0.0.1:7700` | `BBRAFT_SERVER_LISTEN` |
| `-server.tls-cert-file` / `-server.tls-key-file` | none | `BBRAFT_SERVER_TLS_CERT_FILE` / `BBRAFT_SERVER_TLS_KEY_FILE` |
| `-server.public-url` | derived from `server.listen` | `BBRAFT_SERVER_PUBLIC_URL` |
| `-node.listen` | `127.0.0.1:7701` | `BBRAFT_NODE_LISTEN` |
| `-node.tls-cert-file` / `-node.tls-key-file` | none | `BBRAFT_NODE_TLS_CERT_FILE` / `BBRAFT_NODE_TLS_KEY_FILE` |
| `-node.gateway-listen` | `127.0.0.1:7702` | `BBRAFT_NODE_GATEWAY_LISTEN` |
| `-node.gateway-tls-cert-file` / `-node.gateway-tls-key-file` | none | `BBRAFT_NODE_GATEWAY_TLS_CERT_FILE` / `BBRAFT_NODE_GATEWAY_TLS_KEY_FILE` |
| `-node.gateway-dial` | `-node.gateway-listen`, wildcard → `127.0.0.1` | `BBRAFT_NODE_GATEWAY_DIAL` |
| `-node.gateway-ca-file` | system roots | `BBRAFT_NODE_GATEWAY_CA_FILE` |
| `-node.gateway-server-name` | dial host | `BBRAFT_NODE_GATEWAY_SERVER_NAME` |

Transport rules (enforced at startup):

- A plaintext `node.listen` is allowed only on loopback or private addresses.
- The internal gateway needs its TLS pair off loopback.
- The API/lifecycle gateway client uses TLS whenever the gateway TLS pair is
  configured. `InsecureSkipVerify` is never used. `-node.gateway-ca-file` is a
  PEM bundle for a private CA; `-node.gateway-server-name` overrides the
  verified certificate name when it differs from the dial host.
- `-node.gateway-dial` separates the address clients dial from the address the
  gateway binds. A wildcard gateway bind defaults to loopback for dialing.
- `-server.public-url` is the trusted absolute base URL for signed links. It
  is **required** when `server.listen` binds a wildcard address; the daemon
  never advertises `0.0.0.0` or an empty host. Non-wildcard defaults use
  `http` (or `https` with the server TLS pair) and the configured host:port.
  Request `Host` and proxy headers are never trusted. The URL must be
  absolute `http`/`https`, have a hostname, and carry no userinfo, query or
  fragment.

## Authentication bootstrap

At startup the `api` role loads the key file once, creates or maps every
configured owner name to a durable account UUID (`EnsureAccount`), and
persists every key digest under its configured key id (`EnsureAPIKey`). On
each request the bearer middleware:

1. validates the `Bearer` token and public prefix in `auth.Keys`;
2. maps the configured owner name and key id to the durable account and
   `api_keys` UUIDs;
3. records `last_used_at` under a bounded request context;
4. only then calls a handler.

A storage failure in step 3 is a safe `500 http_500` before any handler
effect, never a silent `401`. Foreign-owner resources answer the same `404`
as missing ones.

## HTTP behavior

- Method-pattern `http.ServeMux` routes; unmatched paths and wrong methods
  answer the API error envelope (`not_found` / `method_not_allowed`).
- `apierr.RequestID` adds `X-Request-Id` to every response. Responses are
  `Cache-Control: no-store`.
- `GET /healthz` is unauthenticated liveness only: no build, database, host
  or node details.
- Header time and size are bounded (`ReadHeaderTimeout`, `IdleTimeout`,
  `MaxHeaderBytes`). There is deliberately no short `ReadTimeout` or
  `WriteTimeout`, so streamed commands and uploads can run long.
- `POST /api/v1/sandboxes/{sandboxId}/files/upload-url` issues a signed,
  short-lived upload URL under bearer authentication.
- `POST /api/v1/sandboxes/{sandboxId}/files/download-url` issues a signed,
  short-lived download URL under bearer authentication.
- The eight core file routes are registered on the authenticated `/api/v1`
  mux: `GET`/`PUT`/`DELETE /files`, `GET /files/list`, `GET /files/info`,
  `POST /files/folder`, `POST /files/move` and `POST /files/permissions`.
  They read the owner-scoped sandbox through the same lookup the command
  routes use, wait up to 60 seconds for a starting sandbox, cap inline reads
  and writes at 5 MiB and stream with the frozen node contract.
- `POST /api/v1/sandboxes/{sandboxId}/sshkey` installs one OpenSSH public key
  through the node's `AddSSHKey` under bearer authentication and returns
  `machineIp`, `sshUser`, `sshEndpoint` and the public `hostKey`. It reads the
  owner-scoped sandbox, refuses a non-running or stale boot with `400
  machine_not_running` (never `409`), and stores the endpoint and host key only
  after a successful installation for the same node and lifecycle generation.
  The API exports the pair as the nullable `sshEndpoint` on `get` and list only
  in `ready`/`idle`/`running`; stop, resume or a terminal error clears it.
  `UNAVAILABLE` from the node is `503 ssh_host_key_unavailable` with
  `retryAfterSeconds: 5` and stores nothing; node failures are `502
  sandbox_direct_failed`. The guest installs the forwarded public key in
  `authorized_keys`; the control plane does not persist user public or private
  keys, and never logs key material. The host key is public by design.
  Nodefake returns reference metadata only: this route makes no real SSH
  reachability promise.
- `PUT /api/bbraft/uploads/{token}` and `GET /api/bbraft/downloads/{token}`
  are the two signed receivers and the only API routes outside bearer
  authentication: the HMAC-signed token is the credential and authorizes one
  owner, sandbox and canonical path. The upload receiver streams the raw
  body to the guest in bounded chunks and answers with the extension-only
  `file.uploaded` envelope (`path`, `size`). The download receiver validates
  the node's first message before writing headers, then streams raw
  `application/octet-stream` bytes with backpressure and an `attachment`
  filename; after the headers a node failure aborts the connection instead
  of appending an error to the file. Invalid tokens answer the same safe
  `404`. Absolute URLs always come from the trusted `server.public-url`,
  never from the request `Host` or proxy headers. Both routes are documented
  in `api/bbraft-ext.yaml`.

The API role omits the public receiver token paths from its logs;
`TestRunReceiverTokensNeverReachRoleLogs` proves an issued and a rejected token
never reach the role's log output for upload or download. The server does not
ship a general access-log redactor. Any access log in front of the API MUST
redact the receiver token path segment or record only the route template.

## Lifecycle

The `lifecycle` role uses its own store and gateway dialer. Intervals:

| Flag | Default | Env |
|---|---|---|
| `-lifecycle.retry-seconds` | `5` | `BBRAFT_LIFECYCLE_RETRY_SECONDS` |
| `-lifecycle.reaper-seconds` | `60` | `BBRAFT_LIFECYCLE_REAPER_SECONDS` |
| `-lifecycle.job-poll-seconds` | `15` | `BBRAFT_LIFECYCLE_JOB_POLL_SECONDS` |
| `-lifecycle.stopped-retention-seconds` | `604800` (seven days) | `BBRAFT_LIFECYCLE_STOPPED_RETENTION_SECONDS` |

Handlers are fixed at worker construction. The lifecycle role constructs the
saved-snapshot plugin once, passes `plugin.Handlers()` through
`Options.Handlers` and runs the snapshot reconciler beside the worker under the
same role context, joining both on shutdown. No mutable handler registration
exists. The API role registers the four `/api/v1/saved-snapshots` routes and
the create-from restore adapter directly (replay before resolution).

### Auto-stop and stopped-disk retention

Reaching a sandbox's `ttlSeconds` deadline **archives** the sandbox (a durable
stop: the VM is stopped, its disk is kept on the same node, and the vCPU and
memory reservation is released) instead of deleting it. Acceptance is
transactional against the deadline: a concurrent TTL extension or manual stop
wins. `ttlSeconds: null` disables auto-stop.

An archived sandbox keeps its retained disk until one of:

1. a resume boots it again on the same node (capacity is re-reserved
   atomically there),
2. the stopped-retention reaper deletes it, or
3. an explicit `DELETE`.

The stopped-retention reaper deletes sandboxes that have been stopped longer
than `-lifecycle.stopped-retention-seconds` (default seven days; `0` disables
retention deletion; the value must be nonnegative and fit a duration). It
records an ordinary explicit deletion and uses the normal deletion acceptance
and delete worker: the acceptance rechecks state, stopped time and concurrent
stop/resume work under the row lock, so an old candidate can never delete a
sandbox that resumed meanwhile, and capacity is released exactly once. A
resume that fails against a missing retained disk ends in a terminal error
without creating a replacement disk.

## Node inventory

The `nodehub` role polls connected nodes, writes healthy info through
`UpsertNodeInfo` with the observation time, and keeps canonical reservation
counters untouched. A disconnected node or a failed `Info` call immediately
marks the row stale (`TouchNodeSeen` with the Unix epoch) so placement cannot
use old capacity. Epoch ordering is enforced by the accepted gateway
inventory poller, not by a second role-local state machine. An offline row
that was never registered is benign.

## URL signing keys

The upload and download URL signing keys are derived once per API instance with
HKDF-SHA256 from the installation master key under the fixed, separate
contexts `bbraft/upload-url-signing/v1` and
`bbraft/download-url-signing/v1`. They are therefore stable across API
instances, isolated from the database encryption key and independent from each
other: an upload token can never verify as a download token even if the master
key is the same. No extra flag, environment variable or configuration secret
exists. Both keys are key material: never log them, and never inject them into
a guest or node.

Every API process in one installation must use the identical master key, which
produces the identical HMAC keys. A process with a different master key issues
tokens the other API processes cannot verify. Rotating the master key (and
therefore both signing keys) invalidates outstanding upload and download URLs
immediately: already-issued links fail verification and must be reissued.

The download token also carries a version, purpose, owner, sandbox, canonical
path, expiry and nonce, and is signed over a download-specific domain with
HMAC-SHA256. The receiver validates every claim before contacting the store or
a node, and never logs the token or a request URI that contains it.

## Commands and limits

`-commands.stream-max-seconds` (default `7200`, env
`BBRAFT_COMMANDS_STREAM_MAX_SECONDS`) is the actual streamed-command cap.
`/api/bbraft/info` reports that cap, the 600-second synchronous cap and the
optional `commands.detached: true` capability. Detached execution is additive:
an old server omits the member or sends `false`, and a client keeps the legacy
synchronous/streamed path. The 7200-second cap is unchanged. The command
handler waits for a starting sandbox for at most 60 seconds and maps only an
owner-scoped missing target to `404 not_found`.

## Shutdown

On SIGINT/SIGTERM the daemon closes listeners, cancels and joins every role,
gives HTTP a bounded graceful shutdown, then force-closes remaining
connections so slow bodies observe cancellation. Cancellation and force-close
are followed by an unconditional handler join; the graceful bound is never a
promise to abandon a stuck handler. The role's Store is closed only after
that join. A startup or runtime listener failure cancels the role-local
request context, closes the listener and joins the same way. Startup/runtime
errors cross the executable boundary with a fixed classification; driver
text (which can contain a DSN or credential) is never printed.

## Testing

Focused unit tests run without a database:

```sh
go test ./internal/server/ ./internal/server/config/ ./internal/auth/
```

PostgreSQL integration tests (one isolated schema per test, invoking `Run`
in-process) skip unless the database is provided:

```sh
BBRAFT_TEST_DATABASE_URL=postgres://... go test ./internal/server/
```

The SSH metadata lifecycle and receiver-token log regressions use the same
gate:

```sh
BBRAFT_TEST_DATABASE_URL=postgres://... go test ./internal/server/ -run 'TestRunSSHKey|TestRunReceiverTokens'
```

With `BBRAFT_TEST_REQUIRE_ALL=1`, a missing database is a
failure instead of a skip (the test VM base sets this).
