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

## Phase 2 configuration

These settings turn on the object-store features: snapshots, fork, cross-node
resume, node loss, retention and disk-pressure eviction. Object storage is off
unless `-s3.bucket` (or `BBRAFT_S3_BUCKET`) is set; see
[Object storage](#object-storage) for the secret-file rules and
[Lifecycle](#lifecycle) for when each worker runs.

| Setting | Flag | Env | Default |
|---|---|---|---|
| [Object-store endpoint](#object-storage) | `-s3.endpoint` | `BBRAFT_S3_ENDPOINT` | empty (AWS S3) |
| Object-store region | `-s3.region` | `BBRAFT_S3_REGION` | `us-east-1` on AWS; required with a custom endpoint |
| Bucket | `-s3.bucket` | `BBRAFT_S3_BUCKET` | empty (object store off) |
| Path-style addressing | `-s3.path-style` | `BBRAFT_S3_PATH_STYLE` | `false` |
| Access key id | `-s3.access-key-id` | `BBRAFT_S3_ACCESS_KEY_ID` | none; required with a bucket |
| Secret access key file | `-s3.secret-access-key-file` | `BBRAFT_S3_SECRET_ACCESS_KEY_FILE` (or `BBRAFT_S3_SECRET_ACCESS_KEY`) | none; required with a bucket |
| Create the bucket | `-s3.create-bucket` | `BBRAFT_S3_CREATE_BUCKET` | `false` |
| Created-bucket location hint | `-s3.location-hint` | `BBRAFT_S3_LOCATION_HINT` | empty (R2 only) |
| [Snapshot interval](#lifecycle) | `-lifecycle.snapshot-interval-seconds` | `BBRAFT_LIFECYCLE_SNAPSHOT_INTERVAL_SECONDS` | `60` |
| Snapshot call timeout | `-lifecycle.snapshot-timeout-seconds` | `BBRAFT_LIFECYCLE_SNAPSHOT_TIMEOUT_SECONDS` | `600` |
| [Node-loss grace](#node-loss) | `-lifecycle.node-lost-seconds` | `BBRAFT_LIFECYCLE_NODE_LOST_SECONDS` | `120` (`0` off; else at least `60`) |
| [Data retention](#auto-stop-and-stopped-disk-retention) | `-lifecycle.data-retention-seconds` | `BBRAFT_LIFECYCLE_DATA_RETENTION_SECONDS` | `1209600` (fourteen days; `0` never) |
| [Disk-pressure threshold](#disk-pressure-eviction) | `-lifecycle.disk-low-percent` | `BBRAFT_LIFECYCLE_DISK_LOW_PERCENT` | `10` (`0` off; else `1`-`90`) |
| [Stopped-row retention](#auto-stop-and-stopped-disk-retention) | `-lifecycle.stopped-retention-seconds` | `BBRAFT_LIFECYCLE_STOPPED_RETENTION_SECONDS` | `604800` without an object store, `0` with one |

The snapshot worker, node loss, object-store data retention and eviction are
inert without an object store, and so is chunk garbage collection (its `-gc.*`
flags are in [its own section](#chunk-garbage-collection)); the stopped-row
retention still applies.

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
| `-lifecycle.stopped-retention-seconds` | `604800` (seven days) without an object store, `0` (off) with one | `BBRAFT_LIFECYCLE_STOPPED_RETENTION_SECONDS` |
| `-lifecycle.data-retention-seconds` | `1209600` (fourteen days; `0` never) | `BBRAFT_LIFECYCLE_DATA_RETENTION_SECONDS` |
| `-lifecycle.snapshot-interval-seconds` | `60` | `BBRAFT_LIFECYCLE_SNAPSHOT_INTERVAL_SECONDS` |
| `-lifecycle.snapshot-timeout-seconds` | `600` | `BBRAFT_LIFECYCLE_SNAPSHOT_TIMEOUT_SECONDS` |
| `-lifecycle.node-lost-seconds` | `120` (`0` disables, else at least `60`) | `BBRAFT_LIFECYCLE_NODE_LOST_SECONDS` |
| `-lifecycle.disk-low-percent` | `10` (`0` disables, else `1`–`90`) | `BBRAFT_LIFECYCLE_DISK_LOW_PERCENT` |

With an object store configured, every ready sandbox with snapshots on gets a
generation every `-lifecycle.snapshot-interval-seconds` (±10 % jitter): the
lifecycle role enqueues one `sandbox.snapshot` job per due sandbox, and the
job asks the sandbox's node for `SnapshotVM` under the per-sandbox guard,
bounded by `-lifecycle.snapshot-timeout-seconds`. A node that refuses
(`FAILED_PRECONDITION`, for example a node without an object store) is logged
once per sandbox and node and retried after ten intervals; the sandbox is
never failed for it. A stop writes a final generation from the stopped disk
after the VM is off; if that fails the stop is `failing` and retries, unless
`force` was asked, which stops with the previous generation. A sandbox created
with `snapshots: false` is never uploaded: its stop deletes the VM and its
disk, and it cannot be resumed (`409 snapshots_disabled`). Without an object
store the role behaves as in Phase 1: no snapshot jobs, and stops keep only
the node-local disk.

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
2. its stored data expires (see below),
3. the stopped-retention reaper deletes it,
4. a disk-pressure eviction gives the disk up (below), or
5. an explicit `DELETE`.

The stopped-retention reaper deletes sandboxes that have been stopped longer
than `-lifecycle.stopped-retention-seconds` (`0` disables retention deletion;
the value must be nonnegative and fit a duration). Without an object store the
default is seven days; with one the default is `0` (off), because the data
itself expires instead and the row should survive so the client can still see
what happened to its sandbox. It records an ordinary explicit deletion and uses
the normal deletion acceptance and delete worker: the acceptance rechecks
state, stopped time and concurrent stop/resume work under the row lock, so an
old candidate can never delete a sandbox that resumed meanwhile, and capacity
is released exactly once. A resume that fails against a missing retained disk
ends in a terminal error without creating a replacement disk.

With an object store configured, the **data-retention reaper** expires the
stored data of an archived sandbox stopped longer than
`-lifecycle.data-retention-seconds` (`BBRAFT_LIFECYCLE_DATA_RETENTION_SECONDS`,
default 14 days; `0` never expires data). Each candidate is accepted in its own
transaction that rechecks state, stopped time, expiry and concurrent
stop/resume work under the row lock, oldest stop first. Acceptance sets
`dataExpiredAt` on the sandbox, releases the retained disk reservation exactly
once, clears the local disk reference and records an eviction so VM
reconciliation deletes the copy on the node that held it. The sandbox row
stays in `archived` with `dataExpiredAt` until an explicit `DELETE`, and an
event `sandbox.data_expired` records the transition. Resume, fork and
`POST /saved-snapshots` from an expired sandbox answer
`409 snapshot_data_expired`. Saved snapshots never expire: they are independent
artifacts, and one taken before the source expired stays restorable.

Without an object store there is no stored data to expire, so data retention is
always off and Phase 1's stopped-disk deletion above is the only reaper
window.

### Node loss

With an object store configured (without one there is nowhere to continue
from, so node loss stays off), a node is **lost** when the inventory has not
observed it connected for
`-lifecycle.node-lost-seconds` (default 120 s; `0` turns node loss off, and any
other value must be at least 60 s, two inventory polls). A link that drops and
comes back within that grace moves nothing. The reaper checks every round and,
for each sandbox of a lost node, decides under the sandbox's guard (so no
snapshot, stop or resume call is in flight for it) and commits in one
transaction that rechecks the row and that the node is still lost, so two
lifecycle workers never act twice on one sandbox:

| Sandbox on the lost node | Snapshot generation | Outcome |
|---|---|---|
| ready / idle / running | completed generation | moved: `provisioning` on another node, which restores the last generation (`sandbox.moved` event, chain continued); writes after that generation are lost |
| ready / idle / running | none, or snapshots off | `error`, reason `node_lost` |
| archived, disk on the node | completed generation | stays archived with its disk only in the object store; the next resume restores it on another node |
| archived, disk on the node | none | unchanged: that disk is the only copy, and resume waits for the node |
| archiving (stop in flight) | completed generation | stop completed, archived from that generation; later writes are lost |
| archiving (stop in flight) | none | unchanged: the stop keeps retrying |
| provisioning | | left to its job |

A move places the sandbox like a cross-node resume: never on the lost node or
a node that still holds an evicted copy, only on nodes with the generation's
image and a working object store, with a disk never smaller than the
generation's. When no node has room the sandbox stays where it is (one
warning) and is moved on a later round. If the restore then fails for good,
the sandbox ends stopped with no local disk, like a failed cross-node resume.
Every outcome releases the lost node's reservation and records its copy as an
eviction: when the node returns, VM reconciliation deletes that copy before
anything else looks at its inventory, so it is never reported unknown,
started or snapshotted, and capacity accounting never counts it. Until then
the old VM may still run on a node that is only cut off from the control
plane; nothing reaches it through the API.

Node loss therefore assumes the lost node is really gone. A node that is only
cut off from the server keeps running its copy of a moved sandbox, guest
network included, until it reconnects; reconciliation then deletes that copy
first, and anything written to it after the sandbox's last completed
generation is lost. This release has no node-side self-fence: a node never
stops its own VMs because it has lost the server. A later candidate is for the
node to stop its VMs once it has been out of touch with the server for longer
than the grace.

Three guards keep an outage of the control plane itself from looking like
node loss:

1. **Startup.** A lifecycle worker does nothing for node loss until it has
   run for the grace, so nodes can reconnect after a server restart.
2. **Observer.** A node counts as lost only while at least one other node was
   observed within the grace. When every node is stale at once, the
   inventory poller (the `nodehub` role) is the likelier failure, and with
   split roles a `nodehub` outage would otherwise move or fail every sandbox
   of the install. A single-node install therefore never declares its node
   lost: its sandboxes wait for that node, which is also the only place they
   could run.
3. **Live probe.** A node whose `Info` still answers through the gateway is
   not lost, whatever its inventory row says.

A `nodehub` restarted alone while the lifecycle role keeps running can still
see a node that reconnects slowly as lost once another node has reconnected;
the live probe narrows that window, and running the roles in one process (the
default) avoids it.

### Disk-pressure eviction

With an object store configured (without one a stopped sandbox's retained
disk is the only copy, so eviction stays off), each reaper round finds the
nodes whose reported free disk is below `-lifecycle.disk-low-percent` of
their total (default 10; `0` disables, and values above 90 are refused).
On each pressured node it gives up retained stopped disks, least recently
stopped first, until the node's estimated free disk reaches the threshold;
the next inventory poll corrects the estimate.

A disk is only ever given up when it equals the stop's final generation —
the completed generation cut at or after the stop was requested — so the
eviction loses nothing. The eviction commits first: one transaction that
rechecks the row releases the disk reservation, clears the node and
local-disk pointers, records a `sandbox.disk_evicted` event with reason
`disk_pressure` and opens an eviction row for the copy. A resume admitted
after the commit already sees no local disk and restores from the store.
The node's copy is then deleted — right away, or by VM reconciliation when
the first delete fails, before anything else uses that node's inventory. A
stopped sandbox without that generation is never a candidate. After the
eviction the disk exists only in the object store, and a later resume
restores it through the ordinary resume path, possibly on the same node.

## Chunk garbage collection

With an object store configured, the lifecycle role runs a collector that
deletes chunk and manifest objects no generation can restore any more. Only the
server deletes objects.

| Flag | Default | Env |
|---|---|---|
| `-gc.interval-seconds` | `21600` (six hours) | `BBRAFT_GC_INTERVAL_SECONDS` |
| `-gc.grace-seconds` | `86400` (one day) | `BBRAFT_GC_GRACE_SECONDS` |
| `-gc.history-seconds` | `86400` (one day) | `BBRAFT_GC_HISTORY_SECONDS` |
| `-gc.batch` | `1000` | `BBRAFT_GC_BATCH` |

A generation is **live**, and its manifest and chunks are kept, when it is:

1. the latest completed generation of the chain of a sandbox that is neither
   deleted nor expired, or a completed generation of such a sandbox cut within
   `-gc.history-seconds`;
2. the generation such a sandbox was restored from (create from a generation,
   fork, resume on another node, node loss), until the sandbox has a completed
   generation of its own that was cut after the restore;
3. newer than the latest completed generation of such a sandbox, in any status
   (a failed attempt may have written a manifest whose chunks the next attempt
   skips uploading) and its manifest exists;
4. pinned by a saved snapshot that is not deleted or retired, or by a queued or
   running job (a fork, restore or resume reads it);
5. pending, for any sandbox: an attempt may still be writing it.

A round marks every manifest in the store that is not a sweep candidate (live,
or younger than `-gc.grace-seconds`) and deletes manifests of generations that
are not live and older than the grace, then chunks that no marked manifest
names and that are older than the grace. A listing or read error aborts the
round: nothing is deleted that the round could not prove dead. A deleted
manifest's generation is marked purged and leaves the listings.

An attempt may skip uploading a chunk that exists, so a sweep must not delete a
chunk an attempt is about to reference. The collector deletes in batches of
`-gc.batch` objects, each under a fence: it sets the `sweeping` flag in the
database (a snapshot attempt does not begin while it is set; the snapshot job
retries a few seconds later), waits until no attempt that could still be writing
remains (pending, or failed by a takeover before its deadline), re-reads the
generations that became live meanwhile, and only then purges the batch and
deletes its objects before clearing the flag. An attempt whose owner died keeps
the sweep waiting until its deadline plus a minute, and so does a failed or
taken-over one while its call may still run: only the attempt's own deadline
proves that call ended. The wait is capped at
`-lifecycle.snapshot-timeout-seconds` plus that minute, after which the batch is
dropped, the flag cleared and the round stops (the next round starts over). Two lifecycle roles never sweep at once: the
second finds the flag set and skips its round.

If a collector dies mid-sweep the flag stays set and snapshot attempts are
refused. The flag records when its sweep started; one older than the wait cap
plus ten minutes is a crashed sweep's. The collector clears such a flag at
start and every minute, and a round that finds one takes it over.

Each round logs one `gc round finished` line at info level with the manifests
and chunks marked, chunks kept, chunks and manifests deleted, bytes freed and
the number of batches.

## Object storage

Snapshot data lives in one S3-compatible bucket per installation: Garage
(the default, self-hosted), Cloudflare R2, AWS S3 or another service that
offers the core S3 operations. Configuration selects the service; no other
part of the deployment changes. The `bbraft-server` and `bbraft-node` binaries
share one `-s3.*` flag set, so the same variable names configure both.

| Flag | Env | Notes |
|---|---|---|
| `-s3.endpoint` | `BBRAFT_S3_ENDPOINT` | `http(s)://host[:port]`; empty means AWS S3 |
| `-s3.region` | `BBRAFT_S3_REGION` | the service's configured region, `auto` for R2, the bucket's region on AWS |
| `-s3.bucket` | `BBRAFT_S3_BUCKET` | one bucket per install; key prefixes separate data kinds |
| `-s3.path-style` | `BBRAFT_S3_PATH_STYLE` | `true` for Garage and most self-hosted services |
| `-s3.access-key-id` | `BBRAFT_S3_ACCESS_KEY_ID` | credential identifier |
| `-s3.secret-access-key-file` | `BBRAFT_S3_SECRET_ACCESS_KEY_FILE` | file holding the secret |
| `-s3.location-hint` | `BBRAFT_S3_LOCATION_HINT` | optional, R2 only, applied when the bucket is created |
| `-s3.create-bucket` | `BBRAFT_S3_CREATE_BUCKET` | create the bucket if it does not exist |

A flag given on the command line wins over its `BBRAFT_S3_*` variable. The
secret access key never comes from a flag: it is read from
`BBRAFT_S3_SECRET_ACCESS_KEY` or from the file named by
`-s3.secret-access-key-file`, never both. The file must be a regular file
with mode `0600` or stricter, and a symlink is refused; one trailing newline
is ignored. The key is never written to logs, errors or configuration dumps;
startup failures name the object store and never the credential.

Startup rules: a bucket needs an access key id and a secret from one of those
two sources. With no bucket anywhere the object store is off. The endpoint,
region and bucket are then validated by the object-store implementation when
the server connects, and a refused endpoint or unreachable bucket fails
start-up.

**Phase 1 mode.** Without `-s3.bucket` the server runs exactly as before: no
snapshot uploads, local stop/resume and node-local saved snapshots. Fork and
cross-node resume are refused until an object store is configured.

**Service notes.** Only core S3 operations are used: PUT, GET with ranges,
HEAD, DELETE, ListObjectsV2 and presigned GET/PUT. There is deliberately no
multipart upload: every object is one PUT (a snapshot chunk is at most a few
MiB compressed, a manifest at most 128 MiB; a single PUT reaches 5 GiB).
Chunk integrity is computed and verified by bamboo-raft, keys are immutable,
and retention belongs to the server, so bucket lifecycle rules, versioning,
tagging and ACLs are never required. Presigned URLs are
signed for the configured endpoint, so a self-hosted endpoint must be
reachable by whoever follows the URL, or the server streams the object
itself.

## Node inventory

The `nodehub` role polls connected nodes, writes healthy info through
`UpsertNodeInfo` with the observation time, and keeps canonical reservation
counters untouched. A disconnected node or a failed `Info` call immediately
marks the row stale (`TouchNodeSeen` with the Unix epoch) so placement cannot
use old capacity. Only a connected observation moves `last_up_at`, the clock
node-loss detection reads, so the stale marker never makes a short blip look
like a lost node. Epoch ordering is enforced by the accepted gateway
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
