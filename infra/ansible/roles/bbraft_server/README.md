# bbraft_server

Installs `bbraft-server`, the bamboo-raft control plane, on Debian 13. The
caller supplies a **built binary** and every credential; there is no download,
no generated secret and no real default password or key in this role.

The role creates a system user, the configuration/state directories, the
credential files and a hardened systemd unit whose `ExecStart` contains only
flags and file paths. Flag names and defaults come from the accepted
`internal/server/config` package. The role is not wired into `site.yml` yet:
the shared playbook, inventory and `requirements.yml` belong to the lead.

Dependencies: none beyond `ansible-core` (2.22.0 tested). Everything is
`ansible.builtin`; the role-local filters use the Python standard library only.

## Required variables

| Variable | Meaning |
|---|---|
| `bbraft_server_binary_src` | path on the controller to the built `bbraft-server` binary |
| `bbraft_server_database_url` | `postgres://` or `postgresql://` URL; written to a protected file |
| `bbraft_server_gateway_token` | shared internal node-gateway credential, single line |
| `bbraft_server_master_key_source` | path on the controller to a file of exactly 32 **raw** bytes |
| `bbraft_server_api_keys` | list of `{id, owner, key}`; `key` is the raw API key; required for `roles: api` / `all` |
| `bbraft_server_node_tokens` | list of `{id, tokenSha256}`; digest only, 64 lowercase hex; ids unique, digests may repeat; required for `roles: nodehub` / `all` |

There is no default for the four required scalar secrets, and the optional
object-store secret below has none either. The role stops before
creating a user or a unit when one is empty. An empty API-key or node-token
list stops the run when its role is selected. The master key is copied
byte-for-byte (`remote_src: false` by default; set
`bbraft_server_master_key_remote_src: true` when the source already exists on
the target), then the **copied** file is checked with `stat` for exactly 32
bytes. `-secrets.allow-ephemeral` is never rendered: production always needs a
real master key.

Place the raw secrets in a vault or the gitignored inventory. Placeholders in
`tests/vars.yml` are for the local render test only.

## Files and formats

| File (default) | Mode | Content |
|---|---|---|
| `/usr/local/bin/bbraft-server` | `0755` | caller-built binary |
| `/etc/bbraft-server/database-url` | `0640` root:bbraft-server | the URL, one line |
| `/etc/bbraft-server/gateway-token` | `0640` root:bbraft-server | the gateway token, one line |
| `/etc/bbraft-server/secrets-master-key` | `0640` root:bbraft-server | exactly 32 raw bytes, no trailing newline |
| `/etc/bbraft-server/api-keys.json` | `0640` root:bbraft-server | `{"keys":[{"id":"main","owner":"acct_...","key":"bmb_..."}]}` |
| `/etc/bbraft-server/node-tokens.json` | `0640` root:bbraft-server | `{"nodes":[{"id":"node-example","tokenSha256":"<64 lowercase hex>"}]}` |
| `/etc/bbraft/s3.env` | `0600` root:root | only when the object store is enabled: the five non-secret `BBRAFT_S3_*` settings |
| `/etc/bbraft/s3-secret-access-key` | `0600` root:root | only when the object store is enabled: the secret access key alone |
| `/etc/systemd/system/bbraft-server.service` | `0644` root:root | hardened unit |

The JSON shapes match `auth.LoadKeysFile` and `auth.LoadNodeTokensFile`, which
reject unknown members. A node-token entry with a raw `token` field is
rejected by the role before it can reach a file; store only the SHA-256 digest
of the token the node presents. Node ids must be unique; the loader permits two
ids to share a digest. `/etc/bbraft-server` is `0750` root:bbraft-server and
`/var/lib/bbraft-server` is `0750` bbraft-server:bbraft-server.

## Unit and flags

Default listeners are the accepted Phase 0.5 values:

| Flag | Default |
|---|---|
| `-roles` | `all` |
| `-server.listen` | `127.0.0.1:7700` |
| `-node.listen` | `127.0.0.1:7701` |
| `-node.gateway-listen` | `127.0.0.1:7702` |
| `-commands.stream-max-seconds` | `7200` |
| `-lifecycle.retry-seconds` / `-reaper-seconds` / `-job-poll-seconds` | `5` / `60` / `15` |
| `-lifecycle.snapshot-interval-seconds` / `-snapshot-timeout-seconds` | `60` / `600` |
| `-lifecycle.node-lost-seconds` | `120` (`0` disables, else at least `60`) |
| `-lifecycle.data-retention-seconds` | `1209600` (fourteen days; `0` never expires) |
| `-lifecycle.disk-low-percent` | `10` (`0` disables, else `1`-`90`) |
| `-auth.key-prefix` | `bmb_` |

Credential flags point at the files above: `-database.url-file`,
`-node.gateway-token-file`, `-secrets.master-key-file`, and (when supplied)
`-auth.keys-file` and `-node.tokens-file`. The API-key and node-token flags
are omitted when their lists are empty, so a lifecycle-only server never points
at a file it does not write.

Optional TLS pairs: `bbraft_server_tls_cert_file` / `_key_file`,
`bbraft_server_node_tls_cert_file` / `_key_file`,
`bbraft_server_gateway_tls_cert_file` / `_key_file`. Each pair must be set
together or not at all. The role mirrors the binary's transport rules:

- a plaintext node listener is accepted only on loopback or private
  (RFC 1918 / fc00::/7) addresses and link-local; a public address needs the
  node TLS pair;
- the internal gateway must stay on loopback unless it has its TLS pair.

All three listeners are syntax-checked even when TLS is enabled:
`server.listen`, `node.listen` and `node.gateway-listen` must be `host:port`
with ASCII decimal digits in 1-65535. IPv6 must be bracketed; an unbracketed
`::1:7701` is rejected, as Go's `net.SplitHostPort` rejects it. IPv4-mapped
IPv6 addresses are classified by their IPv4 address
(`[::ffff:127.0.0.1]` is loopback, `[::ffff:10.0.0.1]` is private), matching
`netip`.

## Public URL and gateway client settings

These optional inputs mirror `server.public-url`, `node.gateway-dial`,
`node.gateway-ca-file` and `node.gateway-server-name` in the accepted
`internal/server/config`. Their role defaults are empty and the flags are
omitted from `ExecStart` when unset, so the binary's own defaults stay
authoritative.

| Variable | Meaning |
|---|---|
| `bbraft_server_public_url` | trusted absolute base URL clients use for the API; signed upload URLs are built from it, never from request `Host` or proxy headers |
| `bbraft_server_gateway_dial` | client target the api and lifecycle roles dial for the internal gateway |
| `bbraft_server_gateway_ca_file` | target path to an optional PEM CA bundle that verifies the gateway certificate |
| `bbraft_server_gateway_server_name` | optional certificate name to verify instead of the dial host |

`bbraft_server_public_url` must be an absolute `http` or `https` URL with a
nonempty host and no userinfo, query or fragment; a base path is allowed.
When empty, the binary derives `http(s)://<server.listen>` with the scheme from
the server TLS pair. A wildcard `server.listen` (`0.0.0.0`, `::` or empty host)
has no trusted host, so the `api` role (`all` or any subset containing `api`)
requires an explicit public URL; a `nodehub`-only server does not. Validation
diagnostics name the setting only and never echo a rejected URL, which may
carry credentials or a secret query.

`bbraft_server_gateway_dial` must be `host:port` with a nonempty host and an
ASCII decimal port in 1-65535. When empty, the binary derives it from
`node.gateway-listen`, mapping a wildcard bind to `127.0.0.1`. A plaintext
gateway requires both a loopback `gateway.listen` and a loopback (or
`localhost`) dial target; `[::ffff:127.0.0.1]` counts as loopback, matching
`netip`.

`bbraft_server_gateway_ca_file` and `bbraft_server_gateway_server_name` need
the gateway TLS pair. Without a CA bundle the binary uses the system roots, so
a private CA must already be installed there. The role does not copy the CA
bundle: like the TLS certificate and key paths, this is a target path the
caller provisions. The certificate name must contain no whitespace or NUL.

The intervals (`bbraft_server_stream_max_seconds`, `..._retry_seconds`,
`..._reaper_seconds`, `..._job_poll_seconds`) must be positive decimal
integers. Floats, fractions, exponents, spaces and Unicode digits are rejected
rather than truncated; the validated value is what reaches `ExecStart`.

`ExecStart` values are rendered through `bbraft_systemd_exec_quote`: spaces
stay inside one argument, `%` and `$` become `%%` and `$$`, quotes and
backslashes are escaped, and a value with a line break or NUL is rejected.
`ReadWritePaths` uses `bbraft_systemd_unit_quote`, which escapes `%` the same
way but keeps a literal `$` because systemd does not expand environment
variables outside `ExecStart`. `ExecStart` never contains a secret, only file
paths.

The unit sets `NoNewPrivileges=true`, `ProtectSystem=strict`,
`ProtectHome=true`, `PrivateTmp=true`, `PrivateDevices=true`,
`ProtectKernelTunables/Modules/Logs`, `ProtectControlGroups`, `ProtectClock`,
`RestrictSUIDSGID`, `RestrictNamespaces`, `RestrictRealtime`,
`LockPersonality`, an empty capability bounding set and
`RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK`.
`ReadWritePaths` is the state directory; `/etc/bbraft-server` stays readable but
read-only to the service. A changed unit reloads systemd; a changed binary,
credential file or unit restarts the service. Unchanged runs do neither.

## Object storage (optional)

`bbraft_server_s3_enabled` defaults to `false`. When the inventory sets it to
`true`, the role writes:

- `bbraft_server_s3_env_file` (default `/etc/bbraft/s3.env`, mode `0600`)
  with exactly `BBRAFT_S3_ENDPOINT`, `BBRAFT_S3_REGION`, `BBRAFT_S3_BUCKET`,
  `BBRAFT_S3_PATH_STYLE` and `BBRAFT_S3_ACCESS_KEY_ID`;
- `bbraft_server_s3_secret_file` (default
  `/etc/bbraft/s3-secret-access-key`, mode `0600`) with the secret access key
  alone. Every task that reads or writes the secret sets `no_log: true`.

The unit then carries:

```ini
EnvironmentFile=/etc/bbraft/s3.env
LoadCredential=s3-secret-access-key:/etc/bbraft/s3-secret-access-key
Environment=BBRAFT_S3_SECRET_ACCESS_KEY_FILE=%d/s3-secret-access-key
```

systemd reads the environment file and the source secret as root, then hands
the service a private `0400` copy of the secret under `%d`; the binary's
`-s3.secret-access-key-file` check accepts that mode. The secret value never
appears in `ExecStart`, the environment file or the unit.

systemd does **not** unquote `EnvironmentFile=` or `LoadCredential=`: a quoted
path is logged as `path is not absolute, ignoring` or `Credential source ...
not valid, ignoring`, and the unit starts without the object store. Both lines
therefore render through `bbraft_systemd_abs_path`, which writes the path
verbatim, doubles `%`, and refuses a path that is relative, contains
whitespace, a quote, a backslash, a line break or NUL, has a `.`/`..` element,
or ends with `/`.

| Variable | Meaning |
|---|---|
| `bbraft_server_s3_enabled` | write the files and unit lines (`false` by default) |
| `bbraft_server_s3_endpoint` | `http(s)://host[:port]`; empty means AWS S3 |
| `bbraft_server_s3_region` | the service's configured region (`garage`, `auto` on R2, the AWS region); empty defaults to `us-east-1` only without a custom endpoint |
| `bbraft_server_s3_bucket` | one bucket per installation |
| `bbraft_server_s3_path_style` | `true` for Garage and most self-hosted services |
| `bbraft_server_s3_access_key_id` | credential identifier |
| `bbraft_server_s3_secret_access_key` | the secret access key; supply it from the vault or the gitignored inventory |
| `bbraft_server_s3_dir` | directory for both files, default `/etc/bbraft` |
| `bbraft_server_s3_env_file` / `_secret_file` | target paths; both must stay under `bbraft_server_s3_dir` |

`/etc/bbraft/s3.env` and `/etc/bbraft/s3-secret-access-key` are also the names
a colocated Garage uses (`garage_credentials_file` and `garage_secret_file`),
so the server and node roles can share one set: keep the default paths and
supply the same values in the inventory. The role never creates the bucket:
`-s3.create-bucket` stays at the binary default (`false`), and the bucket
belongs to the object-store deployment.

The lifecycle settings in the table above come from `internal/server/config`
and are rendered as ordinary `ExecStart` flags. The role has no `-gc.*`
variables: chunk garbage collection runs with the server defaults
(`docs/server.md`, "Chunk garbage collection").

## Example (private inventory, never committed)

```yaml
# host_vars/control1.yml, values from a vault or an ignored file
bbraft_server_binary_src: "{{ playbook_dir }}/../../bin/bbraft-server"
bbraft_server_database_url: "postgresql://bbraft:<url-encoded-password>@127.0.0.1:5432/bbraft?sslmode=disable"
bbraft_server_gateway_token: "<token from the vault>"
bbraft_server_master_key_source: "{{ vault_dir }}/bbraft-master-key"   # 32 raw bytes
bbraft_server_api_keys:
  - id: main
    owner: "<account id>"
    key: "bmb_<raw key>"
bbraft_server_node_tokens:
  - id: node-example
    tokenSha256: "<sha256 of the node token>"
```

The role does not run PostgreSQL; use the `postgres` role (or an external
database) and pass its URL. `sslmode=disable` is suitable only for loopback.
The master key file must have no trailing newline: `printf`/`head -c 32`, not
an editor that appends one.

## Tests

Local only, localhost connection, temporary directories; no user, unit or
service is touched:

```sh
cd infra/ansible
mise exec -- sh roles/bbraft_server/tests/run.sh
```

It runs the filter unit tests; `--syntax-check` for the full role and both
plays; a full render with assertions on exact flags, formats, modes, quoting,
hardening and no-secret-in-ExecStart; `systemd-analyze verify` on the rendered
unit before cleanup (API requires it; set `bbraft_server_verify_unit=false`
only in a documented non-systemd test container); lifecycle-only, TLS-pair,
wildcard-API-with-public-URL-gateway-trust and nodehub-only-wildcard renders;
one object-store render asserting the three unit lines, the `0600` file
modes, the five-variable environment file, that the secret never reaches the
unit or the environment file, and that `systemd-analyze verify` reports no
setting it ignored, plus the absence of every S3 line when disabled; a verbose
validation/render pass asserting no credential placeholder
is printed; and rejections for every required input, malformed digest, raw
token,
duplicate effective API key id (empty id defaulting to the owner),
float/non-decimal intervals, unbracketed IPv6, float/Unicode digit ports,
IPv4-mapped public plaintext listeners, invalid listeners even with TLS
enabled, duplicate node ids, one-sided TLS pair, public plaintext node
listener, off-loopback plaintext gateway and dial, duplicate/unknown role,
wildcard API bind without a public URL, relative/non-http/empty-host or
credential/query/fragment public URLs, leading-space, malformed percent
escapes and IPv6 authorities, signed or Unicode-digit dial and URL ports, CA
bundle or
certificate name without the gateway TLS pair, and whitespace (including
line/paragraph separators) or NUL certificate names. It also accepts two node
ids sharing one digest, a wildcard API listener with an explicit public URL, a
nodehub-only wildcard listener without one, a wildcard gateway bind that still
dials loopback, a separate TLS gateway target with its own CA bundle and
certificate name, and `localhost`/IPv4-mapped loopback dial targets.

Not covered locally: installing the binary on a real host, creating the
system user, starting the service and restarting it through handlers. Those
need a disposable Debian 13 host or test VM, run by the lead. Never run this
role in apply or check mode against a real inventory from here.
