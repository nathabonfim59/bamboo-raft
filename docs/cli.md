# bbraft CLI

`bbraft` is the command-line client for bamboo-raft. It speaks the
`/api/v1` API and the small `/api/bbraft` extensions described
in [api.md](api.md). Build it with `make build`; `bin/bbraft` is the
binary used by the compiled tests and the examples below.

## Sign in

```sh
# One installation key from your provider. The key is read from stdin and
# stored in ~/.config/bbraft/config.json with mode 0600.
bin/bbraft login --key-stdin

# Or select the endpoint and key explicitly. BBRAFT_API_URL and
# BBRAFT_API_KEY override the saved configuration; --api-url overrides the URL again.
export BBRAFT_API_URL=http://127.0.0.1:7700
printf '%s\n' "$BBRAFT_API_KEY" | bin/bbraft login --key-stdin
```

### Tailnet names and the static binary

The release `bbraft` is a static build (`CGO_ENABLED=0`), so Go uses its own
resolver: it reads `/etc/hosts`, then asks the nameservers in
`/etc/resolv.conf`, and never goes through the system's NSS or
systemd-resolved. A server reached by its tailnet name
(`<host>.<tailnet>.ts.net`, [infra/README.md](../infra/README.md#api-access-https-or-http))
then fails to resolve on any machine whose `/etc/resolv.conf` points at a
resolver that does not know tailnet names, even though `getent hosts`, `curl`
and browsers resolve it fine. Cloudflare WARP is the common case: it owns
`/etc/resolv.conf` (`nameserver 127.0.2.2` / `127.0.2.3`). The symptom is:

```json
{"event":"error","error":"GET /api/v1/me: request failed: GET: request failed"}
```

while `curl http://<host>.<tailnet>.ts.net/api/v1/me` from the same machine
answers `401` (reached, no key). Fixes, best first:

1. **Make the resolver forward tailnet names.** For WARP: add `ts.net` to its
   *Local Domain Fallback* (Zero Trust dashboard, device profile), so names
   under it go to Tailscale's resolver (`100.100.100.100`). Fixes every
   static Go tool on the machine.
2. **A hosts entry**, which Go's resolver reads first:
   `echo "<tailnet-ip> <host>.<tailnet>.ts.net" | sudo tee -a /etc/hosts`.
   The tailnet IP (`tailscale ip -4` on the server) stays with the machine,
   but remove the line once fix 1 is in.
3. **A cgo build** of the CLI, which resolves through the system like `curl`:
   `CGO_ENABLED=1 go build -trimpath -o bbraft ./cmd/bbraft` at the release
   tag.

`--api-url` with the tailnet IP instead of the name does not work:
`tailscale serve` routes by host name and answers `404` for the bare IP.

`--api-url` selects the server for one invocation. `BBRAFT_API_URL` /
`BBRAFT_API_KEY` override the saved configuration. The saved configuration is used by
later invocations; do not export an API key in scripts that already logged in.

## Quickstart

```sh
id="$(bin/bbraft new --json --type small --ttl 600 --name demo \
  | jq -r 'select(.event == "ready") | .id')"

bin/bbraft exec "$id" --timeout 0 --stream --json -- bash -lc 'printf "hello\n"; exit 0'

bin/bbraft stop "$id" --json
bin/bbraft resume "$id" --json
bin/bbraft info "$id" --json

bin/bbraft delete "$id" --yes --wait --json
```

`new --json` emits one `created` event and then one `ready` event. `stop` is
asynchronous: it returns the accepted stop operation, and `info` or
`stop --status` reports progress. `resume` waits for the same retained disk to
boot on the same node and then runs a small readiness command.

## Output and exit status

| Output | Behavior |
|---|---|
| human | Readable text on stdout; progress and diagnostics on stderr. |
| `--json` | One JSON value or JSON Lines event per command, as documented by the pinned API. |
| pipe | Non-terminal stdout selects JSON automatically for most commands; `--json` makes it explicit. |
| `exec` | Prints JSON frames when `--json` is set or stdout is not a terminal; raw passthrough is the terminal mode. |
| `run` | Prints the command's raw output unless `--json` is set, even when its stdout is a pipe. |
| `ssh`, `scp` | Output belongs to the local OpenSSH client and is always raw; an explicit `--json` is a usage error. |

Exit status is `0` for success, the remote command's status for `exec`/`run`,
`124` for a command timeout, `1` for an API or local failure, and `2` for
invalid usage. Cancellation is transport-specific: a foreground `exec`
(synchronous or transparent detached) stops the known remote process and exits
`1`; an explicit `exec --attach`, a cancelled `run`, and a cancelled `ssh` or
`scp` exit `130`. API failures carry a safe JSON error event in JSON mode.

## Commands

| Command | Purpose |
|---|---|
| `login --key-stdin` | Verify and save an API key. |
| `new [--type small\|default\|large\|xlarge] [--ttl N] [--name NAME] [--from NAME\|CODE] [--fail-fast]` | Create a sandbox, or restore a saved snapshot or share code (`bsh_...`). |
| `new --from NAME\|CODE --count N [...]` | Create `N` sandboxes (`1` to `1000`) from a saved snapshot or share code in one batch and print each id without waiting; cannot be combined with `--fail-fast`. |
| `list [--all] [--json]` | List owner-scoped sandboxes. |
| `info ID` | Show one sandbox, including `ttlSeconds`, `archiveAfter`, `stoppedAt`, stop metadata and `sshEndpoint` when known. |
| `rename ID NAME` | PATCH the display name. |
| `extend ID (--hours H \| --ttl N \| --no-auto-stop)` | Set the remaining auto-stop time from now. |
| `stop ID [--force]` | Stop a sandbox, keep its disk and return the stop operation. |
| `stop ID --status STOP_ID` | Read one stop operation. |
| `resume ID [--no-wait] [--fail-fast] [--type T] [--ttl N \| --no-auto-stop] [--env K=V ...]` | Boot the retained disk on its node. |
| `fork ID [--type T] [-e K=V ...] [--ttl N \| --no-auto-stop] [--fail-fast] [--label K=V ...] [--wait]` | Fork a sandbox into a new sandbox (a ready source is snapshotted first, a stopped one uses its last snapshot); `--wait` polls until ready. |
| `recover ID` | Power-cycle an unresponsive running sandbox without modifying its disk. |
| `exec ID [COMMAND...]` | Run a command; see transports below. |
| `exec ID --detach COMMAND` | Start a detached command and print its process id. |
| `exec ID --status PID` | Read a detached process state and log tails. |
| `exec ID --attach PID` | Follow a detached process from the start. |
| `exec ID --kill PID` | Stop a detached process and every process it started. |
| `run [--push DIR] [--cwd DIR] [--keep] -- COMMAND...` | Create a temporary sandbox, optionally push a directory, run the command and delete the sandbox. `--keep` leaves it with a renewed 900-second auto-stop timer. |
| `push ID DIR [--to PATH]` | Upload a directory as a tar archive to the sandbox. |
| `snapshot ID NAME` | Save a snapshot of the sandbox and poll until it is ready. |
| `snapshot rm NAME` | Delete a saved snapshot name. |
| `snapshot share NAME` | Create a share code (`bsh_...`) for a saved snapshot; the full code is printed once. |
| `snapshot shares NAME` | List share codes for a saved snapshot with their prefix, status, uses and timestamps. |
| `snapshot unshare NAME SHARE_ID` | Revoke one share code of a saved snapshot. |
| `snapshot latest SANDBOX_ID` | Show the newest captured snapshot of a sandbox, or say it has none. |
| `snapshots [SANDBOX_ID] [--all] [--limit N]` | List captured snapshot generations for the account or one sandbox; `--all` follows pagination to the last page. |
| `ssh [-t] ID [COMMAND...]` | Open an SSH session or run one remote command. |
| `scp [-r] SRC... DST` | Copy files to or from a sandbox. |
| `delete ID --yes --wait` | Delete a sandbox and wait for the durable operation. |

## exec transports

A positive timeout at or below 600 seconds is a synchronous command, unless
`--stream` is set. A positive timeout above 600 seconds streams. `run` always
requests streamed output and may select the detached transport on a capable
server.

The CLI reads `/api/bbraft/info` for three dispatch decisions: `--timeout 0`,
a positive timeout above 600 seconds (to decide whether detached is
available), and `run` before create (to choose rolling or legacy TTL). The
shipped `streamMaxSeconds` default is 7200. On a legacy server, `--timeout 0`
sends that returned value exactly, never zero. On a server that advertises
`commands.detached: true`, `--timeout 0` is unlimited: the detached start
omits a timeout entirely and the command is followed through attach.

When the info response contains `commands.detached: true`, a long exec
(`--timeout 0` or above 600) is started once with `detached: true` and
followed through the attach stream. The CLI keeps the legacy
`started`/`stdout`/`stderr`/`exit` JSON shapes, so existing scripts do not
change. A server that omits the member, sends `false`, or has no info endpoint
keeps the legacy streamed path. A missing endpoint falls back to 600 seconds
with one stderr note; a valid response without `detached` uses its
`streamMaxSeconds`; a malformed metadata response is an error, never a silent
fallback. There is no re-POST after an ambiguous start.

`Ctrl-C` on a transparent long command stops the known remote process through
`stopCommand` before exiting with status 1. Cancelling an explicit
`--attach` exits 130 and leaves the detached process running. Use
`exec ID --kill PID` to stop it.

## run lifetime

On a capable server, `run` creates its sandbox with `ttlSeconds: 900`, PATCHes
that exact timer immediately, and renews it every five minutes while setup,
push and the command are in progress. It pushes a directory only when `--push`
is supplied. A completed default run deletes the sandbox; `--keep` sends a
final 900-second PATCH and leaves it. Cancellation, setup failure or an
unconfirmed command result with a known sandbox still deletes it; an ambiguous
create has no id to delete and leaves only the finite TTL. If a final keep
renewal cannot be confirmed, the CLI reports it and leaves the sandbox at its
last finite expiry. On an old server without the detached capability, `run`
keeps the legacy fixed TTL derived from the command timeout and `--keep` exits
1 with `keep_unsupported` before creating a sandbox.

## SSH and SCP

`ssh` and `scp` manage `~/.ssh/bbraft_ed25519`, create it with safe
permissions when missing, and never overwrite an existing key. They install
its public key through the `sshKey` API, pin the returned host key for this
invocation only (not in a shared `known_hosts`), and execute the local OpenSSH
client with an argument vector rather than a shell. `ssh -t` allocates a
terminal; `scp -r` copies directories. One `ssh` command argument is a remote
shell command line; several arguments are quoted as argv before the remote
shell sees them.

These commands require the local OpenSSH client. The CLI tests use a fake API
and a local pseudo-terminal; they prove argument construction, key handling,
host-key pinning and terminal restoration, but they do not prove SSH
reachability. Real network reachability and SSH host-key generation belong to
the node and guest.

## Known limits

- No dashboard panel (Phase 1b), command history, process list,
  file search or find/replace. Detached commands are exposed through status,
  attach and stop only.
- File reads and writes are limited to 5 MiB inline; use the signed download
  URL for larger files. If the sandbox user removes read permission from
  their own `/home/user` or `/tmp` (for example mode `0111`), file operations
  answer `400 invalid_path`; commands still work.
- `createFolder` with mode `0000` uses a StatFile preflight. A concurrent
  creator between that stat and MakeDir can leave the directory at mode `0000`;
  this race is accepted.
- Stop and resume keep the disk on the same node. There is no cross-node or
  empty-disk fallback. A resume whose retained disk is missing ends in a
  terminal `error` sandbox instead of booting a replacement.
- A saved-snapshot restore pin is held until the restore is ready, a confirmed
  outcome proves the retained disk is settled (a confirmed archived stop or a
  terminal missing-disk stop), or cleanup is confirmed. A cancelled or failed
  HTTP request does not release it early; a lease expiry eventually recovers
  it.
- Signed download and upload tokens are bearer-equivalent secrets. The API role
  omits them from its logs; any access log in front of the API MUST redact the
  receiver token path segment.
