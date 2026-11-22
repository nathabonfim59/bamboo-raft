# Deploying several nodes

A bamboo-raft install is **one control plane and any number of KVM nodes**.
This page shows how the pieces fit together and how to add a node on another
host. The Ansible roles and their variables are in
[infra/README.md](../infra/README.md); every flag is in
[server.md](server.md) and [node.md](node.md).

## What runs where

| Piece | Where | Notes |
|---|---|---|
| `bbraft-server` | the control host (inventory group `control`) | the API, the node hub and the lifecycle workers |
| PostgreSQL | the control host only, on loopback | the only database. Nodes never touch it |
| Object store (S3) | anywhere every node and the server can reach | Garage on the control host (`garage` role) or an external service (R2, AWS S3, MinIO, ...) |
| `bbraft-node` | every KVM host (group `nodes`); the control host may be one too | runs the VMs, keeps their disks, uploads and downloads snapshot chunks |

There is one control plane. The server does not run in a primary/secondary
pair (no control-plane HA yet), and nodes have no roles among themselves:
every node is equal and talks only to the server.

## How nodes and the server talk

- **Nodes dial the server**, on `-node.listen` (default `127.0.0.1:7701`).
  The server never opens a connection to a node, so a node needs no inbound
  port for the control link.
- **Every node has its own token.** The node reads its raw token from a
  0600 file (`-node.token-file`). The server holds only its SHA-256 digest,
  keyed by node id, in the node tokens file (`-node.tokens-file`, role
  variable `bbraft_server_node_tokens`). A node whose id or token is not in
  that file is refused.
- **That tokens file is the node catalog.** There is no other list of nodes
  to edit. Once a node connects, the server polls its capacity (vCPUs,
  memory, disk, images) and stores it in Postgres. That is how placement
  knows which node has room. A changed node setting (a bigger capacity, say)
  reaches the server at the next poll; nodes never share state with each
  other.
- **Data between nodes goes through the object store.** A snapshot taken on
  one node is uploaded as deduplicated 4 MiB chunks; a resume, fork or
  node-loss move on another node downloads them. Nodes never copy disks to
  each other directly.

### Encryption on the node link

The node link carries the token and every RPC **in plain TCP**. Both binaries
therefore refuse a link address that is not `localhost`, a loopback IP or a
private IP (RFC 1918 or IPv6 ULA). TLS on the node link is planned for the
next release. Until then a node on another host needs a private, encrypted
path to the server. Pick one of the three below.

## Connecting a node on another host

The examples use `<server>` for the control host and `<node2>` for the new
node.

### Option 1: an SSH tunnel (works anywhere, no extra software)

The node keeps dialing `127.0.0.1:7701`, its own loopback, and an SSH local
forward carries the connection to the server's loopback listener. The
server's `-node.listen` stays at its default `127.0.0.1:7701`.

1. **On `<server>`:** create a user only for the tunnel, with a key that can
   do nothing but forward to the node port. In its `~/.ssh/authorized_keys`:

   ```text
   restrict,port-forwarding,permitopen="127.0.0.1:7701" ssh-ed25519 AAAA... bbraft-tunnel@node2
   ```

   With the `hardening` role, add that user to `hardening_ssh_allow_users`.

2. **On `<node2>`:** a systemd unit that keeps the tunnel up, as a dedicated
   user that owns the key (mode 0600). Pin the server's host key in that
   user's `known_hosts` first.

   ```ini
   # /etc/systemd/system/bbraft-tunnel.service
   [Unit]
   Description=SSH tunnel from bbraft-node to bbraft-server
   After=network-online.target
   Wants=network-online.target
   Before=bbraft-node.service

   [Service]
   User=bbraft-tunnel
   ExecStart=/usr/bin/ssh -N -o ExitOnForwardFailure=yes \
       -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
       -o StrictHostKeyChecking=yes \
       -L 127.0.0.1:7701:127.0.0.1:7701 bbraft-tunnel@<server>
   Restart=always
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```

3. **The node** keeps `bbraft_node_server_addr: 127.0.0.1:7701`, the default.

If the tunnel drops, the node reconnects by itself once it is back. A drop
shorter than `-lifecycle.node-lost-seconds` (120 s) changes nothing. A
longer one counts as a node loss (see [Node loss](#node-loss-and-partitions)).

**The object store over the same tunnel.** With Garage on the control host
listening on loopback (`garage_s3_bind: 127.0.0.1:3900`), add a second
forward, `-L 127.0.0.1:3900:127.0.0.1:3900`, and a second
`permitopen="127.0.0.1:3900"`. Then give the node the remote S3 settings with
`endpoint: http://127.0.0.1:3900`. An external S3 service needs no tunnel.

### Option 2: Tailscale (the roles' default access path)

The `tailscale` and `hardening` roles already put every host on a tailnet and
admit traffic only from it. A tailnet's IPv4 addresses (100.64.0.0/10) are
**not** private addresses in the sense of the link rule, so they are refused.
Its IPv6 addresses (`tailscale ip -6`, in `fd7a:115c:a1e0::/48`) are ULA and
are accepted. Traffic between tailnet hosts is encrypted by WireGuard.

```yaml
# control host
bbraft_server_node_listen: "[<server tailnet IPv6>]:7701"
# every node, the control host's node included
bbraft_node_server_addr: "[<server tailnet IPv6>]:7701"
```

Allow `tcp:7701` from `tag:bbraft` to `tag:bbraft` in the tailnet policy. For
Garage on the control host, set `garage_s3_bind` to the same IPv6 address
(port 3900), allow `tcp:3900`, and use that endpoint in the nodes' S3
settings.

### Option 3: a private network you already have

A provider's private network (a VLAN or vRack with RFC 1918 addresses) or
your own WireGuard mesh works the same way: bind `-node.listen` to the
server's private address and point `-server.addr` at it. A plain provider
VLAN is not encrypted; use it only if you trust that network.

## Adding a node, step by step

1. **Inventory** (private, gitignored): add the host under `nodes` in
   `inventory/hosts.yml` and give it `host_vars` (see
   [infra/README.md](../infra/README.md#adding-a-node)).
2. **Token:** generate a random token, store it in
   `inventory/secrets/`, give it to the node (`bbraft_node_token_src`), and
   add `{id: <node id>, tokenSha256: <sha256 of the token>}` to
   `bbraft_server_node_tokens`. Set `bbraft_node_id` to that id (it defaults
   to the hostname).
3. **Link:** pick an option above and set `bbraft_node_server_addr`.
4. **Object store:** give the node the same bucket as the server. The
   colocated Garage mode (`bbraft_node_s3_env_file`) works only on the
   Garage host itself. Every other node uses the remote mode:

   ```yaml
   bbraft_node_s3:
     endpoint: http://127.0.0.1:3900   # through the tunnel; or your S3 URL
     region: garage
     bucket: bbraft
     path_style: true
     access_key_id: <key id>
   bbraft_node_s3_secret_access_key: "{{ lookup('file', 'secrets/s3-secret') }}"
   ```

5. **Guest images:** the node needs the same image versions as the others.
   The role installs them from your build.
6. **Apply:** `ansible-playbook site.yml --limit <node2> --tags bbraft`
   (after `--check --diff`). Then redeploy the server
   (`--limit <server> --tags bbraft_server`) so it loads the new token
   digest.
7. **Check:** the server's log shows the node connecting, and placement
   starts using it at once.

## SSH into sandboxes on several nodes

`bbraft ssh` connects straight to the node that runs the sandbox, on that
node's `-node.ssh-ingress-addr` and a port from its `-node.ssh-port-range`
(`bbraft_node_ssh_port_range`, for example `22000-22999`). Each node therefore needs its own reachable ingress address, its
tailnet address with the roles, and the port range allowed in the tailnet
policy.

## Capacity and placement

- Each node reports what it offers: detected CPUs, memory and free disk,
  minus a reserve, or the explicit `-node.capacity-*` values. See
  [node.md, Capacity](node.md#capacity).
- A create or resume goes to one node that has room for the whole sandbox.
  **A VM never spans nodes**: if no single node has the memory, the request
  fails with `503 out_of_capacity`, even when the nodes together have enough.
- A resume prefers the node that still holds the sandbox's disk. Otherwise
  it restores the last snapshot from the object store on another node. A
  fork prefers the source's node.

## Node loss and partitions

A node silent for longer than `-lifecycle.node-lost-seconds` (120 s) is
treated as lost: its running sandboxes restart on other nodes from their last
completed snapshot, so up to one snapshot interval (60 s by default) of work
can be lost. A node that was only cut off, not dead, keeps running its VMs
until it reconnects. The server then deletes those copies, and anything
written to them after the move is lost. This release has no node-side
self-fence. Keep the link reliable: run the tunnel under systemd as above,
and prefer a stable network to a laptop-grade one.

## Backups

- **Postgres** on the control host holds every account, key, sandbox and
  snapshot record. Back it up.
- **The master key** (`inventory/secrets/master-key`) encrypts secrets in
  Postgres. Without it a restored database is unreadable.
- **The object store** holds the snapshot data. Garage with one node has no
  replication; use an external service or back up its data directory.
- Node disks are working copies. A sandbox with snapshots on can be restored
  from the object store.

## What has been tested

The two-node end-to-end test (`deploy/dev/e2e-p2.sh`) runs the server, Garage
and two nodes on one KVM host, linked over loopback. It covers resume on
another node, fork, saved snapshots, `count`, node loss and eviction. The
link options above use only the address rules and the standard tools
described here. The tunnel and Tailscale setups are not part of that test.
