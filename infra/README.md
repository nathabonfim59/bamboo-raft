# infra

Everything done to bamboo-raft hosts is done from here, so a node can be
rebuilt or a new one added by running the same code. Nothing is changed on a
host by hand.

## Ansible

`ansible/` holds the host baseline for nodes (`site.yml`):

| Role | Does |
|---|---|
| `kvm` | persists `nested=1` for `kvm_intel`/`kvm_amd` and checks it is on |
| `swap` | gives every swap entry in `/etc/fstab` the same `pri=` so swap is spread over both disks |
| `bulk_disk` | partitions and formats the bulk HDD as XFS (`bbraft-bulk`) and mounts it at `/srv/bulk`; refuses any disk that holds something else |
| `garage` | **optional** (tag `never`): Garage as the local S3 object store, see [Object storage](#object-storage) |
| `kernel_reboot` | reboots when a newer kernel is installed than the one running (`kernel_reboot_allowed`) |
| `tailscale` | installs a pinned Tailscale from its signed apt repo, joins the tailnet once with a single-use auth key, and publishes the API on the tailnet (`tailscale serve`, HTTPS or HTTP: see "API access") |
| `hardening` | nftables inbound filter (tailnet, loopback, WireGuard UDP, DHCP, ICMP only), OpenSSH limited to listed users with no root login, LLMNR/mDNS off |
| `postgres` | PostgreSQL 17 on loopback for `bbraft-server` |
| `bbraft_server` | the control plane binary, credential files and a hardened unit, as `bbraft-server` |
| `bbraft_node` | QEMU, the node binary, verified guest images and a hardened unit, as `bbraft-node` |

### Setup (on your machine)

Tools come from [mise](https://mise.jdx.dev), pinned in
`ansible/mise.toml` (uv and ansible-core), so the setup is the same on any
distro and needs no sudo:

```sh
cd infra/ansible
mise trust && mise install                               # uv + ansible-core
ansible-galaxy collection install -r requirements.yml    # into ./.collections
```

Inside `infra/ansible`, mise puts `ansible` and `ansible-playbook` on the
`PATH` (with mise activated in your shell; otherwise prefix commands with
`mise exec --`).

### Inventory (private)

Real hosts live in `ansible/inventory/`, which is **gitignored**: addresses,
SSH key paths and disk serials never enter the repository. Start from the
template:

```sh
cd infra/ansible
cp -r inventory.example inventory   # then edit inventory/hosts.yml and host_vars/
```

Each node is reached as a sudo user with its own SSH key, both set in
`inventory/hosts.yml`.

### Run

```sh
cd infra/ansible
ansible nodes -m ping                      # reach every node
ansible-playbook site.yml --check --diff   # preview
ansible-playbook site.yml                  # apply
ansible-playbook site.yml -e kernel_reboot_allowed=false   # no reboot
ansible-playbook site.yml --limit node1 --tags swap   # one node, one role
ansible-playbook site.yml --tags all,garage          # baseline + Garage
```

Every role can run again with no change. A second run reporting `changed`
is a bug.

### Adding a node

1. Add it under `nodes` in `inventory/hosts.yml` (the private copy).
2. Create `inventory/host_vars/<name>.yml` with its `kvm_module` and, if it
   has one, the bulk disk's `/dev/disk/by-id/…` path (`ls -l /dev/disk/by-id/`).
3. Preview with `--check --diff --limit <name>`, then apply.

## Deploying bamboo-raft (tailnet only)

A bamboo-raft host is reachable **only over Tailscale**. Nothing listens
on its public interface: the API, node link, gateway, Postgres and Garage
stay on loopback, the API reaches clients through `tailscale serve`
(`https://<host>.<tailnet>.ts.net`, or `http://` with
`tailscale_serve_scheme: http`: see [API access](#api-access-https-or-http)),
and SSH is admitted only from the tailnet.

### Before the first run

- **Tailnet policy** (admin console): a `tag:bbraft` tag owner, and a grant
  from your devices to `tag:bbraft` on `tcp:22` and `tcp:443` (`tcp:80`
  with `tailscale_serve_scheme: http`), plus `tcp:22000-22999` for SSH into
  sandboxes. Turn on MagicDNS. For HTTPS also turn on HTTPS certificates
  (DNS settings); the role refuses to publish over HTTPS without them,
  because `tailscale serve --https` would otherwise wait forever.
- **Auth key**: tagged `tag:bbraft`, pre-approved, single-use, not
  ephemeral. Store it in a mode-600 file on your machine and set
  `tailscale_authkey_file` to its path. It is used once, on the host only
  through a root-only temporary file that is deleted right after.
- **Secrets** live as mode-600 files in the gitignored
  `inventory/secrets/` and are read by lookups in `host_vars`: the Postgres
  password, gateway token, node token, API key (`bmb_…`) and the 32-byte
  master key. The server stores only the node token's SHA-256. **Back up
  `master-key`**: it wraps the install data key, and without it encrypted
  data in Postgres cannot be read.
- **Artifacts** on the controller: `make build` binaries, and a guest image
  directory from `make -C images image` (needs root; built on a KVM VM).
- **Inventory**: hosts in `nodes`; the host that runs Postgres and
  `bbraft-server` also in `control`.

### Order

Run each step with `--check --diff` first, then apply. Use tags: a plain
`ansible-playbook site.yml` also runs the baseline, which may reboot.

```sh
ansible-playbook site.yml --tags tailscale   # 1. join the tailnet (public SSH still open)
# 2. set ansible_host to the tailnet name it printed; `ansible nodes -m ping`
ansible-playbook site.yml --tags hardening   # 3. close the public interface
ansible-playbook site.yml --tags bbraft      # 4. Postgres, bbraft-server, bbraft-node
```

**Tailscale as the primary resolver** (tag `tailscale_dns`, on by default
in a full `tailscale` run): a resolved drop-in sends every name to
Tailscale's resolver (`100.100.100.100`, `Domains=~.`), not only the
tailnet domain. Tailscale forwards public names only to the tailnet's
**global nameservers**, so add them first (admin console, DNS,
Nameservers). The role asks Tailscale's resolver for
`tailscale_dns_check_names` before changing anything and refuses if it
cannot answer; afterwards it checks those names and the host's tailnet
name again. `-e tailscale_dns_primary=false` removes the drop-in.

```sh
ansible-playbook site.yml --tags tailscale_dns
```

Step 3 refuses to run unless Ansible is already connected through a tailnet
address. It arms a `systemd-run` timer that deletes the filter again after
`hardening_firewall_rollback_seconds` (300), loads the filter, reconnects,
and only then disarms the timer. If the reconnect fails, wait for the
timer; failing that, the provider's rescue system. Never
`systemctl stop nftables`: Debian's unit flushes the whole ruleset,
tailscaled's tables included.

Database migrations need no step: every `bbraft-server` start applies the
embedded migrations under a Postgres lock.

### API access: HTTPS or HTTP

`bbraft-server` always listens on loopback; `tailscale serve` is the only way
in, and only devices on the tailnet that the ACLs allow can reach it. Nothing
is published to the internet in either mode (that would be `tailscale
funnel`, which the role never runs). `tailscale_serve_scheme` picks how the
API appears on the tailnet:

| | `https` (default) | `http` |
|---|---|---|
| URL | `https://<host>.<tailnet>.ts.net` | `http://<host>.<tailnet>.ts.net` |
| Listener | 443, tailnet certificate (Let's Encrypt) | 80, no certificate |
| Encryption in transit | TLS inside WireGuard | WireGuard (end to end between tailnet devices) |
| Tailnet setting | MagicDNS + HTTPS certificates | MagicDNS |
| Public trace | the name is in the **Certificate Transparency logs** | none |
| ACL grant | `tcp:443` | `tcp:80` |

**The Certificate Transparency trade-off.** Every certificate Let's Encrypt
issues is published, permanently, in public CT logs that anyone can search
(crt.sh, for example). A tailnet certificate is issued for
`<machine>.<tailnet>.ts.net`, so with `https` the world learns that this
machine name exists in this tailnet. Nothing becomes reachable (the name
resolves only on the tailnet), but the names themselves leak: machine names
often carry a person's or company's name, a username or a role (`bbraft`,
`ci-runner`), and a tailnet's custom DNS name may carry the organisation's
name. Every machine in the tailnet that requests a certificate shows up,
not only this one, and an entry cannot be removed later. Pick neutral
machine names before turning HTTPS certificates on, or use `http`.

**When to use `http`.** The API traffic is still encrypted between tailnet
devices, so `http` loses no confidentiality on the tailnet itself. It gives
up the certificate: clients must accept an `http://` base URL (the `bbraft`
CLI does), browsers mark the page "not secure" and refuse `Secure` cookies,
and a future web panel would need HTTPS. It is the right choice for a
tailnet that should stay out of the CT logs, or where HTTPS certificates
cannot be turned on. Set it with the matching public URL:

```yaml
tailscale_serve_scheme: http
bbraft_server_public_url: http://<host>.<tailnet>.ts.net
```

Switching the scheme on a running host is one `--tags tailscale` run: the
role publishes the new listener and turns the other one off.

**Clients behind Cloudflare WARP** (or any resolver that does not know
tailnet names) reach the API with `curl` but not with the static `bbraft`
CLI, which reads only `/etc/resolv.conf`. See
[docs/cli.md, "Tailnet names and the static binary"](../docs/cli.md#tailnet-names-and-the-static-binary).

### Security limits of this first version

Configuring `hardening_vm_bridge_interfaces` permits IPv4 forwarding only; the input chain still drops bridge access to host services.

- **A VM can open connections to the host.** QEMU user-mode networking
  maps the guest's `10.0.2.2` to the host's loopback, and QEMU runs as the
  node's user, so guest code can reach Postgres, the API, the node link, the
  gateway and Garage on loopback, and the host's other addresses. Each of
  them requires a credential the guest never holds. Accepted for dogfooding
  (only our own jobs run); fixed by the per-VM jail
  before anyone else's code runs on the host.
- **All VMs share one Unix user**, so a QEMU escape reaches every VM's disk.
  Same fix.
- `debian` keeps passwordless sudo; it is reachable only from the tailnet.

## Object storage

bbraft stores snapshots and images in **any S3-compatible object store**
. Pick one of the two options below. Either way, bbraft reads
its settings from these variables (on a node installed by the `garage` role
they are in `/etc/bbraft/s3.env`, mode 600, root only):

| Variable | Meaning |
|---|---|
| `BBRAFT_S3_ENDPOINT` | `http(s)://host:port`; empty means AWS S3 |
| `BBRAFT_S3_REGION` | the service's region name |
| `BBRAFT_S3_BUCKET` | one bucket per install |
| `BBRAFT_S3_PATH_STYLE` | `true` for Garage and most self-hosted services |
| `BBRAFT_S3_ACCESS_KEY_ID` | access key id |
| `BBRAFT_S3_SECRET_ACCESS_KEY` | secret key |

Credentials never go into the repository or the inventory examples, and the
guest VMs never see them.

### Option 1: Garage on the node (self-hosted, the default)

[Garage](https://garagehq.deuxfleurs.fr) runs on the node itself, with
blocks on the bulk disk and metadata on NVMe. It is enough on its own. The
role is tagged `never`, so it runs only when asked for:

```sh
ansible-playbook site.yml --tags all,garage   # baseline + Garage
ansible-playbook site.yml --tags garage       # Garage only
```

What it sets up:

- the official static Garage binary (version and SHA-256 pinned in
  `roles/garage/defaults/main.yml`), run as the `garage` user under a
  hardened systemd unit
- a one-node cluster (replication factor 1, fsync on, no compression
  because bbraft compresses its own chunks)
- RPC, S3 and admin secrets generated **on the host** under
  `/etc/garage/secrets/`
- the bucket `bbraft` and an access key `bbraft` that may read, write and own it
- `/etc/bbraft/s3.env` with the values above

Every port (S3 `3900`, RPC `3901`, admin `3903`) listens on loopback only.
Settings that may need changing per node, set in that node's
`inventory/host_vars/<name>.yml`:

| Variable | Default |
|---|---|
| `garage_data_dir` | `/srv/bulk/garage/data` (needs the `bulk_disk` role, or point it elsewhere) |
| `garage_meta_dir` | `/var/lib/garage/meta` |
| `garage_capacity` | `3T` |
| `garage_s3_bind` | `127.0.0.1:3900` |

To check it works, on the node: `sudo garage -c /etc/garage/garage.toml status`.

### Option 2: an external S3 service (R2, AWS S3, others)

Skip the `garage` tag, create a bucket and a key that may read and write it
with your provider, and give bbraft the values:

| Provider | `BBRAFT_S3_ENDPOINT` | `BBRAFT_S3_REGION` | `BBRAFT_S3_PATH_STYLE` |
|---|---|---|---|
| Cloudflare R2 | `https://<account-id>.r2.cloudflarestorage.com` | `auto` | `true` or `false` |
| AWS S3 | empty | the bucket's region, e.g. `us-east-1` | `false` |
| MinIO, Ceph RGW, others | the service URL | its region | usually `true` |

For R2, create the bucket with the location hint nearest your nodes. bbraft
uses only core S3 operations (PUT, GET with Range, HEAD, DELETE,
ListObjectsV2, multipart, presigned URLs), so any service that implements
those works. No lifecycle rules, versioning or object lock are needed.
