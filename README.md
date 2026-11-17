# bamboo-raft

Self-hosted persistent Linux VMs for AI agents. bamboo-raft runs each sandbox
as a jailed QEMU/KVM virtual machine and gives agents an HTTP API and a CLI to
create sandboxes, run commands, move files, SSH in, stop, resume and snapshot
them.

**Status: 0.1.0, single-node.** One control plane and one KVM node. See
[What works](#what-works) and [Not yet](#not-yet).

## Components

| binary | role |
|---|---|
| `bbraft-server` | control plane: `/api/v1` and `/api/bbraft`, PostgreSQL, node hub |
| `bbraft-node` | one per KVM host: runs QEMU VMs and dials the server |
| `bbraft-guest` | inside every VM: exec, files, freeze/thaw, over gRPC on vsock |
| `bbraft` | the CLI |

Nodes dial the server; the server never opens connections to nodes. Guests
never hold platform credentials.

## Build

```sh
mise install   # Go and protoc, pinned in mise.toml
make build     # every cmd/* into bin/
make check     # gofmt, go mod tidy, go vet, staticcheck, go test -race
```

The guest image is built with mkosi (`make -C images image`, see
[docs/guest-image.md](docs/guest-image.md)).

## Documentation

The documentation site is **[nathabonfim59.github.io/bamboo-raft](https://nathabonfim59.github.io/bamboo-raft/)**,
with the [HTTP API reference](https://nathabonfim59.github.io/bamboo-raft/api-reference/http/). It is
built from the Markdown below, read in place:

- [docs/server.md](docs/server.md): running `bbraft-server`
- [docs/node.md](docs/node.md): running `bbraft-node` on a KVM host
- [docs/guest-image.md](docs/guest-image.md): the guest image
- [docs/cli.md](docs/cli.md): the `bbraft` CLI
- [docs/api.md](docs/api.md): API behavior; the schemas are
  [api/bbraft-v1.yaml](api/bbraft-v1.yaml) and
  [api/bbraft-ext.yaml](api/bbraft-ext.yaml)
- [docs/windows-hyperv-node.md](docs/windows-hyperv-node.md): a development
  node in Hyper-V
- [infra/](infra/README.md): Ansible roles for hosts
- [docs/e2e-smoke.md](docs/e2e-smoke.md): end-to-end tests

## What works

- **Sandboxes on one KVM node**: create from the base image or a saved
  snapshot, list (filters, cursor), info, rename, extend, delete, auto-stop.
- **Stop and resume** on the same node, with the disk kept and grown on
  resume. Stopped sandboxes free their CPU and memory.
- **Commands**: synchronous, streamed and detached (status, attach with
  heartbeats and reconnect, kill).
- **Files**: read/write (5 MiB inline), list, info, mkdir, move, chmod,
  delete, signed upload and download URLs, `bbraft push`.
- **SSH**: `bbraft ssh` and `bbraft scp`, with a managed key and a pinned host
  key.
- **Saved snapshots**: save, list, get, delete, create from a snapshot; local
  to the node that holds them.
- **`bbraft run --push . -- <cmd>`**: a fresh VM per run.
- **Isolation**: every VM runs jailed (own uid, no capabilities,
  no_new_privs, seccomp, private mount/PID/IPC namespaces, read-only chroot,
  cgroup v2 limits) on a TAP with NAT egress. No VM reaches the host, the
  host's management networks, metadata addresses or another VM.

## Not yet

- A web panel.
- Several nodes, object-store snapshots, fork and recover, resume on another
  node.
- Edge proxy and hosted ports, port forwarding, desktop, webhooks and events,
  quotas.
- Environments and secrets, command history.
- Memory snapshots and warm pools.
- Teams, organizations, API key management and billing.

## Contributing

Read [AGENTS.md](AGENTS.md) for the conventions, layout and the contracts the
code keeps.

## License

MIT, see [LICENSE](LICENSE).
