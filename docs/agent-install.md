# Install with an agent

A coding agent (Claude Code, Codex, opencode and others) can install
bamboo-raft for you. Paste this into it:

```text
Install bamboo-raft for me: self-hosted VMs for AI agents.
Follow https://nathabonfim59.github.io/bamboo-raft/agent-install.md step by step.
Ask me before you run anything on a remote host.
```

The rest of this page is written for the agent. It is the same path a person
follows by hand, in order, with links to the full pages. Read it yourself to
know what the agent will ask you for.

## What you need

- **A KVM host** for the sandboxes: Linux x86-64, Debian 13, with `/dev/kvm`,
  `/dev/vhost-vsock`, `/dev/vhost-net` and `/dev/net/tun`, systemd 254 or
  newer and cgroup v2. One host runs the control plane, PostgreSQL and the
  node in 0.1.0. See [node requirements](node.md#requirements).
- **A machine to deploy from** (your laptop): git, [mise](https://mise.jdx.dev)
  and SSH access to the host as a sudo user.
- **A Tailscale tailnet.** A host is reachable only over the tailnet; nothing
  listens on its public interface.

## Rules for the agent

- **Ask before every command that changes a remote host**, and say what it
  changes. Read-only checks (`ansible -m ping`, `--check --diff`) need no
  approval beyond the first SSH connection.
- **Preview first.** Run each playbook with `--check --diff`, show the
  summary, then apply.
- **Host details and secrets stay out of the repository.** Addresses, key
  paths, disk serials and tokens go only in the gitignored
  `infra/ansible/inventory/`, secrets as mode-600 files in
  `inventory/secrets/`. Never print a secret.
- **Stop on the first failure** and report the command and its output. Don't
  work around a failed check: every role refuses on purpose when something is
  unsafe.
- Ask the user for what only they can do: tailnet admin settings, the auth
  key, which disk to use.

## Steps

### 1. Build

```sh
git clone https://github.com/nathabonfim59/bamboo-raft
cd bamboo-raft
mise install   # Go and protoc, pinned in mise.toml
make build     # bin/bbraft, bin/bbraft-server, bin/bbraft-node, bin/bbraft-guest
```

### 2. Build the guest image

The image build needs root and KVM, so run it on the host or a throwaway KVM
VM, never on the user's laptop without asking. See
[Guest image](guest-image.md).

```sh
make -C images image
```

### 3. Prepare Ansible

Follow [infra: Setup](../infra/README.md#setup-on-your-machine):

```sh
cd infra/ansible
mise trust && mise install
ansible-galaxy collection install -r requirements.yml
cp -r inventory.example inventory   # then fill hosts.yml and host_vars/
```

The example `host_vars` read the binaries and guest images from
`bbraft_deploy_dir` (`~/.cache/bamboo-raft/deploy` by default): copy them
there, or point the variables at where they are.

Generate the secrets the inventory reads (Postgres password, gateway token,
node token, API key, 32-byte master key) into `inventory/secrets/`, mode 600.
**Tell the user to back up `master-key`**: without it, encrypted data in
PostgreSQL cannot be read.

### 4. Tailnet

Ask the user to do these in the Tailscale admin console, then wait for them.
The details are in [Before the first run](../infra/README.md#before-the-first-run):

- a `tag:bbraft` tag and a grant to it on `tcp:22`, `tcp:443` (or `tcp:80`)
  and `tcp:22000-22999`;
- MagicDNS on, and HTTPS certificates on unless they choose HTTP
  ([the trade-off](../infra/README.md#api-access-https-or-http));
- a tagged, pre-approved, single-use auth key saved to a mode-600 file.

### 5. Deploy

From `infra/ansible`, in this order, each with `--check --diff` first. See
[Order](../infra/README.md#order).

```sh
ansible-playbook site.yml --tags tailscale   # join the tailnet
# set ansible_host to the tailnet name it printed, then: ansible nodes -m ping
ansible-playbook site.yml --tags hardening   # close the public interface
ansible-playbook site.yml --tags bbraft      # PostgreSQL, bbraft-server, bbraft-node
```

### 6. Check it works

```sh
export BBRAFT_API_URL=https://<host>.<tailnet>.ts.net
bin/bbraft login --key-stdin < infra/ansible/inventory/secrets/api-key
id="$(bin/bbraft new --json --type small --ttl 600 --name hello \
  | jq -r 'select(.event == "ready") | .id')"
bin/bbraft exec "$id" -- uname -a
bin/bbraft delete "$id" --yes --wait
```

If the CLI can't resolve the tailnet name but `curl` can, see
[Tailnet names and the static binary](cli.md#tailnet-names-and-the-static-binary).

### 7. Save a ready-to-use snapshot

This is what makes every next sandbox fast. Set one sandbox up the way your
work needs it, save its disk and start new sandboxes from it:

```sh
id="$(bin/bbraft new --json --name setup | jq -r 'select(.event == "ready") | .id')"
bin/bbraft ssh "$id" -- ./setup.sh           # whatever your project installs
bin/bbraft snapshot "$id" ready-to-test
bin/bbraft new --from ready-to-test          # ready in seconds, already set up
```

Report to the user: the API URL, where the API key file is, the master key
backup reminder, and the snapshot name.
