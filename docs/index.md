---
layout: home
hero:
  name: bamboo-raft
  text: Persistent Linux VMs for AI agents
  tagline: Self-hosted. Every sandbox is a jailed QEMU/KVM virtual machine that agents create, run commands in, SSH into, stop, resume and snapshot over one HTTP API and CLI.
  actions:
    - theme: brand
      text: Run a server
      link: /server
    - theme: alt
      text: HTTP API
      link: /api-reference/http/
features:
  - title: Real VMs
    details: Each sandbox is its own QEMU/KVM machine with a persistent disk, not a container. Stop frees CPU and memory; resume boots the same disk.
  - title: Jailed on the host
    details: Own uid, no capabilities, seccomp, private namespaces, a read-only chroot and cgroup v2 limits per VM. No VM reaches the host, its networks or another VM.
  - title: One API, one CLI
    details: Commands (sync, streamed, detached with reconnect), files, signed upload and download URLs, SSH and saved snapshots over /api/v1 and the bbraft CLI.
  - title: Nodes dial in
    details: A KVM host runs bbraft-node, which dials the control plane. The server never connects to nodes or holds their credentials.
---
