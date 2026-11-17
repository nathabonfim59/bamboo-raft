// Package nodenet owns a node's VM network: the per-node bridge, the private
// IPv4 pool, each VM's TAP and vhost-net file descriptors, nftables isolation,
// IPv4 NAT, bounded SSH ingress ports and the durable allocation identity used
// after a restart.
//
// It is deliberately the only package that creates host network resources for
// sandboxes. Every address, port and rule is derived from the manager's state,
// so a repeated Setup is an adoption of the same identity and Release is
// idempotent.
//
// # Owned resources and non-negotiables
//
// The manager owns exactly these host objects:
//
//   - the bridge named Config.Bridge, which it creates if it is absent and
//     refuses to touch if the name exists as another kind of link;
//   - one persistent TAP per sandbox (bridge slave, port isolation on, a host
//     MAC distinct from the guest's, ownership alias such as
//     "nodenet:bx_f7k2q9hd") and one /dev/vhost-net descriptor, which is not
//     optional: a host without vhost-net fails CheckHost and Setup instead of
//     silently using userspace virtio networking;
//   - the nftables tables inet bbraft_net and bridge bbraft_bridge, replaced
//     atomically in one `nft -f` transaction;
//   - one state file per sandbox in Config.StateDir, holding the guest address,
//     MAC, TAP, ingress address, ingress interface and SSH port;
//   - TCP ports from Config.SSHPorts on Config.IngressAddr, optionally bound
//     to Config.IngressIface.
//
// It never runs `flush ruleset`, never deletes or edits another table, and
// never deletes a link it did not create: ownership is proven by the alias, not
// by a name or a link kind, so a same-name foreign link is reported and left
// alone. In particular the inet bbraft_filter table belongs to
// infra/ansible/roles/hardening: this package only reads that template in tests
// and must never modify it. The hardening role's forward chain accepts IPv4
// traffic on the configured VM bridge; this package's own forward chain then
// drops everything that is not explicitly permitted, so a missing or changed
// hardening rule cannot widen VM egress.
//
// # Isolation model
//
// Guests are statically addressed (no DHCP): Config.Prefix, Config.Gateway and
// the configured resolvers go into the QEMU kernel command line through the
// caller (see Lease). The rules enforce:
//
//   - source authenticity: the bridge-family chain inspects every frame from a
//     TAP before it is bridged or routed, and drops a foreign source MAC, a
//     foreign source IPv4 address, a spoofed ARP sender, and any frame that is
//     neither IPv4 nor ARP (which is also what keeps IPv6 out);
//   - VM to VM: bridge slave port isolation, no forwarded frame between two
//     TAPs, no forwarded ARP, a dropped hairpin at the start of the routed
//     forward chain, and a dropped route to any guest or guest-prefix address;
//   - VM to host: the whole inet input hook from the bridge is dropped;
//   - management and other private, link-local, loopback, CGNAT and multicast
//     destinations are dropped, as are the node's own addresses;
//   - IPv6 is dropped at the bridge source check and at the inet input and
//     forward hooks, because a node with a prefix is not part of Phase 1;
//   - DNS (UDP and TCP port 53) is only allowed to Config.Resolvers;
//   - egress is NATed with masquerade on the uplink. The inbound direction
//     accepts only established/related replies, except that a DNAT connection
//     is never admitted by that generic rule, and the exact (ingress address,
//     published port) SSH tuples this package created, never from the guest
//     bridge. When Config.IngressIface is set, DNAT and its exact forward
//     accept are bound to that interface, and the SSH reply must leave through
//     the same interface; the SSH reply is the only established exception on
//     the egress path, so a private (tailnet) client can reach SSH while a
//     guest still cannot reach the same network.
//
// # Failure and restart behavior
//
// Setup performs bridge/TAP/rules/state steps in order. A failure before the
// state commit undoes every step with a context detached from the caller's
// cancellation and returns the joined cleanup errors. The state file is the
// commit point: it is fsynced, renamed and only then its directory is synced.
// When that final sync fails the rename may already be durable, so the manager
// either removes the record durably or keeps the whole reservation (map, rules,
// link and record) for an adopt retry; it never frees an address whose record
// might still exist.
//
// Reconcile (and New's load) re-renders the rules from the state directory. It
// also deletes links that carry this package's ownership alias but have no
// durable allocation, which is the crash between TAP creation and the commit.
// A state file that disagrees with the configuration, a non-derivable MAC or
// TAP, or two allocations sharing an address, port, TAP or MAC is a hard error
// (ErrStateMismatch or a validation error) instead of a silent re-addressing
// of a running guest, and it is detected before any host command runs.
//
// Release quiesces the TAP while its original rules are still installed, then
// deletes only the link that carries the allocation's alias, then removes it
// from the rules, and removes the durable record last. The address and port
// stay reserved until the link is gone and the record is durably removed, so a
// failed or interrupted release is retryable and can never hand a live stale
// TAP's identity to a new sandbox.
//
// # Privileges
//
// Creating TAPs, enslaving them and writing nftables requires CAP_NET_ADMIN;
// the node service account gets exactly that, not root. The node must also be
// an IPv4 router (net.ipv4.ip_forward=1); CheckHost verifies it and fails
// closed. Privileged integration tests are gated by BBRAFT_TEST_NET=1 and only
// run inside a throwaway VM with sudo (see netns_test.go).
package nodenet
