#!/usr/bin/env bash
# Fast regressions for immutable image v1: the published version bump, the
# SSH/growth build wiring, and the documented contract.
#
# These checks are deliberately text based: the image build and QEMU boot
# happen only in a KVM VM (docs/guest-image.md, images/Makefile). The manifest
# schema stays version 1 with no new required fields, so the existing node and
# verifier still accept the directory.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "$script_dir/.." && pwd -P)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-image-v1.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

# 1. Immutable version bump: v1 is a new directory, never a rebuild of v0.
version=$(tr -d '[:space:]' <"$script_dir/version")
if [ "$version" = "ubuntu-24.04-v1" ]; then
	pass "image version is $version"
else
	fail "image version is '$version', want ubuntu-24.04-v1 (never mutate a published version)"
fi

# 2. Required packages for sshd and partition growth are in the mkosi config.
packages=$(awk '
	/^\[/ { section = $0; next }
	section == "[Content]" && /^[[:space:]]+[a-z0-9][a-z0-9+.-]*[[:space:]]*$/ {
		line = $0
		gsub(/^[[:space:]]+/, "", line)
		gsub(/[[:space:]]+$/, "", line)
		print line
	}
' "$script_dir/mkosi.conf")
for want in openssh-server openssh-client openssh-sftp-server cloud-guest-utils; do
	if printf '%s\n' "$packages" | grep -qx "$want"; then
		pass "mkosi.conf packages $want"
	else
		fail "mkosi.conf is missing package $want"
	fi
done

# 3. build.sh requires the SSH files and refuses baked SSH material. The
# manifest stays schemaVersion 1 with the fields the node already decodes.
for want in \
	'usr/local/sbin/bbraft-ssh-host-key' \
	'etc/ssh/sshd_config' \
	'usr/lib/openssh/sftp-server' \
	'schemaVersion: 1'; do
	if grep -qF -- "$want" "$script_dir/build.sh"; then
		pass "build.sh mentions $want"
	else
		fail "build.sh is missing $want"
	fi
done
if grep -qF 'compgen -G "$root/etc/ssh/ssh_host_*"' "$script_dir/build.sh" &&
	grep -q 'must not bake an authorized_keys' "$script_dir/build.sh"; then
	pass "build.sh rejects baked host keys and authorized_keys"
else
	fail "build.sh does not reject baked SSH material"
fi
if grep -qF 'ssh:' "$script_dir/build.sh" || grep -qF 'growth: {' "$script_dir/build.sh"; then
	fail "build.sh adds manifest fields the node does not need"
else
	pass "build.sh keeps the manifest schema unchanged"
fi
if grep -q 'has("ssh")\|generatedPerBoot' "$script_dir/verify.sh"; then
	fail "verify.sh requires new manifest fields (old images must still verify)"
else
	pass "verify.sh keeps the existing compatibility contract"
fi

# 4. smoke.sh boots twice and compares the per-boot host key fingerprint,
# seeds authorized_keys on boot 1 and checks persistence on boot 2, and the
# in-guest check prints the fingerprint the comparison consumes.
if grep -q 'boot_and_check 1' "$script_dir/smoke.sh" &&
	grep -q 'boot_and_check 2' "$script_dir/smoke.sh" &&
	grep -q -- '-poweroff' "$script_dir/smoke.sh" &&
	grep -q -- '-expect-root-gib' "$script_dir/smoke.sh" &&
	grep -q 'qemu-img resize "$overlay"' "$script_dir/smoke.sh" &&
	grep -q "sshd host key ED25519" "$script_dir/smoke.sh" &&
	grep -q -- '-boot-label' "$script_dir/smoke.sh"; then
	pass "smoke.sh wires rotation, persistence, resume growth and the boot label"
else
	fail "smoke.sh does not wire the two-boot rotation/persistence/growth checks"
fi
if grep -q "PASS sshd host key ED25519" \
	"$script_dir/mkosi.extra/usr/local/bin/bbraft-toolchain-check"; then
	pass "guest check prints the host key fingerprint"
else
	fail "guest check does not print the host key fingerprint"
fi
for want in 'BBRAFT_SMOKE_BOOT' 'authorized_keys persisted across boot'; do
	if grep -qF -- "$want" \
		"$script_dir/mkosi.extra/usr/local/bin/bbraft-toolchain-check"; then
		pass "guest check: $want"
	else
		fail "guest check is missing $want"
	fi
done

# 5. Documentation records the canonical key paths and per-boot semantics,
# including the paths guest AddSSHKey consumers use.
docs="$repo_root/docs/guest-image.md"
for want in \
	'ubuntu-24.04-v1' \
	'ed25519' \
	'/run/ssh/ssh_host_ed25519_key' \
	'/run/ssh/ssh_host_ed25519_key.pub' \
	'per boot' \
	'growpart' \
	'AddSSHKey'; do
	if grep -qF -- "$want" "$docs"; then
		pass "docs/guest-image.md documents $want"
	else
		fail "docs/guest-image.md does not document $want"
	fi
done

if [ "$failures" -ne 0 ]; then
	printf 'image_v1_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'image_v1_test: all checks passed\n'
