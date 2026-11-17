#!/usr/bin/env bash
# Fast regressions for per-boot SSH host identity.
#
# Contract (docs/guest-image.md):
# - the image contains no host key and no authorized_keys;
# - sshd uses exactly one HostKey in /run (tmpfs), ed25519 only, public key
#   authentication only, sandbox user only, no root login;
# - bbraft-ssh-host-key.service generates the key before every sshd entry
#   point, once per boot (it also proves sshd accepts the config);
# - the generator makes a fresh valid ed25519 key, keeps it 0600 root, and
#   removes stale keys from /etc;
# - the postinst enables the units, disables socket activation, and creates
#   the user's authorized_keys directory without baking a key.
#
# The script exercises the real ssh-keygen/sshd when present and a fake
# generator otherwise, and reproduces the pre-fix failure (a HostKey under
# /etc and a baked key in the extra tree).
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-ssh-key.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

config="$script_dir/mkosi.extra/etc/ssh/sshd_config"
unit="$script_dir/mkosi.extra/usr/lib/systemd/system/bbraft-ssh-host-key.service"
generator="$script_dir/mkosi.extra/usr/local/sbin/bbraft-ssh-host-key"
postinst="$script_dir/mkosi.postinst.chroot"

check_config() { # <sshd_config>; reports missing required directives
	local file=$1 missing=0 want
	local required=(
		"HostKey /run/ssh/ssh_host_ed25519_key"
		"HostKeyAlgorithms ssh-ed25519"
		"PubkeyAuthentication yes"
		"PasswordAuthentication no"
		"PermitRootLogin no"
		"AllowUsers user"
		"AuthorizedKeysFile .ssh/authorized_keys"
		"UsePAM yes"
		"StrictModes yes"
		"Subsystem sftp /usr/lib/openssh/sftp-server"
	)
	for want in "${required[@]}"; do
		if ! grep -qxF -- "$want" "$file"; then
			printf 'missing sshd directive: %s\n' "$want" >&2
			missing=1
		fi
	done
	# No second host key source, and no image-provided authorized_keys.
	if grep -qE '^[[:space:]]*HostKey[[:space:]]+/etc/' "$file"; then
		printf 'sshd_config names an /etc host key\n' >&2
		missing=1
	fi
	if [ "$(grep -cE '^[[:space:]]*HostKey[[:space:]]' "$file")" -ne 1 ]; then
		printf 'sshd_config must name exactly one HostKey\n' >&2
		missing=1
	fi
	return "$missing"
}

check_extra_tree() { # <dir>; fails if a host key is committed in the image tree
	local dir=$1 found
	found=$(find "$dir" -type f \( -name 'ssh_host_*' -o -name 'authorized_keys' \) 2>/dev/null || true)
	if [ -n "$found" ]; then
		printf 'baked SSH material in the image tree:\n%s\n' "$found" >&2
		return 1
	fi
	return 0
}

# 1. Committed configuration.
if check_config "$config"; then
	pass "sshd_config has the required host-key/auth directives"
else
	fail "sshd_config is missing a required directive"
fi
if check_extra_tree "$script_dir/mkosi.extra"; then
	pass "image tree has no host key or authorized_keys"
else
	fail "image tree contains baked SSH material"
fi

# sshd itself validates option names and interactions when available.
if command -v sshd >/dev/null 2>&1 && command -v ssh-keygen >/dev/null 2>&1; then
	ssh-keygen -q -t ed25519 -N '' -f "$tmp/validate-key"
	sed "s#/run/ssh/ssh_host_ed25519_key#$tmp/validate-key#" "$config" >"$tmp/sshd.conf"
	chmod 0600 "$tmp/validate-key"
	if sshd -t -f "$tmp/sshd.conf" 2>"$tmp/sshd.err"; then
		pass "sshd -t accepts the committed config"
	else
		fail "sshd -t rejects the committed config: $(cat "$tmp/sshd.err")"
	fi
else
	printf 'skip sshd -t: sshd or ssh-keygen not installed on this host\n'
fi

# 2. Unit ordering: before every sshd entry point and guest readiness, once
# per boot.
for want in \
	"Before=ssh.service ssh.socket ssh@.service bbraft-guest.service" \
	"ConditionPathExists=!/run/ssh/ssh_host_ed25519_key" \
	"ExecStart=/usr/local/sbin/bbraft-ssh-host-key" \
	"RemainAfterExit=yes"; do
	if grep -qxF -- "$want" "$unit"; then
		pass "unit: $want"
	else
		fail "unit is missing '$want'"
	fi
done
guest_unit="$script_dir/mkosi.extra/usr/lib/systemd/system/bbraft-guest.service"
if grep -qxF 'Wants=bbraft-ssh-host-key.service network-online.target' "$guest_unit" &&
	grep -qE '^After=.*bbraft-ssh-host-key\.service' "$guest_unit"; then
	pass "guest agent is ordered after the per-boot host key"
else
	fail "guest agent is not ordered after the per-boot host key"
fi

# 3. Postinst: enable key unit + ssh.service, disable socket activation,
# create the authorized_keys directory, and leave no key in the image.
for want in \
	"systemctl enable bbraft-ssh-host-key.service ssh.service" \
	"systemctl disable ssh.socket" \
	"install -d -m 0700 -o user -g user /home/user/.ssh"; do
	if grep -qF -- "$want" "$postinst"; then
		pass "postinst: $want"
	else
		fail "postinst is missing '$want'"
	fi
done
if grep -q '/etc/ssh/ssh_host_\*' "$postinst"; then
	pass "postinst removes host keys from /etc/ssh"
else
	fail "postinst does not remove host keys from /etc/ssh"
fi

# 4. Generator behavior. Always check ssh-keygen arguments with a fake; use
# the real tool as well when this host has it.
mkdir -p "$tmp/fakebin" "$tmp/run/ssh" "$tmp/etc/ssh" "$tmp/wants"
cat >"$tmp/fakebin/ssh-keygen" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_SSH_KEYGEN_LOG"
key=""
while [ $# -gt 0 ]; do
	case "$1" in
	-f)
		key=$2
		shift 2
		;;
	*) shift ;;
	esac
done
[ -n "$key" ] || exit 2
printf 'fake-private\n' >"$key"
printf 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFake fake\n' >"$key.pub"
EOF
chmod 0755 "$tmp/fakebin/ssh-keygen"
printf 'stale\n' >"$tmp/etc/ssh/ssh_host_ed25519_key"
printf 'stale\n' >"$tmp/etc/ssh/ssh_host_rsa_key"
FAKE_SSH_KEYGEN_LOG="$tmp/keygen.log" \
	BBRAFT_SSH_KEY_DIR="$tmp/run/ssh" BBRAFT_SSH_PRIVSEP_DIR="$tmp/run/sshd" \
	BBRAFT_SSH_ETC_DIR="$tmp/etc/ssh" \
	PATH="$tmp/fakebin:$PATH" "$generator" >"$tmp/generator.out" 2>&1 ||
	fail "generator failed with a fake ssh-keygen: $(cat "$tmp/generator.out")"
if grep -q -- '-t ed25519' "$tmp/keygen.log" && grep -q -- "-N " "$tmp/keygen.log"; then
	pass "generator asks ssh-keygen for an ed25519 key without a passphrase"
else
	fail "generator invoked ssh-keygen without -t ed25519 -N: $(cat "$tmp/keygen.log")"
fi
if [ -f "$tmp/run/ssh/ssh_host_ed25519_key" ] &&
	[ "$(stat -c %a "$tmp/run/ssh/ssh_host_ed25519_key")" = "600" ] &&
	[ "$(stat -c %a "$tmp/run/ssh/ssh_host_ed25519_key.pub")" = "644" ]; then
	pass "generator writes a 0600 private key and 0644 public key"
else
	fail "generator modes are wrong: $(ls -l "$tmp/run/ssh" 2>&1)"
fi
if [ ! -e "$tmp/etc/ssh/ssh_host_ed25519_key" ] && [ ! -e "$tmp/etc/ssh/ssh_host_rsa_key" ]; then
	pass "generator removes stale /etc host keys"
else
	fail "generator left stale /etc host keys"
fi

if command -v ssh-keygen >/dev/null 2>&1; then
	real="$tmp/real"
	mkdir -p "$real/run/ssh" "$real/etc/ssh"
	BBRAFT_SSH_KEY_DIR="$real/run/ssh" BBRAFT_SSH_PRIVSEP_DIR="$real/run/sshd" \
		BBRAFT_SSH_ETC_DIR="$real/etc/ssh" \
		"$generator" >"$real/first.out" 2>&1
	first=$(ssh-keygen -lf "$real/run/ssh/ssh_host_ed25519_key.pub" | awk '{print $2, $4, $5}')
	case "$first" in
	"SHA256:"*"ED25519"*) pass "generator produced a valid ed25519 key: $first" ;;
	*) fail "generator key is not a valid ed25519 key: '${first:-<none>}'" ;;
	esac
	BBRAFT_SSH_KEY_DIR="$real/run/ssh" BBRAFT_SSH_PRIVSEP_DIR="$real/run/sshd" \
		BBRAFT_SSH_ETC_DIR="$real/etc/ssh" \
		"$generator" >"$real/second.out" 2>&1
	second=$(ssh-keygen -lf "$real/run/ssh/ssh_host_ed25519_key.pub" | awk '{print $2}')
	first_only=${first%% *}
	if [ "$first_only" != "$second" ]; then
		pass "generator makes a fresh key on every run"
	else
		fail "generator reused the same host key"
	fi
else
	printf 'skip real key generation: ssh-keygen not installed on this host\n'
fi

# 5. Pre-fix reproductions: the checks above must reject the old shape.
sed 's#^HostKey /run/ssh/ssh_host_ed25519_key#HostKey /etc/ssh/ssh_host_ed25519_key#' \
	"$config" >"$tmp/old.conf"
if check_config "$tmp/old.conf" >"$tmp/old.log" 2>&1; then
	fail "config check accepted a baked /etc HostKey (pre-fix shape)"
else
	pass "config check rejects a baked /etc HostKey"
fi
mkdir -p "$tmp/baked/etc/ssh"
printf 'old key\n' >"$tmp/baked/etc/ssh/ssh_host_ed25519_key"
if check_extra_tree "$tmp/baked" >"$tmp/baked.log" 2>&1; then
	fail "tree check accepted a baked host key (pre-fix shape)"
else
	pass "tree check rejects a baked host key"
fi

if [ "$failures" -ne 0 ]; then
	printf 'ssh_host_key_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'ssh_host_key_test: all checks passed\n'
