#!/usr/bin/env bash
# Build the bamboo-raft Ubuntu 24.04 guest image.
#
# Output (fixed layout, one immutable directory per image version):
#
#   images/out/<version>/kernel
#   images/out/<version>/initrd
#   images/out/<version>/root.qcow2
#   images/out/<version>/manifest.json
#   images/out/<version>/packages.json
#
# The build needs root (or passwordless sudo) because root.qcow2 must contain
# files owned by uid/gid 1000 and mkfs.ext4 -d preserves those owners. QEMU
# smoke tests run unprivileged (images/smoke.sh). See docs/guest-image.md.
# IMAGE_VERSION and MKOSI_VERSION are defined by pins.sh below; the source path
# is resolved from $script_dir at runtime, so a standalone scan cannot see them.
# shellcheck disable=SC2153
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "$script_dir/.." && pwd -P)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=pins.sh
# shellcheck disable=SC1091
. "$script_dir/pins.sh"

image_version=$IMAGE_VERSION
disk_gib=$IMAGE_DISK_GIB
out_root=$IMAGE_OUT
cache=$IMAGE_CACHE
ubuntu_snapshot=$UBUNTU_SNAPSHOT
ubuntu_mirror=""
mkosi_output=bbraft-ubuntu-24.04
source_date_epoch=${SOURCE_DATE_EPOCH:-$SOURCE_DATE_EPOCH_DEFAULT}

die() {
	printf 'images/build: %s\n' "$*" >&2
	exit 1
}

# Explicit test-only opt-in. Only an unset variable or exactly 1 enables the
# unpinned archive build; nothing else can drift into that mode implicitly.
test_unpinned=${BBRAFT_IMAGE_TEST_UNPINNED:-}
case "$test_unpinned" in
"" | 1) ;;
*)
	die "BBRAFT_IMAGE_TEST_UNPINNED must be unset or 1, got '$test_unpinned'"
	;;
esac
if [ "$test_unpinned" = 1 ]; then
	case "$IMAGE_VERSION" in
	*"$TEST_UNPINNED_SUFFIX")
		# BEGIN DOUBLE TEST MARKER GUARD
		# One suffix is the one-marker rule; the base before it must not
		# carry an earlier marker (for example x-unpinned-test-y-unpinned-test).
		base_without_suffix=${IMAGE_VERSION%"$TEST_UNPINNED_SUFFIX"}
		case "$base_without_suffix" in
		*unpinned-test*)
			die "test image base '$IMAGE_VERSION' carries more than one test-only marker"
			;;
		esac
		# END DOUBLE TEST MARKER GUARD
		image_version=$IMAGE_VERSION
		;;
	*unpinned-test*)
		die "test image base '$IMAGE_VERSION' contains the test-only marker but does not end with '$TEST_UNPINNED_SUFFIX'"
		;;
	*)
		image_version="$IMAGE_VERSION$TEST_UNPINNED_SUFFIX"
		;;
	esac
	if [ "${#image_version}" -gt 64 ]; then
		die "test image id '$image_version' is longer than 64 characters; shorten BBRAFT_IMAGE_VERSION"
	fi
	out_root=$IMAGE_TEST_OUT
	cache=$IMAGE_TEST_CACHE
	ubuntu_snapshot=$UBUNTU_UNPINNED_SNAPSHOT
	ubuntu_mirror=$UBUNTU_UNPINNED_MIRROR
else
	case "$IMAGE_VERSION" in
	*unpinned-test*)
		die "pinned image id '$IMAGE_VERSION' contains the test-only marker; set BBRAFT_IMAGE_TEST_UNPINNED=1 for the test-only unpinned build"
		;;
	esac
fi

usage() {
	cat <<'EOF'
usage: images/build.sh [--help]

Builds the pinned Ubuntu 24.04 guest image into images/out/<version>/.
No arguments: all knobs are environment variables documented in
docs/guest-image.md (BBRAFT_IMAGE_OUT, BBRAFT_IMAGE_CACHE,
BBRAFT_IMAGE_VERSION, BBRAFT_IMAGE_DISK_GIB, SOURCE_DATE_EPOCH).

Test-only unpinned archive mode (never for release, VM-only):
BBRAFT_IMAGE_TEST_UNPINNED=1 selects images/out-unpinned-test and
images/.cache-unpinned-test with id <version>-unpinned-test,
distribution.snapshot=unpinned and distribution.mirror. Override the
disposable roots with BBRAFT_IMAGE_TEST_OUT and BBRAFT_IMAGE_TEST_CACHE.
EOF
}

case "${1:-}" in
--help | -h)
	usage
	exit 0
	;;
"") ;;
*)
	usage >&2
	exit 2
	;;
esac

# Validate every value that later feeds root-side arithmetic or deletion
# before doing any privileged work. Image ids must be one safe path component.
validate_inputs() {
	case "$image_version" in
	"" | "." | "..")
		die "invalid image version '$image_version'"
		;;
	*[!a-z0-9._-]*)
		die "invalid image version '$image_version'; use only lowercase letters, digits, '.', '_' and '-'"
		;;
	esac
	case "$image_version" in
	[!a-z0-9]*)
		die "invalid image version '$image_version'; must start with a lowercase letter or digit"
		;;
	esac
	if [ "${#image_version}" -gt 64 ]; then
		die "image version '$image_version' is longer than 64 characters"
	fi
	case "$disk_gib" in
	"" | 0* | *[!0-9]*)
		die "BBRAFT_IMAGE_DISK_GIB must be a positive integer without leading zeros, got '$disk_gib'"
		;;
	esac
	if [ "${#disk_gib}" -gt 4 ]; then
		die "BBRAFT_IMAGE_DISK_GIB is too large, got '$disk_gib' (maximum 1024)"
	fi
	if [ "$disk_gib" -lt 1 ] || [ "$disk_gib" -gt 1024 ]; then
		die "BBRAFT_IMAGE_DISK_GIB must be between 1 and 1024, got '$disk_gib'"
	fi
	case "$source_date_epoch" in
	"" | 0* | *[!0-9]*)
		die "SOURCE_DATE_EPOCH must be a positive integer without leading zeros, got '$source_date_epoch'"
		;;
	esac
	if [ "${#source_date_epoch}" -gt 10 ]; then
		die "SOURCE_DATE_EPOCH is too large, got '$source_date_epoch'"
	fi
	if [ "$source_date_epoch" -lt 1 ] || [ "$source_date_epoch" -gt 4102444800 ]; then
		die "SOURCE_DATE_EPOCH must be between 1 and 4102444800 (2100-01-01), got '$source_date_epoch'"
	fi
	case "$out_root:$cache" in
	/:* | *:/)
		die "BBRAFT_IMAGE_OUT and BBRAFT_IMAGE_CACHE must not be '/'"
		;;
	esac
	if [ -e "$out_root/$image_version" ]; then
		die "image version '$image_version' already exists at $out_root/$image_version; bump images/version instead of replacing it"
	fi
}

# Keep modes apart by cache/output root, not merely by image id. The shared
# canonical_path/paths_overlap helpers live in pins.sh: readlink -m resolves
# existing symlink components, and equal/overlapping roots (including the
# root itself) are rejected before any download or mkdir.
validate_mode_paths() {
	local pinned_out pinned_cache test_out test_cache
	pinned_out=$(canonical_path "$IMAGE_OUT")
	pinned_cache=$(canonical_path "$IMAGE_CACHE")
	test_out=$(canonical_path "$IMAGE_TEST_OUT")
	test_cache=$(canonical_path "$IMAGE_TEST_CACHE")
	# The disposable test roots must stay disjoint from each other in every
	# mode, including a pinned invocation that never uses them.
	if paths_overlap "$test_out" "$test_cache"; then
		die "test-only mode output '$IMAGE_TEST_OUT' and cache '$IMAGE_TEST_CACHE' must not equal or overlap"
	fi
	# BEGIN CROSS MODE ISOLATION GUARD
	# Compare the configured pinned roots with the configured test roots in
	# both modes. A pinned invocation must not be able to reuse an unpinned
	# root (or an alias of it) through BBRAFT_IMAGE_CACHE/BBRAFT_IMAGE_OUT.
	if paths_overlap "$pinned_out" "$test_out" ||
		paths_overlap "$pinned_out" "$test_cache" ||
		paths_overlap "$pinned_cache" "$test_out" ||
		paths_overlap "$pinned_cache" "$test_cache"; then
		die "pinned and test-only image roots must not equal or overlap (pinned out '$IMAGE_OUT', pinned cache '$IMAGE_CACHE', test out '$IMAGE_TEST_OUT', test cache '$IMAGE_TEST_CACHE')"
	fi
	# END CROSS MODE ISOLATION GUARD
}

validate_inputs
validate_mode_paths
export SOURCE_DATE_EPOCH=$source_date_epoch
build_start_epoch=${BBRAFT_BUILD_START:-$(date +%s)}
case "$build_start_epoch" in
"" | 0* | *[!0-9]*)
	die "BBRAFT_BUILD_START must be a positive integer epoch without leading zeros, got '$build_start_epoch'"
	;;
esac
if [ "${#build_start_epoch}" -gt 10 ] || [ "$build_start_epoch" -gt 4102444800 ]; then
	die "BBRAFT_BUILD_START is out of range, got '$build_start_epoch'"
fi
export BBRAFT_BUILD_START=$build_start_epoch

# BEGIN PREFLIGHT
# Match mkosi.bootloader.python_binary: only an interpreter under /usr is used
# for the pefile import; outside /usr mkosi falls back to python3. Validate the
# exact interpreter mkosi will use, and pin it via MKOSI_INTERPRETER so sudo
# and the bin/mkosi shim agree.
preflight_mkosi_python() {
	local requested=${MKOSI_INTERPRETER:-python3} candidate resolved
	candidate=$(command -v "$requested" 2>/dev/null || true)
	[ -n "$candidate" ] ||
		die "host Python '$requested' not found; install the Ubuntu packages python3 and python3-pefile"
	resolved=$(readlink -f -- "$candidate")
	case "$resolved" in
	/usr/*) ;;
	*)
		die "host Python '$candidate' resolves outside /usr ($resolved); mkosi's python_binary only uses an interpreter under /usr for the pefile import; use /usr/bin/python3 with python3-pefile"
		;;
	esac
	if ! "$resolved" -c 'import pefile' >/dev/null 2>&1; then
		die "host Python ($resolved) cannot import pefile; install the Ubuntu package python3-pefile"
	fi
	MKOSI_INTERPRETER=$resolved
	export MKOSI_INTERPRETER
}
preflight_mkosi_python
# END PREFLIGHT

# BEGIN GO PREFLIGHT
# Resolve the real Go binary (through mise/shim wrappers if needed) and require
# the go.mod toolchain before sudo or any download. GO may be a bare command
# name (make/shell injection); never let sudo's secure_path pick another Go.
preflight_go() {
	local requested=${GO:-go} candidate goroot resolved goversion
	candidate=$(command -v "$requested" 2>/dev/null || true)
	[ -n "$candidate" ] ||
		die "Go toolchain '$requested' not found; install Go $GO_VERSION or set GO=/absolute/path/to/go"
	goroot=$("$candidate" env GOROOT 2>/dev/null || true)
	[ -n "$goroot" ] || die "cannot read GOROOT from '$candidate'"
	case "$goroot" in
	/*) ;;
	*) die "GOROOT from '$candidate' is not absolute: '$goroot'" ;;
	esac
	resolved="$goroot/bin/go"
	[ -x "$resolved" ] || die "resolved Go binary '$resolved' is not executable"
	goversion=$(GOTOOLCHAIN=local "$resolved" env GOVERSION 2>/dev/null || true)
	[ "$goversion" = "go$GO_VERSION" ] ||
		die "Go toolchain '$resolved' reports '$goversion' with GOTOOLCHAIN=local, want go$GO_VERSION (images/pins.sh and go.mod)"
	GO=$resolved
	export GO
}

gomod_go=$(awk '$1 == "go" { print $2; exit }' "$repo_root/go.mod")
[ -n "$gomod_go" ] || die "cannot read the go directive from $repo_root/go.mod"
if [ "$gomod_go" != "$GO_VERSION" ]; then
	die "images/pins.sh GO_VERSION=$GO_VERSION does not match go.mod go $gomod_go"
fi
preflight_go
# END GO PREFLIGHT

# Read-only host QEMU evidence for the review log. No QEMU is launched here.
print_host_qemu_version() {
	local tool=$1 path
	if path=$(command -v "$tool" 2>/dev/null); then
		printf 'images/build: host %s (%s): %s\n' "$tool" "$path" "$("$tool" --version 2>/dev/null | head -n 1)" >&2
	else
		printf 'images/build: host %s: missing\n' "$tool" >&2
	fi
}
print_host_qemu_version qemu-system-x86_64
print_host_qemu_version qemu-img

# Re-exec through passwordless sudo when invoked by the normal build user.
# The lead runs `make -C images image` in test VMs, where sudo is
# passwordless; anything else gets an explicit error instead of a prompt.
if [ "$(id -u)" -ne 0 ]; then
	if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
		exec sudo -n \
			--preserve-env=BBRAFT_IMAGE_OUT,BBRAFT_IMAGE_CACHE,BBRAFT_IMAGE_VERSION,BBRAFT_IMAGE_DISK_GIB,BBRAFT_IMAGE_TEST_UNPINNED,BBRAFT_IMAGE_TEST_OUT,BBRAFT_IMAGE_TEST_CACHE,SOURCE_DATE_EPOCH,BBRAFT_BUILD_START,GO,MKOSI_INTERPRETER,HTTP_PROXY,HTTPS_PROXY,NO_PROXY,http_proxy,https_proxy,no_proxy \
			-- "$0" "$@"
	fi
	die "must build as root (root.qcow2 keeps uid 1000 ownership): run with sudo or set up passwordless sudo; see docs/guest-image.md"
fi

# Re-check after sudo: env preservation is explicit, but never trust it.
validate_inputs
validate_mode_paths
preflight_mkosi_python
preflight_go
umask 022

need() {
	command -v "$1" >/dev/null 2>&1 || die "missing host tool '$1'; install the packages listed in docs/guest-image.md"
}
for tool in curl sha256sum tar xz python3 mkfs.ext4 e2fsck qemu-img jq git apt-get dpkg chroot; do
	need "$tool"
done
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)' ||
	die "mkosi ${MKOSI_VERSION} needs Python >= 3.9 (found $(python3 --version 2>&1))"

mkdir -p "$out_root" "$cache"
out_root=$(cd -- "$out_root" && pwd -P)
cache=$(cd -- "$cache" && pwd -P)
validate_inputs

fetch_verified() {
	local url=$1 want=$2 dest=$3 tmp
	if [ -f "$dest" ] && printf '%s  %s\n' "$want" "$dest" | sha256sum -c --status; then
		return 0
	fi
	tmp="$dest.part.$$"
	rm -f "$tmp"
	printf 'images/build: downloading %s\n' "$url" >&2
	curl -fL --retry 3 --connect-timeout 20 -o "$tmp" "$url" ||
		die "download failed: $url"
	if ! printf '%s  %s\n' "$want" "$tmp" | sha256sum -c --status; then
		rm -f "$tmp"
		die "sha256 mismatch for $url (want $want)"
	fi
	mv -f "$tmp" "$dest"
}

# Pinned mkosi from its source tarball; never apt's mkosi and no host install.
mkosi_dir="$cache/$MKOSI_SOURCE_DIR"
if [ ! -x "$mkosi_dir/bin/mkosi" ]; then
	mkosi_tar="$cache/mkosi-v$MKOSI_VERSION.tar.gz"
	fetch_verified "$MKOSI_URL" "$MKOSI_SHA256" "$mkosi_tar"
	rm -rf "$mkosi_dir.tmp"
	mkdir -p "$mkosi_dir.tmp"
	tar -xzf "$mkosi_tar" -C "$mkosi_dir.tmp" --strip-components=1 ||
		die "cannot extract $mkosi_tar"
	rm -rf "$mkosi_dir"
	mv "$mkosi_dir.tmp" "$mkosi_dir"
	chmod 0755 "$mkosi_dir/bin/mkosi"
fi
mkosi_cmd="$mkosi_dir/bin/mkosi"
mkosi_version=$("$mkosi_cmd" --version | head -n 1)
[ -n "$mkosi_version" ] || die "pinned mkosi does not run"
grep -q "^Snapshot=$UBUNTU_SNAPSHOT$" "$script_dir/mkosi.conf" ||
	die "mkosi.conf Snapshot= does not match pins.sh UBUNTU_SNAPSHOT=$UBUNTU_SNAPSHOT"

build_out="$cache/build-$image_version"
rm -rf "$build_out" "$cache/workspace"
mkdir -p "$build_out"
if [ "$test_unpinned" = 1 ]; then
	printf 'images/build: TEST-ONLY unpinned archive build: mkosi %s, Ubuntu %s %s from %s, version %s, disk %s GiB\n' \
		"$mkosi_version" "$UBUNTU_VERSION" "$UBUNTU_CODENAME" "$ubuntu_mirror" "$image_version" "$disk_gib" >&2
else
	printf 'images/build: mkosi %s, Ubuntu %s %s snapshot %s, version %s, disk %s GiB\n' \
		"$mkosi_version" "$UBUNTU_VERSION" "$UBUNTU_CODENAME" "$ubuntu_snapshot" "$image_version" "$disk_gib" >&2
fi
mkosi_args=(
	--directory "$script_dir"
	--output-dir "$build_out"
	--cache-dir "$cache/mkosi-cache"
	--package-cache-dir "$cache/apt"
	--workspace-dir "$cache/workspace"
)
if [ "$test_unpinned" = 1 ]; then
	# The empty CLI value resets mkosi.conf's Snapshot= for this build only;
	# --mirror selects the ordinary Ubuntu archive. All keyring/signature and
	# checksum behaviour is mkosi's unchanged default.
	mkosi_args+=(--snapshot "" --mirror "$ubuntu_mirror")
	expected_snapshot=none
	expected_mirror=$ubuntu_mirror
else
	mkosi_args+=(--snapshot "$ubuntu_snapshot")
	expected_snapshot=$ubuntu_snapshot
	expected_mirror=none
fi
mkosi_args+=(build)
# Run mkosi's real workspace/source relationship check on these exact args
# and assert the resolved configuration: an empty CLI snapshot must actually
# reset the mkosi.conf pin, not merely appear in argv. Both checks run before
# the build touches packages.
"$script_dir/mkosi_config_check.py" --mkosi "$mkosi_cmd" \
	--expect-snapshot "$expected_snapshot" --expect-mirror "$expected_mirror" \
	-- "${mkosi_args[@]}"
"$mkosi_cmd" "${mkosi_args[@]}"
root="$build_out/$mkosi_output"
[ -d "$root/usr" ] || die "mkosi produced no directory image at $root"
install -d -m 0755 "$root/usr/local/bin" "$root/usr/local/sbin"

# The guest agent comes from cmd/bbraft-guest (A1.1). It is built with the
# host Go and copied in, so the image does not need a Go module cache.
[ -d "$repo_root/cmd/bbraft-guest" ] ||
	die "cmd/bbraft-guest is missing; A1.1 must land before building an image"
printf 'images/build: building bbraft-guest\n' >&2
(
	cd "$repo_root"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOTOOLCHAIN=local \
		GOMODCACHE="$cache/go-mod" GOCACHE="$cache/go-build" \
		"$GO" build \
		-trimpath -buildvcs=false \
		-o "$root/usr/local/bin/bbraft-guest" ./cmd/bbraft-guest
)
chmod 0755 "$root/usr/local/bin/bbraft-guest"

# Pinned Go toolchain (version from go.mod).
go_tar="$cache/go$GO_VERSION.linux-amd64.tar.gz"
fetch_verified "$GO_URL" "$GO_SHA256" "$go_tar"
rm -rf "$root/usr/local/go"
tar -xzf "$go_tar" -C "$root/usr/local"
chmod -R a+rX "$root/usr/local/go"
ln -sfn /usr/local/go/bin/go "$root/usr/local/bin/go"
ln -sfn /usr/local/go/bin/gofmt "$root/usr/local/bin/gofmt"

# Pinned Node.js 24 toolchain.
node_tar="$cache/node-v$NODE_VERSION-linux-x64.tar.xz"
fetch_verified "$NODE_URL" "$NODE_SHA256" "$node_tar"
rm -rf "$root/usr/local/node" "$root/usr/local/node-v$NODE_VERSION-linux-x64"
tar -xJf "$node_tar" -C "$root/usr/local"
mv "$root/usr/local/node-v$NODE_VERSION-linux-x64" "$root/usr/local/node"
for binary in node npm npx corepack; do
	ln -sfn "/usr/local/node/bin/$binary" "$root/usr/local/bin/$binary"
done

# Installed files that build.sh depends on; catch a broken extra tree early.
for path in \
	usr/local/bin/bbraft-guest \
	usr/local/bin/bbraft-toolchain-check \
	usr/local/sbin/bbraft-grow-root \
	usr/local/sbin/bbraft-network-config \
	usr/local/sbin/bbraft-ssh-host-key \
	usr/lib/systemd/system/bbraft-guest.service \
	usr/lib/systemd/system/bbraft-grow-root.service \
	usr/lib/systemd/system/bbraft-network-config.service \
	usr/lib/systemd/system/bbraft-ssh-host-key.service \
	etc/systemd/system/systemd-networkd.service.d/10-bbraft-network-config.conf \
	etc/ssh/sshd_config \
	etc/bbraft/guest.env \
	usr/sbin/sshd \
	usr/bin/ssh-keygen \
	usr/lib/openssh/sftp-server; do
	[ -e "$root/$path" ] || die "image is missing $path (check images/mkosi.extra and mkosi.conf)"
done
chmod 0755 "$root/usr/local/bin/bbraft-toolchain-check" \
	"$root/usr/local/sbin/bbraft-grow-root" \
	"$root/usr/local/sbin/bbraft-network-config" \
	"$root/usr/local/sbin/bbraft-ssh-host-key"

# Image immutability for SSH identity: no private host key may be baked into
# the tree. bbraft-ssh-host-key.service generates one in /run/ssh per boot.
if compgen -G "$root/etc/ssh/ssh_host_*" >/dev/null; then
	die "image contains an SSH host key under /etc/ssh; host keys are per-boot"
fi
[ -d "$root/home/user/.ssh" ] || die "image is missing /home/user/.ssh for authorized_keys"
[ ! -e "$root/home/user/.ssh/authorized_keys" ] ||
	die "image must not bake an authorized_keys; the node installs it through AddSSHKey"

# Kernel and initrd: generated explicitly by mkosi.postinst.chroot. Verify the
# virtio/vsock modules before extracting, so a broken image fails at build time
# instead of at QEMU boot.
mapfile -t kernels < <(find "$root/boot" -maxdepth 1 -type f -name 'vmlinuz-*' | sort -V)
[ "${#kernels[@]}" -gt 0 ] || die "no kernel under $root/boot"
kernel=${kernels[$((${#kernels[@]} - 1))]}
kver=${kernel##*/vmlinuz-}
initrd="$root/boot/initrd.img-$kver"
[ -s "$initrd" ] || die "no initrd for $kver; mkosi.postinst.chroot update-initramfs failed"
initrd_list="$cache/initrd-$kver.list"
[ -x "$root/usr/bin/lsinitramfs" ] || die "initramfs-tools lsinitramfs is missing from the image"
chroot "$root" /usr/bin/lsinitramfs "/boot/initrd.img-$kver" >"$initrd_list" ||
	die "lsinitramfs failed for /boot/initrd.img-$kver"
# Accept a driver only as a real module in the initrd/modules tree or as a
# verified builtin (CONFIG_<symbol>=y or exact modules.builtin path). Ubuntu's
# 6.8.0-149 has virtio_blk/pci/net builtin and vsock/transport as modules.
"$script_dir/check_kernel_modules.sh" "$root" "$kver" "$initrd_list"

# Whole-disk ext4: no partition table, so root=/dev/vda works and the guest
# only has to resize the filesystem when a node grows the overlay.
work=$(mktemp -d "$cache/build.XXXXXX")
staging_root=""
cleanup() {
	status=$?
	trap - EXIT
	if [ -n "${staging_root:-}" ]; then rm -rf "$staging_root"; fi
	if [ -n "${work:-}" ]; then rm -rf "$work"; fi
	exit "$status"
}
trap cleanup EXIT INT TERM

raw="$work/root.raw"
expected_bytes=$((disk_gib * 1024 * 1024 * 1024))
truncate -s "$expected_bytes" "$raw"
printf 'images/build: creating ext4 root (%s GiB)\n' "$disk_gib" >&2
mkfs.ext4 -q -F -L bbraft-root -m 0 -E root_owner=0:0 -d "$root" "$raw"
e2fsck -fn "$raw" >/dev/null || die "e2fsck reports errors in the root filesystem"

# Stage under a parent directory so the leaf keeps the exact image id that
# verify.sh and publish.sh require; never touch an existing version directory.
staging_root="$out_root/.staging.$$"
mkdir -p "$staging_root"
stage="$staging_root/$image_version"
mkdir -p "$stage"
install -m 0644 "$kernel" "$stage/kernel"
install -m 0644 "$initrd" "$stage/initrd"
qemu-img convert -f raw -O qcow2 -o cluster_size=65536 "$raw" "$stage/root.qcow2"
qemu-img check "$stage/root.qcow2" >/dev/null || die "qemu-img check failed"

virtual_size=$(qemu-img info --output=json "$stage/root.qcow2" | jq -r '."virtual-size"')
[ "$virtual_size" = "$expected_bytes" ] ||
	die "root.qcow2 virtual size is $virtual_size, want $expected_bytes"
backing=$(qemu-img info --output=json "$stage/root.qcow2" | jq -r '."backing-filename" // empty')
[ -z "$backing" ] || die "root.qcow2 must be standalone, not backed by '$backing'"

mkosi_manifest="$build_out/$mkosi_output.manifest"
[ -s "$mkosi_manifest" ] || die "mkosi did not write $mkosi_manifest"
jq empty "$mkosi_manifest" || die "mkosi package manifest is not valid JSON"
cp "$mkosi_manifest" "$stage/packages.json"

# Fixed filenames and real hashes in the node-facing manifest.
created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sha_kernel=$(sha256sum "$stage/kernel" | cut -d' ' -f1)
sha_initrd=$(sha256sum "$stage/initrd" | cut -d' ' -f1)
sha_root=$(sha256sum "$stage/root.qcow2" | cut -d' ' -f1)
sha_packages=$(sha256sum "$stage/packages.json" | cut -d' ' -f1)
size_kernel=$(stat -c %s "$stage/kernel")
size_initrd=$(stat -c %s "$stage/initrd")
size_root=$(stat -c %s "$stage/root.qcow2")
size_packages=$(stat -c %s "$stage/packages.json")
podman_version=$("$script_dir/dpkg_version.sh" "$root/var/lib/dpkg/status" podman) ||
	die "cannot read the podman package version from the guest dpkg status"
build_duration=$(( $(date +%s) - build_start_epoch ))
jq -n \
	--arg id "$image_version" \
	--arg created_at "$created_at" \
	--argjson source_date_epoch "$SOURCE_DATE_EPOCH" \
	--arg ubuntu_version "$UBUNTU_VERSION" \
	--arg ubuntu_codename "$UBUNTU_CODENAME" \
	--arg ubuntu_architecture "$UBUNTU_ARCH" \
	--arg ubuntu_snapshot "$ubuntu_snapshot" \
	--arg ubuntu_mirror "$ubuntu_mirror" \
	--arg kernel_version "$kver" \
	--arg mkosi_version "$mkosi_version" \
	--arg go_version "$GO_VERSION" \
	--arg node_version "$NODE_VERSION" \
	--arg podman_version "$podman_version" \
	--argjson disk_gib "$disk_gib" \
	--argjson virtual_size "$virtual_size" \
	--argjson build_duration "$build_duration" \
	--arg sha_kernel "$sha_kernel" --argjson size_kernel "$size_kernel" \
	--arg sha_initrd "$sha_initrd" --argjson size_initrd "$size_initrd" \
	--arg sha_root "$sha_root" --argjson size_root "$size_root" \
	--arg sha_packages "$sha_packages" --argjson size_packages "$size_packages" \
	'{
		schemaVersion: 1,
		id: $id,
		createdAt: $created_at,
		sourceDateEpoch: $source_date_epoch,
		distribution: ({
			id: "ubuntu",
			version: $ubuntu_version,
			codename: $ubuntu_codename,
			architecture: $ubuntu_architecture,
			snapshot: $ubuntu_snapshot
		} + (if $ubuntu_mirror == "" then {} else {mirror: $ubuntu_mirror} end)),
		kernel: { version: $kernel_version },
		root: {
			filesystem: "ext4",
			format: "qcow2",
			diskGiB: $disk_gib,
			virtualSizeBytes: $virtual_size,
			growth: "whole-disk ext4 resized online by bbraft-grow-root.service before bbraft-guest.service (growpart first when root is a partition)"
		},
		toolchain: {
			go: $go_version,
			node: $node_version,
			podmanPackage: $podman_version
		},
		build: { mkosi: $mkosi_version, durationSeconds: $build_duration },
		artifacts: {
			kernel: { sha256: $sha_kernel, size: $size_kernel },
			initrd: { sha256: $sha_initrd, size: $size_initrd },
			"root.qcow2": { sha256: $sha_root, size: $size_root },
			"packages.json": { sha256: $sha_packages, size: $size_packages }
		}
	}' >"$stage/manifest.json"

"$script_dir/verify.sh" "$stage"

dest="$out_root/$image_version"
if [ "$test_unpinned" = 1 ]; then
	# Test-only materialization: publish.sh refuses test-marked manifests, so
	# the disposable output has its own no-clobber stager that only accepts a
	# suffixed, unpinned, mirrored image under IMAGE_TEST_OUT.
	"$script_dir/stage_test.sh" "$stage" "$dest"
else
	"$script_dir/publish.sh" "$stage" "$dest"
fi
rmdir "$staging_root"
staging_root=""
if [ "$test_unpinned" = 1 ]; then
	printf 'images/build: TEST-ONLY materialized %s (duration %ss)\n' "$dest" "$build_duration"
else
	printf 'images/build: published %s (duration %ss)\n' "$dest" "$build_duration"
fi
(
	cd "$dest"
	sha256sum kernel initrd root.qcow2 manifest.json packages.json
)

# Artifacts belong to the invoking user, not root.
if [ -n "${SUDO_UID:-}" ] && [ "$SUDO_UID" != "0" ]; then
	chown -R "$SUDO_UID:${SUDO_GID:-$SUDO_UID}" "$out_root" "$cache"
fi
