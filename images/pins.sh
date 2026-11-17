# shellcheck shell=bash
# The variables below are consumed by the scripts that source this file
# (build.sh, smoke.sh, pins_test.sh); a standalone scan cannot see that use.
# shellcheck disable=SC2034
# Pinned inputs for the bamboo-raft guest image build.
#
# Every version below is quoted in docs/guest-image.md with its upstream
# source and checksum. Bump a version here and in images/version only when the
# image content changes; never rebuild a published version in place.
#
# This file is sourced by images/build.sh (bash) and images/Makefile.

# Image version directory name. Also the manifest "id".
IMAGE_VERSION="${BBRAFT_IMAGE_VERSION:-$(tr -d '[:space:]' <"$(dirname "${BASH_SOURCE[0]}")/version")}"

# Test-only unpinned Ubuntu archive build (orchestrator-authorized for M2
# unblocking; VM-only). The normal id always gains this suffix, so a test
# image can never be mistaken for a release image. Suffixing is idempotent so
# a sudo re-exec cannot double it. Only the apt snapshot selection changes:
# mkosi/Go/Node pins, checksums, signature verification and
# SOURCE_DATE_EPOCH are untouched.
TEST_UNPINNED_SUFFIX="-unpinned-test"
UBUNTU_UNPINNED_SNAPSHOT="unpinned"
# mkosi's default Ubuntu x86_64 archive mirror. The manifest records it; the
# security suite keeps mkosi's normal security.ubuntu.com endpoint.
UBUNTU_UNPINNED_MIRROR="http://archive.ubuntu.com/ubuntu"
case "$IMAGE_VERSION" in
*"$TEST_UNPINNED_SUFFIX") IMAGE_TEST_VERSION=$IMAGE_VERSION ;;
*) IMAGE_TEST_VERSION="$IMAGE_VERSION$TEST_UNPINNED_SUFFIX" ;;
esac

# Ubuntu release. Packages come from the pinned snapshot below, so a rebuild
# sees the same repository contents; packages.json records exact versions.
UBUNTU_VERSION="24.04"
UBUNTU_CODENAME="noble"
UBUNTU_ARCH="amd64"
# Ubuntu archive snapshot (snapshot.ubuntu.com); noble's apt detects it.
# Pinned in mkosi.conf as well; build.sh asserts the two agree.
UBUNTU_SNAPSHOT="20261108T182718Z"

# Base ext4 filesystem size. Must be at least nodetest's default 10 GiB disk;
# the node may grow a per-VM overlay beyond this and the guest grows the
# filesystem on boot (bbraft-grow-root.service).
IMAGE_DISK_GIB="${BBRAFT_IMAGE_DISK_GIB:-10}"

# mkosi, run from its source tree through bin/mkosi. Not installed on the
# host and never taken from apt. Upstream: https://github.com/systemd/mkosi
MKOSI_VERSION="27.1"
MKOSI_URL="https://github.com/systemd/mkosi/archive/refs/tags/v${MKOSI_VERSION}.tar.gz"
MKOSI_SHA256="33520d662e151d6c43e910c3840ac971fd6023e4488d9c10599e199a07f77d8b"
MKOSI_SOURCE_DIR="mkosi-${MKOSI_VERSION}"

# Go toolchain inside the image. Version tracks go.mod (go 1.27.3).
# Upstream: https://go.dev/dl/
GO_VERSION="1.27.3"
GO_URL="https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
GO_SHA256="65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40"

# Node.js inside the image. Upstream checksums:
# https://nodejs.org/dist/v24.22.2/SHASUMS256.txt
NODE_VERSION="24.22.2"
NODE_URL="https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz"
NODE_SHA256="48cc0e21e048790e54c0cf5749c2725005313d652651c40b7a21feb9697aaf75"

# Default SOURCE_DATE_EPOCH for deterministic file timestamps (2025-01-01Z).
# The ext4 superblock and qcow2 metadata still carry build time, so this is
# not a claim of bit-for-bit reproducibility.
SOURCE_DATE_EPOCH_DEFAULT="1735689600"

# Output and cache locations, relative to images/ unless overridden.
IMAGE_OUT="${BBRAFT_IMAGE_OUT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/out}"
IMAGE_CACHE="${BBRAFT_IMAGE_CACHE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.cache}"

# The test-only unpinned mode has its own disposable output and cache roots,
# isolated by mode rather than only by image id so apt/mkosi entries from an
# unpinned build can never steer a pinned build (or the reverse).
IMAGE_TEST_OUT="${BBRAFT_IMAGE_TEST_OUT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/out-unpinned-test}"
IMAGE_TEST_CACHE="${BBRAFT_IMAGE_TEST_CACHE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.cache-unpinned-test}"
IMAGE_TEST_DIR="$IMAGE_TEST_OUT/$IMAGE_TEST_VERSION"

# Canonical mode-path isolation shared by build.sh, stage_test.sh and the
# shell regressions. readlink -m resolves existing symlink components before
# comparison. A trailing slash (and the root itself) is handled explicitly:
# "/" is an ancestor of every absolute path.
canonical_path() {
	readlink -m -- "$1"
}

paths_overlap() { # <a> <b>: equal or one contains the other
	local a b
	a=${1%/}
	b=${2%/}
	[ -z "$a" ] && a=/
	[ -z "$b" ] && b=/
	[ "$a" = "$b" ] && return 0
	[ "$a" = / ] && return 0
	[ "$b" = / ] && return 0
	case "$a/" in "$b/"*) return 0 ;; esac
	case "$b/" in "$a/"*) return 0 ;; esac
	return 1
}
