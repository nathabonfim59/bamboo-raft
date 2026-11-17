#!/usr/bin/env bash
# Regression for the manifest-verification loop in images/Makefile.
#
# The old recipe failed on an empty out/ directory and, worse, masked an early
# verify failure if a later directory verified. This test embeds that old
# recipe, proves both failures with fixtures, then proves check-manifests fixes
# them. No downloads, image build or QEMU are involved.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-check-manifests.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

# verify.sh needs qemu-img only for the good fixture; a tiny stub keeps this
# test independent of qemu-utils.
mkdir -p "$tmp/bin"
cat >"$tmp/bin/qemu-img" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
info) printf '{"virtual-size": 16777216, "format": "qcow2"}\n' ;;
check) printf 'No errors were found on the image.\n' ;;
*) exit 1 ;;
esac
EOF
chmod 0755 "$tmp/bin/qemu-img"
mkdir -p "$tmp/bin-raw"
cat >"$tmp/bin-raw/qemu-img" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
info) printf '{"virtual-size": 16777216, "format": "raw"}\n' ;;
check) printf 'No errors were found on the image.\n' ;;
*) exit 1 ;;
esac
EOF
chmod 0755 "$tmp/bin-raw/qemu-img"
export PATH="$tmp/bin:$PATH"

make_fixture() { # <dir>
	local dir=$1 id
	mkdir -p "$dir"
	id=$(basename "$dir")
	printf 'kernel\n' >"$dir/kernel"
	printf 'initrd\n' >"$dir/initrd"
	printf 'qcow2\n' >"$dir/root.qcow2"
	printf '{}\n' >"$dir/packages.json"
	jq -n \
		--arg id "$id" \
		--arg sha_kernel "$(sha256sum "$dir/kernel" | cut -d' ' -f1)" \
		--argjson size_kernel "$(stat -c %s "$dir/kernel")" \
		--arg sha_initrd "$(sha256sum "$dir/initrd" | cut -d' ' -f1)" \
		--argjson size_initrd "$(stat -c %s "$dir/initrd")" \
		--arg sha_root "$(sha256sum "$dir/root.qcow2" | cut -d' ' -f1)" \
		--argjson size_root "$(stat -c %s "$dir/root.qcow2")" \
		--arg sha_packages "$(sha256sum "$dir/packages.json" | cut -d' ' -f1)" \
		--argjson size_packages "$(stat -c %s "$dir/packages.json")" \
		'{schemaVersion:1,id:$id,root:{virtualSizeBytes:16777216},artifacts:{
			kernel:{sha256:$sha_kernel,size:$size_kernel},
			initrd:{sha256:$sha_initrd,size:$size_initrd},
			"root.qcow2":{sha256:$sha_root,size:$size_root},
			"packages.json":{sha256:$sha_packages,size:$size_packages}}}' >"$dir/manifest.json"
}

# Exact pre-fix recipe, including its missing `|| exit`.
old_recipe() { # <out-dir>
	local out=$1
	if [ -d "$out" ]; then
		for d in "$out"/*/; do
			[ -f "$d/manifest.json" ] && ./verify.sh "$d"
		done
	fi
}

run_old() { # <out-dir>; returns old recipe status
	( cd "$script_dir" && old_recipe "$1" )
}

run_new() { # <out-dir>; returns check-manifests status
	make -C "$script_dir" --no-print-directory check-manifests OUT="$1" >"$tmp/make.log" 2>&1
}

expect_status() { # <want> <got> <description>
	if [ "$1" -eq "$2" ]; then
		pass "$3"
	else
		fail "$3 (status $2, want $1)"
	fi
}

# 1. Empty out directory.
mkdir -p "$tmp/empty"
if run_old "$tmp/empty"; then old_status=0; else old_status=$?; fi
expect_status 1 "$old_status" "old recipe fails on empty out (pre-fix reproduction)"
if run_new "$tmp/empty"; then new_status=0; else new_status=$?; fi
expect_status 0 "$new_status" "check-manifests skips empty out"

# 2. Out directory with only non-image subdirectories.
mkdir -p "$tmp/nonimage/notes"
if run_old "$tmp/nonimage"; then old_status=0; else old_status=$?; fi
expect_status 1 "$old_status" "old recipe fails on a non-image dir"
if run_new "$tmp/nonimage"; then new_status=0; else new_status=$?; fi
expect_status 0 "$new_status" "check-manifests skips non-image dirs"

# 2b. Out directory does not exist at all.
if run_new "$tmp/absent"; then new_status=0; else new_status=$?; fi
expect_status 0 "$new_status" "check-manifests skips a missing out dir"

# 3. Bad directory sorts before a good one: old recipe exits 0 (masked).
mkdir -p "$tmp/mixed/a-bad"
printf '{}\n' >"$tmp/mixed/a-bad/manifest.json"
make_fixture "$tmp/mixed/z-good"
if run_old "$tmp/mixed"; then old_status=0; else old_status=$?; fi
expect_status 0 "$old_status" "old recipe masks a-bad failure because z-good verifies"
if run_new "$tmp/mixed"; then new_status=0; else new_status=$?; fi
if [ "$new_status" -ne 0 ]; then
	pass "check-manifests fails immediately on a-bad"
else
	fail "check-manifests masked a-bad because z-good verifies"
fi
if grep -q 'schemaVersion' "$tmp/make.log"; then
	pass "check-manifests failure names the bad manifest"
else
	fail "check-manifests failure does not name the bad manifest"
fi

# 4. The same bad+good ordering under an output path containing a space: the
# quoted glob must still find and reject a-bad.
space="$tmp/space out"
mkdir -p "$space/a-bad"
printf '{}\n' >"$space/a-bad/manifest.json"
make_fixture "$space/z-good"
if run_new "$space"; then
	fail "check-manifests masked a-bad under a space-containing OUT"
else
	pass "check-manifests rejects a-bad under a space-containing OUT"
fi
if grep -q 'schemaVersion' "$tmp/make.log"; then
	pass "space-containing OUT failure names the bad manifest"
else
	fail "space-containing OUT failure does not name the bad manifest"
fi

# 5. verify.sh requires root.qcow2 to actually report format=qcow2.
make_fixture "$tmp/bad-format"
if PATH="$tmp/bin-raw:$PATH" "$script_dir/verify.sh" "$tmp/bad-format" >"$tmp/format.log" 2>&1; then
	fail "verify accepted a non-qcow2 image"
else
	pass "verify rejects a non-qcow2 image"
fi
if grep -q 'format is' "$tmp/format.log"; then
	pass "non-qcow2 failure names the format"
else
	fail "non-qcow2 failure does not name the format"
fi

if [ "$failures" -ne 0 ]; then
	printf 'check_manifests_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'check_manifests_test: all checks passed\n'
