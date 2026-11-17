#!/usr/bin/env bash
# Read a package version from a target dpkg status database without executing
# anything inside the image (build-time chroot may not have /proc).
#
#   dpkg_version.sh <status-file> <package>
#
# Prints the Version of exactly one "install ok installed" stanza. Fails when
# the package is absent, only removed/config-files, or has multiple installed
# stanzas (ambiguous).
set -euo pipefail

die() {
	printf 'dpkg_version: %s\n' "$*" >&2
	exit 1
}

[ $# -eq 2 ] || die "usage: dpkg_version.sh <status-file> <package>"
status_file=$1
package=$2
[ -r "$status_file" ] || die "cannot read dpkg status file $status_file"

version=$(awk -v package="$package" '
	BEGIN { RS = ""; FS = "\n"; installed = 0; found = "" }
	{
		name = ""; version = ""; status = ""
		for (i = 1; i <= NF; i++) {
			if ($i ~ /^Package:[ \t]/) name = substr($i, 10)
			else if ($i ~ /^Version:[ \t]/) version = substr($i, 10)
			else if ($i ~ /^Status:[ \t]/) status = substr($i, 9)
		}
		if (name == package && status == "install ok installed") {
			installed++
			found = version
		}
	}
	END {
		if (installed == 0) exit 1
		if (installed > 1) exit 2
		if (found == "") exit 3
		print found
	}
' "$status_file") || {
	rc=$?
	case "$rc" in
	1) die "package $package is not installed in $status_file" ;;
	2) die "package $package has multiple installed stanzas in $status_file" ;;
	3) die "package $package has an empty Version in $status_file" ;;
	*) die "cannot parse $status_file (awk exit $rc)" ;;
	esac
}
printf '%s\n' "$version"
