# shellcheck shell=bash
# Go toolchain pinned by images/pins.sh; installed at /usr/local/go.
export PATH="/usr/local/go/bin:$PATH"
# Use the image's toolchain, never download another one at test time.
export GOTOOLCHAIN=local
