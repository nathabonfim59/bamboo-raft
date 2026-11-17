# Guest image build and smoke entry points. Heavy work runs on a KVM host;
# this Makefile is deliberately separate from the repository root Makefile.
SHELL := /bin/bash
GO ?= go
OUT ?= out
MKOSI_BIN ?= $(CURDIR)/.cache/mkosi-27.1/bin/mkosi
# Short and on the large disk: QEMU QMP unix sockets must fit in
# sockaddr_un.sun_path (107 bytes) even after t.TempDir's per-test suffix and
# the VM work directory, so keep this well under the old $HOME/bbraft-kvm-review.
KVM_TMP ?= $(HOME)/.bbk

# Smoke entry point. Overridable for focused argv tests; the default is the
# real harness. The test-only image path is sourced from pins.sh inside the
# recipe instead of being expanded by Make, so a path containing quotes or
# shell-looking text stays data.
SMOKE ?= ./smoke.sh

# Exact KVM test selections. kvm_gate.sh verifies every required top-level
# test name exists (exact line, go exit status checked) BEFORE the expensive
# image build, and the runs themselves are verbose so the review log shows
# exactly which subtests executed. A renamed or not-yet-published test fails
# the gate instead of silently matching nothing.
KVM_A2_GATE_TESTS := TestKVMConformanceDirect TestKVMConformanceLink TestKVMOwnershipAndDiskGrowth TestKVMLinkReconnect
KVM_FULL_GATE_TESTS := TestKVMConformanceDirect TestKVMConformanceLink TestKVMOwnershipAndDiskGrowth TestKVMLinkReconnect TestKVMSnapshotIntegrityAndGrowth TestKVMGuestAgentCrashDuringFreeze TestKVMNodeManagerRestartDuringFreeze
KVM_A2_CONFORMANCE_RUN := TestKVMConformance(Direct|Link)/(Info|Lifecycle|UnknownSandbox|DeleteFences.*|Exec.*|PutFile.*)$$
KVM_A2_FOCUS_RUN := TestKVM(OwnershipAndDiskGrowth|LinkReconnect)$$
# Full conformance including Snapshot, without re-running the A2 subset.
KVM_FULL_CONFORMANCE_RUN := TestKVMConformance(Direct|Link)$$
# Focused real-KVM regressions, including the A3 restart-while-frozen case.
KVM_FULL_FOCUS_RUN := TestKVM(OwnershipAndDiskGrowth|LinkReconnect|SnapshotIntegrityAndGrowth|GuestAgentCrashDuringFreeze|NodeManagerRestartDuringFreeze)$$
# The role review wrapper from the bbraft_node role (A3.2); it receives the
# absolute path of a freshly built bbraft-node binary.
ROLE_TESTS := $(CURDIR)/../infra/ansible/roles/bbraft_node/tests/controller.sh
KVM_TEST_ENV := BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR="$(CURDIR)/out" TMPDIR="$(KVM_TMP)" GOTMPDIR="$(KVM_TMP)"

.PHONY: all image smoke image-unpinned-test smoke-unpinned-test check check-manifests mkosi-config-check cpu-probe review review-prereqs review-node review-all clean help

all: check image

# Shared review prerequisites, run exactly once per review: repository checks,
# image-local checks, and the cheap CPU-only QEMU probe. Later stages must not
# repeat them.
review-prereqs:
	$(MAKE) -C .. check
	$(MAKE) check
	./cpu_probe.sh

# One command for an A1 VM review: prerequisites (root checks, image
# checks, CPU probe), then the mkosi build and the unprivileged smoke boot.
# Make stops at the first failure; nothing is installed or deployed.
review: review-prereqs
	./build.sh
	./smoke.sh

# A2 VM review: prerequisites, the KVM presence gate (before the expensive
# build), one image build and smoke boot, then the non-snapshot conformance
# subset and focused regressions, verbose. Uses a short temp dir on a large
# disk for both TMPDIR and GOTMPDIR (Unix socket limits, avoid tmpfs).
# Override KVM_TMP=/path if needed. review-node stays the A2 gate; it never
# runs the role tests.
review-node: review-prereqs
	./kvm_gate.sh ./internal/node $(KVM_A2_GATE_TESTS)
	./build.sh
	./smoke.sh
	@mkdir -p "$(KVM_TMP)"
	$(KVM_TEST_ENV) $(GO) -C .. test -race -v ./internal/node \
		-run '$(KVM_A2_CONFORMANCE_RUN)' -timeout 30m
	$(KVM_TEST_ENV) $(GO) -C .. test -race -v ./internal/node \
		-run '$(KVM_A2_FOCUS_RUN)' -timeout 10m

# Full A3 VM review: prerequisites and the KVM presence gate once, then the
# node binary and the mandatory ansible role gate (all before the expensive
# image build), then one image build and smoke boot, then the full real-KVM
# conformance including Snapshot plus the focused regressions, verbose.
# Exactly one root check and one image build per invocation.
review-all: review-prereqs
	./kvm_gate.sh ./internal/node $(KVM_FULL_GATE_TESTS)
	$(GO) -C .. build -trimpath -o "$(CURDIR)/.cache/bbraft-node" ./cmd/bbraft-node
	"$(ROLE_TESTS)" "$(CURDIR)/.cache/bbraft-node"
	./build.sh
	./smoke.sh
	@mkdir -p "$(KVM_TMP)"
	$(KVM_TEST_ENV) $(GO) -C .. test -race -v ./internal/node \
		-run '$(KVM_FULL_CONFORMANCE_RUN)' -timeout 60m
	$(KVM_TEST_ENV) $(GO) -C .. test -race -v ./internal/node \
		-run '$(KVM_FULL_FOCUS_RUN)' -timeout 30m

# Build the pinned Ubuntu 24.04 image into images/out/<version>/.
image:
	./build.sh

# Boot the built image with unprivileged QEMU and exercise the guest agent.
smoke:
	$(SMOKE)

# TEST-ONLY unpinned archive build. VM-only, never a release path: it builds
# into images/out-unpinned-test with the -unpinned-test id and an isolated
# images/.cache-unpinned-test, and images/publish.sh refuses the result.
image-unpinned-test:
	BBRAFT_IMAGE_TEST_UNPINNED=1 ./build.sh

# Boot the disposable test image explicitly; the default smoke selection is
# always the pinned images/out image. pins.sh is sourced here so the path is
# a shell variable (data), never Make-time interpolation into recipe text.
smoke-unpinned-test:
	@set -e; . ./pins.sh; exec "$(SMOKE)" --image-dir "$$IMAGE_TEST_DIR"

# Cheap early CPU gate for VM reviews: unprivileged paused QEMU, enforced
# fixed model, QMP-proven readiness, no image and no boot.
cpu-probe:
	./cpu_probe.sh

# Verify every built image directory under OUT. Missing/empty/non-image dirs
# are skipped successfully; the first manifest verification error stops the
# loop (a later good directory must not mask it).
check-manifests:
	@[ -n "$(OUT)" ] || { echo "check-manifests: OUT is empty" >&2; exit 2; }; \
	shopt -s nullglob; found=0; \
	for d in "$(OUT)"/*/; do \
		[ -f "$$d/manifest.json" ] || continue; \
		found=1; \
		./verify.sh "$$d" || exit 1; \
	done; \
	if [ "$$found" -eq 0 ]; then echo "check-manifests: no built images in $(OUT)"; fi

# Run mkosi's own workspace/source relationship check (not a reimplemented
# predicate) on the exact build argv. Skips only when the pinned mkosi source
# is not cached; build.sh runs it after downloading mkosi, before mkosi build.
mkosi-config-check:
	@if [ -x "$(MKOSI_BIN)" ]; then \
		. ./pins.sh; \
		./mkosi_config_check.py --mkosi "$(MKOSI_BIN)" \
			--expect-snapshot "$$UBUNTU_SNAPSHOT" --expect-mirror none -- \
			--directory "$(CURDIR)" \
			--output-dir "$(CURDIR)/.cache/build-$$IMAGE_VERSION" \
			--cache-dir "$(CURDIR)/.cache/mkosi-cache" \
			--package-cache-dir "$(CURDIR)/.cache/apt" \
			--workspace-dir "$(CURDIR)/.cache/workspace" \
			--snapshot "$$UBUNTU_SNAPSHOT" \
			build && \
		./mkosi_config_check.py --mkosi "$(MKOSI_BIN)" \
			--expect-snapshot none --expect-mirror "$$UBUNTU_UNPINNED_MIRROR" -- \
			--directory "$(CURDIR)" \
			--output-dir "$(CURDIR)/.cache-unpinned-test/build-$$IMAGE_TEST_VERSION" \
			--cache-dir "$(CURDIR)/.cache-unpinned-test/mkosi-cache" \
			--package-cache-dir "$(CURDIR)/.cache-unpinned-test/apt" \
			--workspace-dir "$(CURDIR)/.cache-unpinned-test/workspace" \
			--snapshot "" --mirror "$$UBUNTU_UNPINNED_MIRROR" \
			build; \
	else echo "skip mkosi-config-check: pinned mkosi not cached at $(MKOSI_BIN)"; fi

# Fast local checks only: shell syntax (one bash -n per file; a single
# bash -n with many names would only parse the first), publish/pin/manifest
# regressions, the shared CPU resolver/probe with fake QMP, the KVM presence
# gate with a fake go, SSH
# host-key/growth/smoke-cleanup/image-v1 regressions,
# Go vet/tests for the smoke client when the guest stubs are present, and
# verification of already built images via check-manifests.
check: mkosi-config-check check-manifests
	@for f in build.sh smoke.sh cpu_probe.sh cpu_probe_test.sh kvm_gate.sh kvm_gate_test.sh verify.sh publish.sh publish_test.sh stage_test.sh test_mode_test.sh pins.sh pins_test.sh check_manifests_test.sh toolchain_requirements_test.sh cpu_flag_test.sh build_preflight_test.sh go_preflight_test.sh check_kernel_modules.sh check_kernel_modules_test.sh dpkg_version.sh dpkg_version_test.sh ssh_host_key_test.sh grow_root_test.sh image_v1_test.sh smoke_cleanup_test.sh net_config_test.sh mkosi.extra/usr/local/sbin/bbraft-ssh-host-key mkosi.extra/usr/local/sbin/bbraft-grow-root mkosi.extra/usr/local/sbin/bbraft-network-config mkosi.extra/usr/local/bin/bbraft-toolchain-check mkosi.postinst.chroot; do \
		bash -n "$$f" || exit 1; \
	done
	python3 -c 'compile(open("mkosi_config_check.py").read(), "mkosi_config_check.py", "exec")'
	python3 -c 'compile(open("fake_qmp.py").read(), "fake_qmp.py", "exec")'
	./publish_test.sh
	./pins_test.sh
	./check_manifests_test.sh
	./toolchain_requirements_test.sh
	./cpu_flag_test.sh
	./cpu_probe_test.sh
	./kvm_gate_test.sh
	./build_preflight_test.sh
	./go_preflight_test.sh
	./check_kernel_modules_test.sh
	./dpkg_version_test.sh
	./ssh_host_key_test.sh
	./grow_root_test.sh
	./image_v1_test.sh
	./smoke_cleanup_test.sh
	./net_config_test.sh
	./test_mode_test.sh
	@if [ -f smoke/main.go ] && [ -d ../internal/proto/guestv1 ]; then \
		$(GO) vet . ./smoke && $(GO) test . ./smoke; \
		else echo "skip smoke vet/test: internal/proto/guestv1 is not in this worktree yet"; fi

clean:
	rm -rf out .cache out-unpinned-test .cache-unpinned-test

help:
	@sed -n '1,10p' Makefile
