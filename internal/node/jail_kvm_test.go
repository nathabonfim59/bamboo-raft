package node

// A10 final jailed acceptance (lead-owned gate; root throwaway VM only).
//
// The flows below are the acceptance contract; a normal run skips
// everything here.
//
// Link runs the BUILT production bbraft-node main from the role-rendered,
// attempt-unique unit, installed as a real runtime unit (systemctl
// start/stop/restart, the real delegated <unit>/{supervisor,vms} cgroup). It
// proves the whole production startup and dials the test Hub over nodelink.
// Direct runs a test-only helper (TestKVMJailDirectHelper) under the SAME
// unit file with only ExecStart and Environment replaced by the drop-in: a
// real jailed Manager on a unix socket. Direct cannot run the cmd-only
// startup proofs (root supervisor, delegation, accounts, IO device); Link is
// the production-startup proof.
//
// The one test substitution is NetworkNamespacePath=/run/netns/<nodeNS> in
// the attempt's drop-in (production runs in the host netns). Everything else
// in [Service] is the rendered unit, asserted live with systemctl show.
//
// Prerequisites, all prepared by the lead as the normal VM user before the
// root run (attempt-unique units come from the role's own render step):
//
//	go build -o "$dir/bbraft-node" ./cmd/bbraft-node
//	go test -c -o "$dir/node.test" ./internal/node
//	# render one unit per attempt (link, direct, restart, snapshot) through
//	# the role variables; writes $units/manifest.json (see jailKVMManifest)
//	sudo env BBRAFT_TEST_KVM_JAIL=1 BBRAFT_TEST_KVM=1 BBRAFT_TEST_REQUIRE_ALL=1 \
//	  BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  BBRAFT_TEST_KVM_JAIL_UNITS=$units BBRAFT_TEST_KVM_JAIL_BIN=$dir \
//	  "$dir/node.test" -test.run '^TestKVMJail' -test.v -test.timeout 90m
//
// Ownership: every unit, drop-in, config/data/stage dir, netns and cgroup
// is attempt-unique; the harness refuses to start if any object of that
// identity exists or is loaded, captures each object's identity right after
// creating it, and removes only still-matching objects after the unit is
// proven stopped and its whole cgroup subtree proven empty
// (jailKVMCleanupPlan). Any uncertainty preserves everything and fails.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	jailKVMGateEnv   = "BBRAFT_TEST_KVM_JAIL"
	jailKVMUnitsEnv  = "BBRAFT_TEST_KVM_JAIL_UNITS"
	jailKVMBinEnv    = "BBRAFT_TEST_KVM_JAIL_BIN"
	jailKVMHelperEnv = "BBRAFT_TEST_KVM_JAIL_HELPER"
)

// jailKVMManifest is $BBRAFT_TEST_KVM_JAIL_UNITS/manifest.json, written by
// the render step: one entry per attempt (link, direct, restart, snapshot),
// each with its attempt id, the rendered unit file and the names the role
// rendered from it (service, config dir, data dir, image dir, stage dir).
type jailKVMManifest struct {
	Attempts map[string]jailKVMRendered `json:"attempts"`
}

// jailKVMRendered is one attempt's rendered identity.
type jailKVMRendered struct {
	Attempt      string `json:"attempt"`
	Unit         string `json:"unit"`
	Service      string `json:"service"`
	NodeID       string `json:"nodeId"`
	ConfigDir    string `json:"configDir"`
	DataDir      string `json:"dataDir"`
	ImageDir     string `json:"imageDir"`
	StageDir     string `json:"stageDir"`
	BinaryPath   string `json:"binaryPath"`
	BinarySHA256 string `json:"binarySha256"`
	UnitPath     string `json:"unitPath"`
}

// jailKVMEnv is the validated gate environment.
type jailKVMEnv struct {
	manifest jailKVMManifest
	binDir   string
	imageDir string
	imageID  string
}

// restart runs systemctl restart (KillMode=process: only the node main is
// stopped; jailed QEMUs keep running for the next main to adopt) and returns
// the new InvocationID, which must differ from the previous one, with the
// unit active and a main pid.
func (a *jailKVMAttempt) restart(t *testing.T) string {
	t.Helper()
	svc := a.rendered.Service
	old := jailKVMShow(t, svc, "InvocationID")["InvocationID"]
	jailKVMRun(t, 2*time.Minute, "systemctl", "restart", svc+".service")
	show := jailKVMShow(t, svc, "InvocationID", "ActiveState", "MainPID")
	if inv := show["InvocationID"]; old == "" || inv == "" || inv == old || show["ActiveState"] != "active" || show["MainPID"] == "0" {
		t.Fatalf("restart of %s: %v (previous invocation %q)\n%s", svc, show, old, a.journalTail(inv))
	}
	return show["InvocationID"]
}

// directSocket is the Direct helper's unix socket in the data dir.
func (a *jailKVMAttempt) directSocket() string { return a.rendered.DataDir + "/direct.sock" }

func jailKVMCreate(t *testing.T, c nodev1.NodeClient, id string, diskGiB uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	if _, err := c.CreateVM(ctx, &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: diskGiB}}); err != nil {
		t.Fatalf("CreateVM %s: %v", id, err)
	}
}

func jailKVMExec(t *testing.T, c nodev1.NodeClient, id, command string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	res, err := nodetest.Exec(ctx, c, &nodev1.ExecRequest{SandboxId: id, Command: command})
	if err != nil || res.Exit.GetExitCode() != 0 {
		t.Fatalf("Exec(%s, %q) = %+v, %v", id, command, res, err)
	}
	return res.Stdout
}

func jailKVMRootfsBytes(t *testing.T, c nodev1.NodeClient, id string) uint64 {
	t.Helper()
	out := jailKVMExec(t, c, id, `df -B1 / | awk 'NR==2 {print $2}'`)
	n, err := strconv.ParseUint(strings.TrimSpace(out), 10, 64)
	if err != nil {
		t.Fatalf("df of %s: %q: %v", id, out, err)
	}
	return n
}

// TestKVMJailAcceptance runs Phase 1 conformance, SSH and the jail
// inspection over Link (production main) and Direct (test helper).
func TestKVMJailAcceptance(t *testing.T) {
	env := jailKVMRequire(t)
	for _, tc := range []struct {
		name   string
		direct bool
	}{
		{name: "Link"},
		{name: "Direct", direct: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tp := kvmNetNewTopology(t, "j"+strings.ToLower(tc.name[:1]))
			tp.createNamespaces(t, true)
			a := env.attempt(t, strings.ToLower(tc.name), tp)
			a.stage(t)
			var client nodev1.NodeClient
			if tc.direct {
				a.install(t, a.directExec(t))
				inv := a.start(t)
				a.assertBoundary(t, inv)
				// The helper logs the same startup phases (including the
				// local-CID exclusion from node.New) and is ready only after
				// its socket listens and the tool self-check passed.
				a.assertStartupLog(t, inv)
				client = a.dialDirect(t)
			} else {
				hub := a.startHub(t)
				a.install(t, nil)
				inv := a.start(t)
				a.assertBoundary(t, inv)
				a.assertStartupLog(t, inv)
				client = a.waitLink(t, hub).Client
			}
			tp.client = client
			t.Run("AllPhase1", func(t *testing.T) {
				nodetest.Run(t, client, nodetest.Options{
					Resources:   &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
					BootTimeout: 3 * time.Minute,
					FenceWatch:  3 * time.Minute,
					Phase1:      nodetest.AllPhase1,
				})
			})
			tp.sshAcceptance(t, client, a.rendered.DataDir+"/net")
			t.Run("JailInspection", func(t *testing.T) {
				const ida, idb = "bx_aaaaaaaa", "bx_bbbbbbbb"
				for _, id := range []string{ida, idb} {
					jailKVMCreate(t, client, id, 10)
					// Registered right after a successful create: a Fatal
					// anywhere below still deletes through the API before
					// the parent's no-resources assertion (DeleteVM is
					// idempotent, so the explicit deletes below stay).
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
						defer cancel()
						if _, err := client.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
							t.Errorf("inspection cleanup DeleteVM %s: %v", id, err)
						}
					})
				}
				qa, qb := a.qemu(t, ida), a.qemu(t, idb)
				a.assertJailed(t, qa)
				a.assertJailed(t, qb)
				if qa.UID == qb.UID {
					t.Fatalf("two VMs share uid %d", qa.UID)
				}
				a.probe(t, jailKVMCrossUIDCases(qa, qb))
				for _, id := range []string{ida, idb} {
					if _, err := client.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
						t.Fatalf("DeleteVM %s: %v", id, err)
					}
				}
			})
			tp.assertNoSandboxResources(t, a.rendered.DataDir+"/net")
		})
	}
}

// TestKVMJailRestart restarts the production unit with a running and a
// STOPPED VM: the running QEMU is adopted unchanged (pid, start ticks, uid,
// cgroup), its guest process and file are reached through the NEW link,
// and the STOPPED VM stays stopped with its endpoint denied until a resume
// with a rotated key.
func TestKVMJailRestart(t *testing.T) {
	env := jailKVMRequire(t)
	tp := kvmNetNewTopology(t, "jr")
	tp.createNamespaces(t, true)
	a := env.attempt(t, "restart", tp)
	a.stage(t)
	hub := a.startHub(t)
	a.install(t, nil)
	inv1 := a.start(t)
	a.assertStartupLog(t, inv1)
	link1 := a.waitLink(t, hub)
	c1 := link1.Client
	const running, stopped = "bx_rrrrrrrr", "bx_ssssssss"
	jailKVMCreate(t, c1, running, 10)
	jailKVMCreate(t, c1, stopped, 10)
	jailKVMExec(t, c1, running, "echo persisted > restart-file && sync")
	proc, err := c1.StartProcess(t.Context(), &nodev1.StartProcessRequest{SandboxId: running, ProcessId: 1, Command: "sleep 3600"})
	if err != nil || proc.GetState() != nodev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Fatalf("StartProcess = %+v, %v", proc, err)
	}
	stoppedSSH := kvmNetAddSSHKeyFresh(t, tp, c1, stopped)
	if _, err := c1.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: stopped}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	tp.waitReachable(t, fmt.Sprintf("%s:%d", stoppedSSH.GetHost(), stoppedSSH.GetPort()), false, time.Minute)
	before := a.qemu(t, running)

	inv2 := a.restart(t)
	a.assertStartupLog(t, inv2)
	link2 := a.waitLink(t, hub)
	if err := jailKVMCheckNewLink(link1.Done, inv1, inv2); err != nil {
		t.Fatal(err)
	}
	c2 := link2.Client
	if err := jailKVMCheckAdopted(before, a.qemu(t, running)); err != nil {
		t.Fatal(err)
	}
	if out, err := c2.GetProcess(t.Context(), &nodev1.GetProcessRequest{SandboxId: running, ProcessId: 1}); err != nil ||
		out.GetProcess().GetState() != nodev1.ProcessState_PROCESS_STATE_RUNNING || out.GetProcess().GetPid() != proc.GetPid() {
		t.Fatalf("guest process after restart = %+v, %v (want the same running pid %d)", out, err, proc.GetPid())
	}
	if _, data, err := nodetest.ReadFile(t.Context(), c2, &nodev1.ReadFileRequest{SandboxId: running, Path: "restart-file"}); err != nil || string(data) != "persisted\n" {
		t.Fatalf("persisted file after restart = %q, %v", data, err)
	}
	if vm, err := c2.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: stopped}); err != nil || vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("STOPPED VM after restart = %+v, %v", vm, err)
	}
	tp.waitReachable(t, fmt.Sprintf("%s:%d", stoppedSSH.GetHost(), stoppedSSH.GetPort()), false, 10*time.Second)
	if _, err := c2.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: stopped}); err != nil {
		t.Fatalf("StartVM after restart: %v", err)
	}
	rotated := kvmNetAddSSHKeyFresh(t, tp, c2, stopped)
	if err := jailKVMCheckRotatedHostKey(stoppedSSH, rotated); err != nil {
		t.Fatal(err)
	}
	tp.waitReachable(t, fmt.Sprintf("%s:%d", rotated.GetHost(), rotated.GetPort()), true, 2*time.Minute)
	for _, id := range []string{running, stopped} {
		if _, err := c2.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
			t.Fatalf("DeleteVM %s: %v", id, err)
		}
	}
}

// TestKVMJailSnapshotGrowth repeats TestKVMSnapshotIntegrityAndGrowth's
// point-in-time, growth and independence checks over the production main
// through nodev1 only (no root qemu-img, no process kill), and adds STOPPED
// disk growth.
func TestKVMJailSnapshotGrowth(t *testing.T) {
	env := jailKVMRequire(t)
	tp := kvmNetNewTopology(t, "js")
	tp.createNamespaces(t, true)
	a := env.attempt(t, "snapshot", tp)
	a.stage(t)
	hub := a.startHub(t)
	a.install(t, nil)
	a.assertStartupLog(t, a.start(t))
	c := a.waitLink(t, hub).Client
	const source, child, snapshotID = "bx_22222222", "bx_33333333", "kvm-jail-integrity"
	jailKVMCreate(t, c, source, 10)
	jailKVMExec(t, c, source, "mkdir -p snap && echo before-snapshot > snap/state && sync")
	if _, err := c.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{SandboxId: source, SnapshotId: snapshotID}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	jailKVMExec(t, c, source, "echo after-snapshot > snap/state && sync")
	if _, err := c.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: child, FromSnapshot: snapshotID,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 12}}); err != nil {
		t.Fatalf("larger snapshot child: %v", err)
	}
	if got := jailKVMExec(t, c, child, "cat snap/state"); got != "before-snapshot\n" {
		t.Fatalf("child point-in-time data = %q", got)
	}
	if got := jailKVMExec(t, c, source, "cat snap/state"); got != "after-snapshot\n" {
		t.Fatalf("source data = %q", got)
	}
	if size := jailKVMRootfsBytes(t, c, child); size < uint64(12)<<30*95/100 {
		t.Fatalf("grown child filesystem bytes = %d", size)
	}
	// Deleting the source and the saved snapshot cannot affect the child.
	if _, err := c.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: source}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteSnapshot(t.Context(), &nodev1.DeleteSnapshotRequest{SnapshotId: snapshotID}); err != nil {
		t.Fatal(err)
	}
	if got := jailKVMExec(t, c, child, "cat snap/state"); got != "before-snapshot\n" {
		t.Fatalf("independent child data = %q", got)
	}
	// STOPPED disk growth: StartVM grows the kept disk and the guest root fs.
	if _, err := c.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: child}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if _, err := c.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: child, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 14}}); err != nil {
		t.Fatalf("StartVM with a larger disk: %v", err)
	}
	if size := jailKVMRootfsBytes(t, c, child); size < uint64(14)<<30*95/100 {
		t.Fatalf("STOPPED-grown filesystem bytes = %d", size)
	}
	if got := jailKVMExec(t, c, child, "cat snap/state"); got != "before-snapshot\n" {
		t.Fatalf("data after STOPPED growth = %q", got)
	}
	if _, err := c.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: child}); err != nil {
		t.Fatal(err)
	}
}
