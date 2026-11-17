package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// jailKVMFakeMain scripts what a unit's main process looks like at each
// sample: steps[i] is used by the i-th Show (the last step repeats).
type jailKVMFakeMain struct {
	mu      sync.Mutex
	steps   []jailKVMFakeMainStep
	shows   int
	calls   []string
	onShow  func(n int) // optional: runs after the n-th Show
	current jailKVMFakeMainStep
}

type jailKVMFakeMainStep struct {
	inv    string
	pid    int
	exe    jailKVMIdentity
	link   string
	start  string
	status string
	exeErr error
}

const (
	jailKVMFakeExecutorStatus = "Uid:\t0\t0\t0\t0\nNoNewPrivs:\t0\nCapInh:\t0000000000000000\nCapPrm:\t000001ffffffffff\nCapEff:\t000001ffffffffff\nCapBnd:\t000001ffffffffff\nCapAmb:\t0000000000000000\n"
	jailKVMFakeNodeStatus     = "Uid:\t0\t0\t0\t0\nNoNewPrivs:\t1\nCapInh:\t0000000000000000\nCapPrm:\t00000000082c11eb\nCapEff:\t00000000082c11eb\nCapBnd:\t00000000082c11eb\nCapAmb:\t0000000000000000\n"
)

var (
	jailKVMFakeStaged   = jailKVMIdentity{Dev: 66306, Ino: 917001}
	jailKVMFakeExecutor = jailKVMIdentity{Dev: 66306, Ino: 12}
)

func jailKVMFakeExecutorStep() jailKVMFakeMainStep {
	return jailKVMFakeMainStep{inv: "inv1", pid: 4242, exe: jailKVMFakeExecutor, link: "/usr/lib/systemd/systemd-executor", start: "777", status: jailKVMFakeExecutorStatus}
}

func jailKVMFakeNodeStep() jailKVMFakeMainStep {
	return jailKVMFakeMainStep{inv: "inv1", pid: 4242, exe: jailKVMFakeStaged, link: "/var/lib/bbraft-kt-stage-a1/bin/bbraft-node", start: "777", status: jailKVMFakeNodeStatus}
}

func (f *jailKVMFakeMain) ops() jailKVMMainOps {
	return jailKVMMainOps{
		Show: func(context.Context) (string, int, error) {
			f.mu.Lock()
			f.current = f.steps[min(f.shows, len(f.steps)-1)]
			f.shows++
			n, s, hook := f.shows, f.current, f.onShow
			f.calls = append(f.calls, "show")
			f.mu.Unlock()
			if hook != nil {
				hook(n)
			}
			return s.inv, s.pid, nil
		},
		Exe: func(pid int) (jailKVMIdentity, string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.calls = append(f.calls, "exe")
			if pid != f.current.pid {
				return jailKVMIdentity{}, "", fmt.Errorf("pid %d: %w", pid, errors.New("no such process"))
			}
			return f.current.exe, f.current.link, f.current.exeErr
		},
		StartTime: func(pid int) (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.calls = append(f.calls, "start")
			return f.current.start, nil
		},
		Status: func(pid int) (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.calls = append(f.calls, "status:"+f.current.link)
			return f.current.status, nil
		},
	}
}

// TestJailKVMAwaitMainExe: the main process's status is asserted only once
// /proc/<MainPID>/exe is the staged bbraft-node inode, with the
// InvocationID, MainPID, exe identity and start time re-proven around the
// status read; the early systemd-executor sample is kept as evidence and
// never asserted; a never-matching exe times out.
func TestJailKVMAwaitMainExe(t *testing.T) {
	await := func(f *jailKVMFakeMain, timeout time.Duration) (jailKVMMainProof, error) {
		return jailKVMAwaitMainExe(t.Context(), "inv1", jailKVMFakeStaged, timeout, time.Millisecond, f.ops())
	}
	t.Run("executor then staged binary", func(t *testing.T) {
		f := &jailKVMFakeMain{steps: []jailKVMFakeMainStep{jailKVMFakeExecutorStep(), jailKVMFakeExecutorStep(), jailKVMFakeNodeStep()}}
		p, err := await(f, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if p.Initial.ExeLink != "/usr/lib/systemd/systemd-executor" || p.Initial.Exe != jailKVMFakeExecutor || p.Initial.Status["CapBnd"] != "000001ffffffffff" {
			t.Fatalf("initial sample %+v: want the executor evidenced with its full bounding set", p.Initial)
		}
		if p.Matched.Exe != jailKVMFakeStaged || p.Matched.PID != 4242 || p.Matched.Invocation != "inv1" || p.Matched.Status["CapBnd"] != "00000000082c11eb" || p.Matched.Status["NoNewPrivs"] != "1" {
			t.Fatalf("matched sample %+v", p.Matched)
		}
		if p.Samples < 3 {
			t.Fatalf("samples %d, want the executor samples counted", p.Samples)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		// The matched status read is bracketed by full samples: show, exe,
		// start, status, show, exe, start.
		last := strings.Join(f.calls[len(f.calls)-7:], " ")
		if last != "show exe start status:/var/lib/bbraft-kt-stage-a1/bin/bbraft-node show exe start" {
			t.Fatalf("calls %q: the status read must be re-proven on both sides", f.calls)
		}
	})
	t.Run("staged binary on the first sample", func(t *testing.T) {
		f := &jailKVMFakeMain{steps: []jailKVMFakeMainStep{jailKVMFakeNodeStep()}}
		p, err := await(f, 5*time.Second)
		if err != nil || p.Matched.Status["CapBnd"] != "00000000082c11eb" || p.Initial.Exe != jailKVMFakeStaged {
			t.Fatalf("proof %+v, %v", p, err)
		}
	})
	t.Run("never matching exe times out without a matched status", func(t *testing.T) {
		f := &jailKVMFakeMain{steps: []jailKVMFakeMainStep{jailKVMFakeExecutorStep()}}
		p, err := await(f, 30*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "systemd-executor") {
			t.Fatalf("err %v: want DeadlineExceeded naming the observed exe", err)
		}
		if p.Matched.Status != nil {
			t.Fatalf("a non-matching process's status was returned as matched: %+v", p.Matched)
		}
	})
	t.Run("an unreadable exe is not a match", func(t *testing.T) {
		gone := jailKVMFakeNodeStep()
		gone.exeErr = errors.New("no such process")
		f := &jailKVMFakeMain{steps: []jailKVMFakeMainStep{gone}}
		if _, err := await(f, 30*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("pre-canceled reads nothing", func(t *testing.T) {
		f := &jailKVMFakeMain{steps: []jailKVMFakeMainStep{jailKVMFakeNodeStep()}}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := jailKVMAwaitMainExe(ctx, "inv1", jailKVMFakeStaged, time.Second, time.Millisecond, f.ops()); !errors.Is(err, context.Canceled) {
			t.Fatalf("err %v", err)
		}
		if len(f.calls) != 0 {
			t.Fatalf("calls %q", f.calls)
		}
	})
	for name, change := range map[string]func(s *jailKVMFakeMainStep){
		"invocation changes":     func(s *jailKVMFakeMainStep) { s.inv = "inv2" },
		"main pid gone":          func(s *jailKVMFakeMainStep) { s.pid = 0 },
		"main pid changes":       func(s *jailKVMFakeMainStep) { s.pid = 4243 },
		"exe changes":            func(s *jailKVMFakeMainStep) { s.exe = jailKVMIdentity{Dev: 66306, Ino: 917002} },
		"start time changes":     func(s *jailKVMFakeMainStep) { s.start = "778" },
		"exe becomes unreadable": func(s *jailKVMFakeMainStep) { s.exeErr = errors.New("no such process") },
	} {
		t.Run(name+" around the status read", func(t *testing.T) {
			after := jailKVMFakeNodeStep()
			change(&after)
			f := &jailKVMFakeMain{steps: []jailKVMFakeMainStep{jailKVMFakeNodeStep(), after}}
			p, err := await(f, 5*time.Second)
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err %v: want an immediate refusal", err)
			}
			if p.Matched.Status != nil {
				t.Fatalf("an unproven status was returned: %+v", p.Matched)
			}
		})
	}
	for name, step := range map[string]jailKVMFakeMainStep{
		"invocation changes while waiting": {inv: "inv2", pid: 4242, exe: jailKVMFakeExecutor, link: "/usr/lib/systemd/systemd-executor", start: "777"},
		"main pid gone while waiting":      {inv: "inv1", pid: 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := &jailKVMFakeMain{steps: []jailKVMFakeMainStep{jailKVMFakeExecutorStep(), step, jailKVMFakeNodeStep()}}
			if _, err := await(f, 5*time.Second); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err %v: want an immediate refusal", err)
			}
		})
	}
}

// TestJailKVMCheckMainStatus: the matched main process's status must show
// exactly the supervisor's twelve capabilities (bounding, permitted and
// effective), nothing inheritable or ambient, NoNewPrivs and root.
func TestJailKVMCheckMainStatus(t *testing.T) {
	ok := jailKVMParseStatus(jailKVMFakeNodeStatus)
	if err := jailKVMCheckMainStatus(ok); err != nil {
		t.Fatalf("exact status refused: %v", err)
	}
	for name, edit := range map[string][2]string{
		"full bounding set":  {"CapBnd", "000001ffffffffff"},
		"one more bounding":  {"CapBnd", "00000000082c11ef"},
		"one less bounding":  {"CapBnd", "00000000082c11ea"},
		"wider effective":    {"CapEff", "000001ffffffffff"},
		"narrower effective": {"CapEff", "00000000082c11ea"},
		"wider permitted":    {"CapPrm", "00000000082c11ef"},
		"inheritable":        {"CapInh", "0000000000001000"},
		"ambient":            {"CapAmb", "0000000000001000"},
		"no NoNewPrivs":      {"NoNewPrivs", "0"},
		"not root":           {"Uid", "1000\t1000\t1000\t1000"},
		"saved uid not root": {"Uid", "0\t0\t1000\t0"},
		"fs uid not root":    {"Uid", "0\t0\t0\t1000"},
		"short uid":          {"Uid", "0\t0"},
		"unparsable":         {"CapBnd", "zz"},
		"missing":            {"CapEff", ""},
	} {
		t.Run(name, func(t *testing.T) {
			st := jailKVMParseStatus(jailKVMFakeNodeStatus)
			if edit[1] == "" {
				delete(st, edit[0])
			} else {
				st[edit[0]] = edit[1]
			}
			if err := jailKVMCheckMainStatus(st); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// TestJailKVMStatStartTime: field 22 is counted after the last ") ", so a
// comm containing ") " cannot shift it.
func TestJailKVMStatStartTime(t *testing.T) {
	tail := " S 1 4242 4242 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 9 0 987654 1000000 500"
	for comm, want := range map[string]string{"(bbraft-node)": "987654", "(a) b) c)": "987654", "((sd-executor))": "987654"} {
		if got, err := jailKVMStatStartTime("4242 " + comm + tail); err != nil || got != want {
			t.Errorf("comm %s: %q, %v; want %s", comm, got, err, want)
		}
	}
	for name, line := range map[string]string{"no comm": "4242 bbraft-node S 1", "short": "4242 (x) S 1 2", "not a number": "4242 (x) S 1 4242 4242 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 9 0 zz 1 2"} {
		if _, err := jailKVMStatStartTime(line); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
