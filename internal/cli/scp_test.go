package cli

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// sshTestAccess builds an sshAccess for direct invocation-builder tests.
func sshTestAccess(host string, port int) sshAccess {
	key, _ := parseSSHHostKey(sshTestHostKey(20))
	return sshAccess{
		sandboxID:      sshTestSandboxID,
		target:         sshTarget{Host: host, Port: port, User: sshTestUser, HostKey: key},
		privateKeyPath: "/home/u/.ssh/" + sshKeyFileName,
		knownHostsPath: "/tmp/known_hosts",
	}
}

// sshTestSCPInvocation parses args and builds the scp command.
func sshTestSCPInvocation(t *testing.T, access sshAccess, args []string, recursive bool) (ProcessCommand, error) {
	t.Helper()
	plan, err := parseSCPPlan(args, recursive)
	if err != nil {
		return ProcessCommand{}, err
	}
	return scpInvocation(access, plan, strings.NewReader(""), nil, nil), nil
}

// TestSCPInvocationShapes covers upload, download, multiple sources, recursion,
// ports, IPv6 and the destination-last rule.
func TestSCPInvocationShapes(t *testing.T) {
	baseOptions := func() []string {
		return []string{
			"-o", "IdentitiesOnly=yes",
			"-o", "StrictHostKeyChecking=yes",
			"-o", "UserKnownHostsFile=" + sshConfigValue("/tmp/known_hosts"),
			"-o", "GlobalKnownHostsFile=/dev/null",
			"-o", "HostKeyAlias=" + sshTestSandboxID,
			"-o", "CheckHostIP=no",
			"-o", "VerifyHostKeyDNS=no",
			"-o", "UpdateHostKeys=no",
		}
	}
	cases := []struct {
		name      string
		args      []string
		recursive bool
		access    sshAccess
		want      func() []string
	}{
		{
			name:   "upload",
			args:   []string{"./local.txt", sshTestSandboxID + ":/home/user/"},
			access: sshTestAccess("203.0.113.5", 22),
			want: func() []string {
				return append(append([]string{"-i", "/home/u/.ssh/" + sshKeyFileName}, baseOptions()...),
					"./local.txt", "user@203.0.113.5:/home/user/")
			},
		},
		{
			name:   "download",
			args:   []string{sshTestSandboxID + ":/home/user/output.zip", "./output.zip"},
			access: sshTestAccess("203.0.113.5", 22),
			want: func() []string {
				return append(append([]string{"-i", "/home/u/.ssh/" + sshKeyFileName}, baseOptions()...),
					"user@203.0.113.5:/home/user/output.zip", "./output.zip")
			},
		},
		{
			name:   "multiple sources",
			args:   []string{"a.txt", "b.txt", sshTestSandboxID + ":/home/user/"},
			access: sshTestAccess("203.0.113.5", 22),
			want: func() []string {
				return append(append([]string{"-i", "/home/u/.ssh/" + sshKeyFileName}, baseOptions()...),
					"a.txt", "b.txt", "user@203.0.113.5:/home/user/")
			},
		},
		{
			name:      "recursive port",
			args:      []string{"tree", sshTestSandboxID + ":/home/user/"},
			recursive: true,
			access:    sshTestAccess("203.0.113.5", 2200),
			want: func() []string {
				args := []string{"-r", "-i", "/home/u/.ssh/" + sshKeyFileName, "-P", "2200"}
				args = append(args, baseOptions()...)
				return append(args, "tree", "user@203.0.113.5:/home/user/")
			},
		},
		{
			name:   "ipv6",
			args:   []string{"./f", sshTestSandboxID + ":/home/user/"},
			access: sshTestAccess("2001:db8::1", 2222),
			want: func() []string {
				args := []string{"-i", "/home/u/.ssh/" + sshKeyFileName, "-P", "2222"}
				args = append(args, baseOptions()...)
				return append(args, "./f", "user@[2001:db8::1]:/home/user/")
			},
		},
		{
			name:   "path with spaces",
			args:   []string{sshTestSandboxID + ":/home/user/a b.txt", "./out file"},
			access: sshTestAccess("203.0.113.5", 22),
			want: func() []string {
				return append(append([]string{"-i", "/home/u/.ssh/" + sshKeyFileName}, baseOptions()...),
					"user@203.0.113.5:/home/user/a b.txt", "./out file")
			},
		},
		{
			name:   "remote option-like path",
			args:   []string{"x", sshTestSandboxID + ":-rf"},
			access: sshTestAccess("203.0.113.5", 22),
			want: func() []string {
				return append(append([]string{"-i", "/home/u/.ssh/" + sshKeyFileName}, baseOptions()...),
					"x", "user@203.0.113.5:-rf")
			},
		},
		{
			name:   "local option-like path",
			args:   []string{"-rf", sshTestSandboxID + ":/home/user/"},
			access: sshTestAccess("203.0.113.5", 22),
			want: func() []string {
				return append(append([]string{"-i", "/home/u/.ssh/" + sshKeyFileName}, baseOptions()...),
					"./-rf", "user@203.0.113.5:/home/user/")
			},
		},
		{
			name:   "local relative colon path",
			args:   []string{"dir/a:b", sshTestSandboxID + ":/home/user/"},
			access: sshTestAccess("203.0.113.5", 22),
			want: func() []string {
				return append(append([]string{"-i", "/home/u/.ssh/" + sshKeyFileName}, baseOptions()...),
					"./dir/a:b", "user@203.0.113.5:/home/user/")
			},
		},
		{
			name:   "local absolute colon path",
			args:   []string{"/tmp/a:b", sshTestSandboxID + ":/home/user/"},
			access: sshTestAccess("203.0.113.5", 22),
			want: func() []string {
				return append(append([]string{"-i", "/home/u/.ssh/" + sshKeyFileName}, baseOptions()...),
					"/tmp/a:b", "user@203.0.113.5:/home/user/")
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			command, err := sshTestSCPInvocation(t, testCase.access, testCase.args, testCase.recursive)
			if err != nil {
				t.Fatalf("scp invocation: %v", err)
			}
			want := testCase.want()
			if !slices.Equal(command.Args, want) {
				t.Fatalf("args = %q, want %q", command.Args, want)
			}
			if command.Path != "scp" {
				t.Fatalf("path = %q", command.Path)
			}
			if command.Foreground {
				t.Fatal("piped stdin marked scp foreground")
			}
		})
	}
}

// TestParseSCPPlanRejects rejects impossible operands before any request.
func TestParseSCPPlanRejects(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "no remote", args: []string{"a", "b"}},
		{name: "two remote", args: []string{sshTestSandboxID + ":/a", sshTestSandboxID2 + ":/b"}},
		{name: "empty operand", args: []string{"", sshTestSandboxID + ":/a"}},
		{name: "NUL operand", args: []string{"a\x00b", sshTestSandboxID + ":/a"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := parseSCPPlan(testCase.args, false); err == nil {
				t.Fatal("plan accepted impossible operands")
			} else if _, ok := errors.AsType[*UsageError](err); !ok {
				t.Fatalf("error = %v, want UsageError", err)
			}
		})
	}
	t.Run("invalid id treated as local colon path", func(t *testing.T) {
		plan, err := parseSCPPlan([]string{"bx_bad:/path", sshTestSandboxID + ":/dest"}, false)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if plan.remoteIndex != 1 || plan.remoteID != sshTestSandboxID {
			t.Fatalf("plan = %+v, want the valid id as the only remote", plan)
		}
	})
}

// TestSCPCommand runs scp through the App: API setup, exit codes, recursion and
// usage errors.
func TestSCPCommand(t *testing.T) {
	t.Run("recursive upload", func(t *testing.T) {
		captured, calls := sshTestCaptureApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(21)), 0)
		status := captured.env.run(t, "scp", "--recursive", "./tree", sshTestSandboxID+":/home/user/")
		if status != 0 {
			t.Fatalf("exit status = %d; stderr=%q", status, captured.stderr.String())
		}
		command := sshTestCall(t, calls)
		if command.Path != "scp" || len(command.Args) == 0 || command.Args[0] != "-r" {
			t.Fatalf("args = %q, want scp -r", command.Args)
		}
		if command.Args[len(command.Args)-1] != "user@203.0.113.5:/home/user/" {
			t.Fatalf("destination = %q", command.Args[len(command.Args)-1])
		}
	})
	t.Run("exit code", func(t *testing.T) {
		captured, _ := sshTestCaptureApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(22)), 44)
		if status := captured.env.run(t, "scp", "./a", sshTestSandboxID+":/b"); status != 44 {
			t.Fatalf("exit status = %d, want 44", status)
		}
	})
	t.Run("explicit json", func(t *testing.T) {
		called := false
		runner := func(context.Context, ProcessCommand) (int, error) {
			called = true
			return 0, nil
		}
		env := sshTestApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(23)), sshTestHome(t), runner)
		if status := env.run(t, "scp", "--json", "./a", sshTestSandboxID+":/b"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
		if called {
			t.Fatal("scp ran after a usage error")
		}
	})
	t.Run("missing remote", func(t *testing.T) {
		called := false
		handler := func(http.ResponseWriter, *http.Request) { called = true }
		runner := func(context.Context, ProcessCommand) (int, error) { return 0, nil }
		env := sshTestApp(t, handler, sshTestHome(t), runner)
		if status := env.run(t, "scp", "./a", "./b"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
		if called {
			t.Fatal("API called without a remote operand")
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		started := make(chan struct{})
		runner := func(ctx context.Context, _ ProcessCommand) (int, error) {
			close(started)
			<-ctx.Done()
			return -1, ctx.Err()
		}
		env := sshTestApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(24)), sshTestHome(t), runner)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan int, 1)
		finished := false
		defer func() {
			cancel()
			if !finished {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
				}
			}
		}()
		go func() { done <- env.runContext(ctx, "scp", "./a", sshTestSandboxID+":/b") }()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("scp did not start")
		}
		cancel()
		select {
		case status := <-done:
			finished = true
			if status != 130 {
				t.Fatalf("exit status = %d, want 130", status)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("scp did not stop")
		}
	})
}

// TestLocalSCPPath pins the local operand disambiguation rules.
func TestLocalSCPPath(t *testing.T) {
	cases := []struct {
		arg  string
		want string
	}{
		{arg: "plain", want: "plain"},
		{arg: "./plain", want: "./plain"},
		{arg: "-rf", want: "./-rf"},
		{arg: "./-rf", want: "./-rf"},
		{arg: "a:b", want: "./a:b"},
		{arg: "dir/a:b", want: "./dir/a:b"},
		{arg: "/tmp/a:b", want: "/tmp/a:b"},
		{arg: "/tmp/file", want: "/tmp/file"},
		{arg: "with space", want: "with space"},
	}
	for _, testCase := range cases {
		got, err := localSCPPath(testCase.arg)
		if err != nil {
			t.Fatalf("local path %q: %v", testCase.arg, err)
		}
		if got != testCase.want {
			t.Errorf("localSCPPath(%q) = %q, want %q", testCase.arg, got, testCase.want)
		}
	}
	if _, err := localSCPPath("a\x00b"); err == nil {
		t.Fatal("NUL local path accepted")
	}
}

// TestSCPFlagsAreNotShellInterpolated keeps metacharacters inside one argv.
func TestSCPFlagsAreNotShellInterpolated(t *testing.T) {
	access := sshTestAccess("203.0.113.5", 22)
	command, err := sshTestSCPInvocation(t, access,
		[]string{"a;b", "$(whoami)", sshTestSandboxID + ":/home/user/`id`"}, false)
	if err != nil {
		t.Fatalf("invocation: %v", err)
	}
	for _, want := range []string{"a;b", "$(whoami)", "user@203.0.113.5:/home/user/`id`"} {
		if !slices.Contains(command.Args, want) {
			t.Fatalf("args = %q, want %q as one element", command.Args, want)
		}
	}
}

// TestSCPInvocationUsesPortFlag capital-P is scp's port flag.
func TestSCPInvocationUsesPortFlag(t *testing.T) {
	access := sshTestAccess("203.0.113.5", 2222)
	command, err := sshTestSCPInvocation(t, access, []string{"a", sshTestSandboxID + ":/b"}, false)
	if err != nil {
		t.Fatalf("invocation: %v", err)
	}
	index := slices.Index(command.Args, "-P")
	if index < 0 || index+1 >= len(command.Args) || command.Args[index+1] != strconv.Itoa(2222) {
		t.Fatalf("args = %q, want -P 2222", command.Args)
	}
}
