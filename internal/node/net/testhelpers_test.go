package nodenet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
)

// netTestCommand is one recorded host command.
type netTestCommand struct {
	Name  string
	Args  []string
	Stdin string
}

func (c netTestCommand) String() string {
	if len(c.Args) == 0 {
		return c.Name
	}
	return c.Name + " " + strings.Join(c.Args, " ")
}

// netTestLink is one link in the fake host.
type netTestLink struct {
	Kind  string
	Alias string
	MAC   string
}

// netTestRunner is a Runner that records commands, emulates links, and can fail
// or cancel exactly one command. It is safe for concurrent use so the manager's
// concurrency test is race-clean.
type netTestRunner struct {
	mu      sync.Mutex
	runs    []netTestCommand
	outputs []netTestCommand

	// links maps an existing link name to its kind and alias.
	links map[string]netTestLink
	// out holds canned Output results by command string.
	out map[string]netTestOutput

	// failOnce fails the first Run whose command string has this prefix.
	failOnce string
	failErr  error
	// failIf fails every Run for which it returns an error.
	failIf   func(netTestCommand) error
	cancelOn string
	cancel   context.CancelFunc
}

type netTestOutput struct {
	data []byte
	err  error
}

func netTestNewRunner(t *testing.T) *netTestRunner {
	t.Helper()
	return &netTestRunner{links: make(map[string]netTestLink), out: make(map[string]netTestOutput)}
}

func (r *netTestRunner) Run(ctx context.Context, name string, args []string, stdin []byte) error {
	cmd := netTestCommand{Name: name, Args: slices.Clone(args), Stdin: string(stdin)}

	r.mu.Lock()
	r.runs = append(r.runs, cmd)
	cancelOn, cancel, failOnce, failErr := r.cancelOn, r.cancel, r.failOnce, r.failErr
	text := cmd.String()
	if cancelOn != "" && strings.HasPrefix(text, cancelOn) {
		r.cancelOn = ""
	} else {
		cancelOn = ""
	}
	if failOnce != "" && strings.HasPrefix(text, failOnce) {
		r.failOnce = ""
	} else {
		failOnce = ""
	}
	var failErrNow error
	switch {
	case failOnce != "":
		failErrNow = failErr
	case r.failIf != nil:
		failErrNow = r.failIf(cmd)
	}
	// A failed command did not change the host, so the fake link table is only
	// updated on success.
	if cancelOn == "" && failOnce == "" && failErrNow == nil {
		r.emulateLinkChange(cmd)
	}
	r.mu.Unlock()

	if cancelOn != "" {
		if cancel != nil {
			cancel()
		}
		return context.Canceled
	}
	if failErrNow != nil {
		return failErrNow
	}
	return ctx.Err()
}

// emulateLinkChange keeps the link table consistent for later inspections.
func (r *netTestRunner) emulateLinkChange(cmd netTestCommand) {
	if cmd.Name != "ip" || len(cmd.Args) < 3 {
		return
	}
	switch {
	case cmd.Args[0] == "link" && cmd.Args[1] == "add" && len(cmd.Args) >= 5:
		kind := "unknown"
		if cmd.Args[3] == "type" {
			kind = cmd.Args[4]
		}
		r.links[cmd.Args[2]] = netTestLink{Kind: kind}
	case cmd.Args[0] == "link" && cmd.Args[1] == "del" && len(cmd.Args) >= 4:
		delete(r.links, cmd.Args[3])
	case cmd.Args[0] == "link" && cmd.Args[1] == "set" && len(cmd.Args) >= 6 &&
		cmd.Args[2] == "dev" && cmd.Args[4] == "alias":
		if link, ok := r.links[cmd.Args[3]]; ok {
			link.Alias = cmd.Args[5]
			r.links[cmd.Args[3]] = link
		}
	case cmd.Args[0] == "link" && cmd.Args[1] == "set" && len(cmd.Args) >= 6 &&
		cmd.Args[2] == "dev" && cmd.Args[4] == "address":
		if link, ok := r.links[cmd.Args[3]]; ok {
			link.MAC = cmd.Args[5]
			r.links[cmd.Args[3]] = link
		}
	}
}

func (r *netTestRunner) Output(ctx context.Context, name string, args []string) ([]byte, error) {
	cmd := netTestCommand{Name: name, Args: slices.Clone(args)}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outputs = append(r.outputs, cmd)
	if canned, ok := r.out[cmd.String()]; ok {
		return canned.data, canned.err
	}
	if name == "ip" && len(args) >= 4 && args[0] == "-j" && args[1] == "-d" &&
		args[2] == "link" && args[3] == "show" {
		if len(args) == 6 && args[4] == "dev" {
			link, ok := r.links[args[5]]
			if !ok {
				return nil, &CommandError{
					Name:     name,
					Args:     cmd.Args,
					Stderr:   fmt.Sprintf(`Device "%s" does not exist.`, args[5]),
					ExitCode: 1,
					Err:      errors.New("exit status 1"),
				}
			}
			return append(append([]byte("["), netTestLinkJSON(args[5], link)...), ']'), nil
		}
		if len(args) == 4 {
			names := slices.Sorted(maps.Keys(r.links))
			var b bytes.Buffer
			b.WriteString("[")
			for i, linkName := range names {
				if i > 0 {
					b.WriteString(",")
				}
				b.Write(netTestLinkJSON(linkName, r.links[linkName]))
			}
			b.WriteString("]")
			return b.Bytes(), nil
		}
	}
	if cmd.String() == "ip -j -4 addr show" {
		// A host with no addresses at all: tests that care list their own.
		return []byte("[]"), nil
	}
	return nil, fmt.Errorf("nodenet test: unexpected Output command %q", cmd.String())
}

func netTestLinkJSON(name string, link netTestLink) []byte {
	return fmt.Appendf(nil, `{"ifname":%q,"ifalias":%q,"address":%q,"linkinfo":{"info_kind":%q}}`,
		name, link.Alias, link.MAC, link.Kind)
}

func (r *netTestRunner) setLink(name, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.links[name] = netTestLink{Kind: kind}
}

func (r *netTestRunner) setAlias(name, alias string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	link := r.links[name]
	link.Alias = alias
	r.links[name] = link
}

func (r *netTestRunner) linkKind(name string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	link, ok := r.links[name]
	return link.Kind, ok
}

func (r *netTestRunner) linkAlias(name string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.links[name].Alias
}

func (r *netTestRunner) linkMAC(name string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.links[name].MAC
}

// outputCommands returns every Output command as "name arg arg ...".
func (r *netTestRunner) outputCommands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.outputs))
	for i, cmd := range r.outputs {
		out[i] = cmd.String()
	}
	return out
}

// clearFaults removes every injected failure.
func (r *netTestRunner) clearFaults() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failOnce = ""
	r.failIf = nil
	r.cancelOn = ""
	r.cancel = nil
}

// commands returns every Run command as "name arg arg ...".
func (r *netTestRunner) commands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.runs))
	for i, cmd := range r.runs {
		out[i] = cmd.String()
	}
	return out
}

// indexOf returns the index of the first Run command with the given prefix.
func (r *netTestRunner) indexOf(prefix string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.IndexFunc(r.runs, func(c netTestCommand) bool {
		return strings.HasPrefix(c.String(), prefix)
	})
}

// nftBatches returns the ruleset batches applied through nft.
func (r *netTestRunner) nftBatches() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, cmd := range r.runs {
		if cmd.Name == "nft" {
			out = append(out, cmd.Stdin)
		}
	}
	return out
}

// lastNftBatch is the most recently applied ruleset.
func (r *netTestRunner) lastNftBatch(t *testing.T) string {
	t.Helper()
	batches := r.nftBatches()
	if len(batches) == 0 {
		t.Fatal("no nft batch was applied")
	}
	return batches[len(batches)-1]
}

// netTestTapper returns a real temporary file per TAP instead of a device, and
// records the links it created in the runner so deletion is observable.
type netTestTapper struct {
	mu        sync.Mutex
	runner    *netTestRunner
	dir       string
	opened    []string
	persisted int
	vhost     int
	tapErr    error
	files     []*os.File
	names     map[*os.File]string
}

func netTestNewTapper(t *testing.T, runner *netTestRunner) *netTestTapper {
	t.Helper()
	return &netTestTapper{runner: runner, dir: t.TempDir(), names: make(map[*os.File]string)}
}

func (f *netTestTapper) OpenTap(ctx context.Context, name string) (*os.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = append(f.opened, name)
	if f.tapErr != nil {
		return nil, f.tapErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(f.dir, "tap-")
	if err != nil {
		return nil, err
	}
	f.files = append(f.files, file)
	f.names[file] = name
	f.runner.setLink(name, "tun")
	return file, nil
}

// PersistTap records the call and enforces the ordering contract: a TAP may only
// be made persistent after its ownership alias exists.
func (f *netTestTapper) PersistTap(ctx context.Context, file *os.File) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	f.persisted++
	name := f.names[file]
	if name == "" {
		return errors.New("persist of an unknown tap descriptor")
	}
	// The ownership alias must already exist when a TAP is made persistent.
	if !strings.HasPrefix(f.runner.linkAlias(name), aliasPrefix) {
		return errors.New("tap persisted before its ownership alias")
	}
	return nil
}

func (f *netTestTapper) OpenVhostNet(ctx context.Context) (*os.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.vhost++
	file, err := os.CreateTemp(f.dir, "vhost-")
	if err != nil {
		return nil, err
	}
	f.files = append(f.files, file)
	return file, nil
}

func (f *netTestTapper) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.opened)
}

// persistedCount is how many TAPs were marked persistent.
func (f *netTestTapper) persistedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.persisted
}

// netTestState is a stateStore with fault injection at the commit boundary.
type netTestState struct {
	// afterRename makes a failing Write first perform the real file write and
	// rename, so the error models "renamed, then the directory sync failed".
	afterRename bool
	// writeFailures and removeFailures are how many calls fail before
	// succeeding; the errors are writeErr and removeErr.
	writeFailures  int
	removeFailures int
	writeErr       error
	removeErr      error
	// removed counts successful removals.
	removed int
}

func (s *netTestState) Write(root *os.Root, a Allocation) error {
	if s.afterRename {
		if err := writeAllocationFile(root, a); err != nil {
			return err
		}
	}
	if s.writeFailures > 0 {
		s.writeFailures--
		if s.writeErr == nil {
			return os.ErrInvalid
		}
		return s.writeErr
	}
	if s.afterRename {
		return syncDir(root)
	}
	return writeAllocation(root, a)
}

func (s *netTestState) Remove(root *os.Root, sandboxID string) error {
	if s.removeFailures > 0 {
		s.removeFailures--
		if s.removeErr == nil {
			return os.ErrInvalid
		}
		return s.removeErr
	}
	s.removed++
	return removeAllocation(root, sandboxID)
}

// netTestConfig is the reference node: a /16 guest range, two public
// resolvers, a management network and a three-port SSH ingress pool.
func netTestConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Bridge:      "bbraft0",
		Prefix:      netip.MustParsePrefix("10.42.0.0/16"),
		Gateway:     netip.MustParseAddr("10.42.0.1"),
		Resolvers:   []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("8.8.8.8")},
		Management:  []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")},
		IngressAddr: netip.MustParseAddr("203.0.113.10"),
		SSHPorts:    PortRange{Min: 30000, Max: 30002},
		StateDir:    t.TempDir(),
	}
}

// netTestAllocation builds a consistent allocation for the given id and
// address without touching a manager.
func netTestAllocation(cfg Config, sandboxID string, ip netip.Addr, sshPort uint16) Allocation {
	a := Allocation{
		SandboxID: sandboxID,
		GuestIP:   ip,
		Prefix:    cfg.Prefix.Masked(),
		Gateway:   cfg.Gateway,
		Bridge:    cfg.Bridge,
		MAC:       macForAddr(ip),
		TAPName:   tapFor(sandboxID),
		SSHPort:   sshPort,
	}
	if sshPort != 0 {
		a.IngressAddr = cfg.IngressAddr
		a.IngressIface = cfg.IngressIface
	}
	return a
}

// netTestManager builds a manager with recording fakes.
func netTestManager(t *testing.T, cfg Config) (*Manager, *netTestRunner, *netTestTapper) {
	t.Helper()
	runner := netTestNewRunner(t)
	tapper := netTestNewTapper(t, runner)
	m, err := New(cfg, Deps{Runner: runner, Taps: tapper})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return m, runner, tapper
}

// nonCommentLines drops the leading comment lines of a ruleset so assertions
// about tables are not confused by documentation that names them.
func nonCommentLines(batch string) []string {
	var out []string
	for line := range strings.SplitSeq(batch, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}
