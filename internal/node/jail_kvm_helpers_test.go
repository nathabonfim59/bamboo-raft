package node

// Test-only helpers of the A10 final jailed harness (jail_kvm_test.go). The
// pure helpers here are specified by jail_kvm_pure_test.go; the
// infrastructure methods in jail_kvm_test.go are still stubs (checkpoint).
// No production code depends on this file.

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// jailKVMUnit is a parsed systemd unit: the [Service] directives in file
// order (list-valued directives keep every occurrence) and the ExecStart
// argv, joined the way systemd joins continuation lines.
type jailKVMUnit struct {
	Service   map[string][]string
	ExecStart []string
}

// jailKVMListDirectives may appear more than once in [Service].
var jailKVMListDirectives = []string{"DeviceAllow", "Environment", "ReadWritePaths", "ReadOnlyPaths"}

// jailKVMParseUnit parses a role-rendered unit. It refuses a unit without a
// [Service] section or ExecStart, a dangling continuation and a directive
// that appears twice when it is not list-valued.
func jailKVMParseUnit(text string) (jailKVMUnit, error) {
	u := jailKVMUnit{Service: map[string][]string{}}
	section, seenService := "", false
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		for strings.HasSuffix(line, "\\") {
			i++
			if i >= len(lines) {
				return jailKVMUnit{}, errors.New("unit: dangling continuation at the end")
			}
			line = strings.TrimSuffix(line, "\\") + " " + strings.TrimSpace(lines[i])
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = line[1 : len(line)-1]
			seenService = seenService || section == "Service"
			continue
		}
		if section != "Service" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return jailKVMUnit{}, fmt.Errorf("unit: %q is not KEY=VALUE", line)
		}
		key = strings.TrimSpace(key)
		if _, dup := u.Service[key]; dup && !slices.Contains(jailKVMListDirectives, key) {
			return jailKVMUnit{}, fmt.Errorf("unit: %s appears more than once", key)
		}
		u.Service[key] = append(u.Service[key], value)
	}
	if !seenService {
		return jailKVMUnit{}, errors.New("unit: no [Service] section")
	}
	exec, ok := u.Service["ExecStart"]
	if !ok {
		return jailKVMUnit{}, errors.New("unit: no ExecStart")
	}
	argv, err := jailKVMSplitWords(exec[0])
	if err != nil {
		return jailKVMUnit{}, fmt.Errorf("unit: ExecStart: %w", err)
	}
	if len(argv) == 0 {
		return jailKVMUnit{}, errors.New("unit: empty ExecStart")
	}
	u.ExecStart = argv
	return u, nil
}

// jailKVMSplitWords splits a systemd command line or path list: words
// separated by whitespace, "..." quoting with \" and \\ escapes, and the
// specifier escapes %% and $$ undone.
func jailKVMSplitWords(text string) ([]string, error) {
	var out []string
	var b strings.Builder
	inWord, quoted := false, false
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case quoted && c == '\\':
			if i+1 >= len(text) {
				return nil, errors.New("dangling backslash")
			}
			i++
			b.WriteByte(text[i])
		case c == '"':
			quoted, inWord = !quoted, true
		case !quoted && (c == ' ' || c == '\t'):
			if inWord {
				out = append(out, b.String())
				b.Reset()
				inWord = false
			}
		case (c == '%' || c == '$') && i+1 < len(text) && text[i+1] == c:
			b.WriteByte(c)
			i++
			inWord = true
		default:
			b.WriteByte(c)
			inWord = true
		}
	}
	if quoted {
		return nil, errors.New("unterminated quote")
	}
	if inWord {
		out = append(out, b.String())
	}
	return out, nil
}

// jailKVMExpect is what one attempt's rendered ExecStart must carry: every
// flag listed in Flags with exactly its value, in tap mode, and none of the
// dev/test flags.
type jailKVMExpect struct {
	Binary string
	Flags  map[string]string
}

// jailKVMBoolFlags are bbraft-node's value-less flags; every other flag the
// role renders carries one value (which may itself start with '-').
var jailKVMBoolFlags = []string{"-node.cpu-allow-host"}

// jailKVMCheckExec checks a parsed ExecStart against want.
func jailKVMCheckExec(argv []string, want jailKVMExpect) error {
	if len(argv) == 0 || argv[0] != want.Binary {
		return fmt.Errorf("ExecStart binary %q, want %q", slices.Concat(argv, []string{""})[0], want.Binary)
	}
	flags := map[string]string{}
	for i := 1; i < len(argv); i++ {
		name := argv[i]
		if !strings.HasPrefix(name, "-") {
			return fmt.Errorf("ExecStart: %q is not a flag", name)
		}
		if _, dup := flags[name]; dup {
			return fmt.Errorf("ExecStart: %s appears twice", name)
		}
		if slices.Contains(jailKVMBoolFlags, name) {
			flags[name] = ""
			continue
		}
		if i+1 >= len(argv) {
			return fmt.Errorf("ExecStart: %s has no value", name)
		}
		i++
		flags[name] = argv[i]
	}
	if flags["-node.network-mode"] != "tap" {
		return fmt.Errorf("ExecStart: -node.network-mode %q, want tap", flags["-node.network-mode"])
	}
	for _, name := range jailKVMBoolFlags {
		if _, ok := flags[name]; ok {
			return fmt.Errorf("ExecStart: dev/test flag %s", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(want.Flags)) {
		got, ok := flags[name]
		if !ok || got != want.Flags[name] {
			return fmt.Errorf("ExecStart: %s = %q (present %v), want %q", name, got, ok, want.Flags[name])
		}
	}
	return nil
}

// jailKVMDirect replaces ExecStart and adds Environment= for the Direct
// helper (test binary only).
type jailKVMDirect struct {
	Exec []string
	Env  map[string]string
}

var (
	jailKVMSafePath = regexp.MustCompile(`^/[A-Za-z0-9/._-]+$`)
	jailKVMEnvKey   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func jailKVMHasControl(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f })
}

// jailKVMQuote quotes one word for a unit file: backslash and quote escaped,
// and % (always) and $ (exec only) doubled so systemd expands nothing.
func jailKVMQuote(s string, exec bool) string {
	s = strings.NewReplacer(`\`, `\\`, `"`, `\"`, `%`, `%%`).Replace(s)
	if exec {
		s = strings.ReplaceAll(s, "$", "$$")
	}
	return `"` + s + `"`
}

// jailKVMDropIn renders the attempt's only drop-in: NetworkNamespacePath=
// netnsPath and, for Direct, the ExecStart reset plus the helper ExecStart
// and Environment. It refuses relative paths and control characters.
func jailKVMDropIn(netnsPath string, direct *jailKVMDirect) (string, error) {
	if !jailKVMSafePath.MatchString(netnsPath) || path.Clean(netnsPath) != netnsPath {
		return "", fmt.Errorf("drop-in: netns path %q is not a clean absolute path", netnsPath)
	}
	var b strings.Builder
	b.WriteString("[Service]\nNetworkNamespacePath=" + netnsPath + "\n")
	if direct == nil {
		return b.String(), nil
	}
	if len(direct.Exec) == 0 || !jailKVMSafePath.MatchString(direct.Exec[0]) || path.Clean(direct.Exec[0]) != direct.Exec[0] {
		return "", errors.New("drop-in: the Direct helper needs an absolute clean executable")
	}
	words := make([]string, 0, len(direct.Exec))
	for _, w := range direct.Exec {
		if jailKVMHasControl(w) {
			return "", fmt.Errorf("drop-in: control character in %q", w)
		}
		words = append(words, jailKVMQuote(w, true))
	}
	b.WriteString("ExecStart=\nExecStart=" + strings.Join(words, " ") + "\n")
	for _, k := range slices.Sorted(maps.Keys(direct.Env)) {
		v := direct.Env[k]
		if !jailKVMEnvKey.MatchString(k) || jailKVMHasControl(v) {
			return "", fmt.Errorf("drop-in: invalid environment entry %q", k)
		}
		b.WriteString("Environment=" + jailKVMQuote(k+"="+v, false) + "\n")
	}
	return b.String(), nil
}

// jailKVMBoundaryCaps is the root jail supervisor's exact bounding set
// (docs/node.md "Jailed QEMU"; role unit). A rendered unit that differs is a
// weakened or widened boundary and is refused, never mirrored.
var jailKVMBoundaryCaps = []string{"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER", "CAP_KILL", "CAP_MKNOD", "CAP_NET_ADMIN",
	"CAP_SETGID", "CAP_SETPCAP", "CAP_SETUID", "CAP_SYS_ADMIN", "CAP_SYS_CHROOT", "CAP_SYS_PTRACE"}

// jailKVMBoundaryDevices is the exact explicit DeviceAllow set. /dev/vsock
// is read-only: the supervisor's one startup VM_SOCKETS_GET_LOCAL_CID ioctl
// on an O_RDONLY fd (startupLocalCID); no jail's private /dev holds it.
var jailKVMBoundaryDevices = []string{"/dev/kvm rwm", "/dev/vhost-vsock rw", "/dev/vhost-net rw", "/dev/net/tun rw", "/dev/null rwm", "/dev/urandom rwm", "/dev/vsock r"}

// jailKVMBoundaryImpliedDevices is what systemd adds to the effective
// DeviceAllow set on its own: ProtectClock=yes (required above) implies
// DeviceAllow=char-rtc r (systemd.exec(5)), read-only RTC nodes. A unit
// never states it; the live show must carry exactly it.
var jailKVMBoundaryImpliedDevices = []string{"char-rtc r"}

// jailKVMBoundaryLiveDevices is the exact effective DeviceAllow set the
// running unit shows: the explicit seven plus the ProtectClock implication.
func jailKVMBoundaryLiveDevices() []string {
	return slices.Concat(jailKVMBoundaryDevices, jailKVMBoundaryImpliedDevices)
}

// jailKVMBoundaryFamilies is the exact RestrictAddressFamilies set.
var jailKVMBoundaryFamilies = []string{"AF_INET", "AF_INET6", "AF_NETLINK", "AF_UNIX", "AF_VSOCK"}

// jailKVMBoundaryWant is what the attempt's paths must be. They come from
// the validated attempt (manifest) and its ExecStart, never from the unit's
// own path directives.
type jailKVMBoundaryWant struct {
	NetnsPath string
	DataDir   string
	Service   string
}

// jailKVMSetOf returns the sorted members of words, refusing duplicates.
func jailKVMSetOf(what string, words []string) ([]string, error) {
	out := slices.Sorted(slices.Values(words))
	if len(slices.Compact(slices.Clone(out))) != len(out) {
		return nil, fmt.Errorf("boundary: %s %q repeats an entry", what, words)
	}
	return out, nil
}

// jailKVMImmutable validates unit against the fixed supervisor boundary
// (every hardening directive of the role unit, exact capabilities, empty
// ambient set, the closed device set, the exact address families and
// resource limits, ReadOnlyPaths exactly /sys/fs/cgroup and ReadWritePaths
// exactly want's data dir plus its own service cgroup, no
// User/Group/DynamicUser, no namespace or syscall filter and no
// NetworkNamespacePath of its own) and returns the normalized
// `systemctl show` values it must have once loaded with want.NetnsPath.
func jailKVMImmutable(unit jailKVMUnit, want jailKVMBoundaryWant) (map[string]string, error) {
	if !jailKVMSafePath.MatchString(want.NetnsPath) || !jailKVMSafePath.MatchString(want.DataDir) ||
		path.Clean(want.DataDir) != want.DataDir || !regexp.MustCompile(`^[A-Za-z0-9_.-]+$`).MatchString(want.Service) {
		return nil, fmt.Errorf("boundary: invalid expectation %+v", want)
	}
	one := func(key string) (string, bool) {
		v, ok := unit.Service[key]
		if !ok || len(v) != 1 {
			return "", false
		}
		return strings.TrimSpace(v[0]), true
	}
	require := func(key string, allowed ...string) error {
		v, ok := one(key)
		if !ok || !slices.Contains(allowed, v) {
			return fmt.Errorf("boundary: %s = %q (present %v), want one of %q", key, v, ok, allowed)
		}
		return nil
	}
	caps, ok := one("CapabilityBoundingSet")
	if !ok || !slices.Equal(slices.Sorted(slices.Values(strings.Fields(caps))), jailKVMBoundaryCaps) {
		return nil, fmt.Errorf("boundary: CapabilityBoundingSet %q is not exactly %q", caps, jailKVMBoundaryCaps)
	}
	if v, ok := one("AmbientCapabilities"); !ok || v != "" {
		return nil, fmt.Errorf("boundary: AmbientCapabilities %q must be present and empty", v)
	}
	yes := []string{"true", "yes"}
	for _, check := range []error{
		require("NoNewPrivileges", yes...),
		require("SystemCallArchitectures", "native"),
		require("Delegate", yes...),
		require("DelegateSubgroup", "supervisor"),
		require("DevicePolicy", "closed"),
		require("ProtectSystem", "strict"),
		require("ProtectHome", yes...),
		require("PrivateTmp", yes...),
		require("ProtectControlGroups", "false", "no"),
		require("ProtectKernelTunables", yes...),
		require("ProtectKernelModules", yes...),
		require("ProtectKernelLogs", yes...),
		require("ProtectClock", yes...),
		require("RestrictSUIDSGID", yes...),
		require("LockPersonality", yes...),
		require("KillMode", "process"),
		require("UMask", "0077"),
		require("LimitCORE", "0"),
		require("LimitNOFILE", "65536"),
		require("TasksMax", "infinity"),
		require("TimeoutStopSec", "30"),
	} {
		if check != nil {
			return nil, check
		}
	}
	devices, err := jailKVMSetOf("DeviceAllow", unit.Service["DeviceAllow"])
	if err != nil {
		return nil, err
	}
	if !slices.Equal(devices, slices.Sorted(slices.Values(jailKVMBoundaryDevices))) {
		return nil, fmt.Errorf("boundary: DeviceAllow %q, want exactly %q", unit.Service["DeviceAllow"], jailKVMBoundaryDevices)
	}
	families, ok := one("RestrictAddressFamilies")
	if fam, err := jailKVMSetOf("RestrictAddressFamilies", strings.Fields(families)); !ok || err != nil || !slices.Equal(fam, jailKVMBoundaryFamilies) {
		return nil, fmt.Errorf("boundary: RestrictAddressFamilies %q, want exactly %q", families, jailKVMBoundaryFamilies)
	}
	for _, key := range []string{"User", "Group", "DynamicUser", "SupplementaryGroups", "RestrictNamespaces", "SystemCallFilter",
		"NetworkNamespacePath", "PrivateNetwork", "InaccessiblePaths", "BindPaths", "BindReadOnlyPaths"} {
		if _, present := unit.Service[key]; present {
			return nil, fmt.Errorf("boundary: the rendered unit sets %s", key)
		}
	}
	if v, ok := unit.Service["PrivateDevices"]; ok && (len(v) != 1 || !slices.Contains([]string{"false", "no"}, v[0])) {
		return nil, fmt.Errorf("boundary: PrivateDevices %q", v)
	}
	paths := func(key string) ([]string, error) {
		v, ok := unit.Service[key]
		if !ok || len(v) != 1 {
			return nil, fmt.Errorf("boundary: %s must appear exactly once", key)
		}
		words, err := jailKVMSplitWords(v[0])
		if err != nil {
			return nil, fmt.Errorf("boundary: %s: %w", key, err)
		}
		return jailKVMSetOf(key, words)
	}
	cgroup := "/sys/fs/cgroup/system.slice/" + want.Service + ".service"
	ro, err := paths("ReadOnlyPaths")
	if err != nil {
		return nil, err
	}
	if !slices.Equal(ro, []string{"/sys/fs/cgroup"}) {
		return nil, fmt.Errorf("boundary: ReadOnlyPaths %q, want exactly /sys/fs/cgroup", ro)
	}
	rw, err := paths("ReadWritePaths")
	if err != nil {
		return nil, err
	}
	if wantRW := slices.Sorted(slices.Values([]string{want.DataDir, cgroup})); !slices.Equal(rw, wantRW) {
		return nil, fmt.Errorf("boundary: ReadWritePaths %q, want exactly %q", rw, wantRW)
	}
	lower := make([]string, len(jailKVMBoundaryCaps))
	for i, c := range jailKVMBoundaryCaps {
		lower[i] = strings.ToLower(c)
	}
	return map[string]string{
		"User": "", "Group": "", "DynamicUser": "no",
		"CapabilityBoundingSet": strings.Join(lower, " "), "AmbientCapabilities": "",
		"NoNewPrivileges": "yes", "SystemCallArchitectures": "native",
		"Delegate": "yes", "DelegateSubgroup": "supervisor",
		"DevicePolicy": "closed", "DeviceAllow": strings.Join(jailKVMBoundaryLiveDevices(), "\n"),
		"ProtectSystem": "strict", "ProtectHome": "yes", "PrivateTmp": "yes", "ProtectControlGroups": "no",
		"ProtectKernelTunables": "yes", "ProtectKernelModules": "yes", "ProtectKernelLogs": "yes", "ProtectClock": "yes",
		"RestrictSUIDSGID": "yes", "LockPersonality": "yes",
		"RestrictAddressFamilies": strings.Join(jailKVMBoundaryFamilies, " "),
		"ReadOnlyPaths":           "/sys/fs/cgroup", "ReadWritePaths": want.DataDir + " " + cgroup,
		"KillMode": "process", "UMask": "0077", "LimitCORE": "0", "LimitNOFILE": "65536",
		"TasksMax": "infinity", "TimeoutStopUSec": "30s",
		"RestrictNamespaces": "no", "SystemCallFilter": "",
		"NetworkNamespacePath": want.NetnsPath,
	}, nil
}

// jailKVMSetProperties compare as sets: word sets for the capability,
// family and path properties, exact unique "path mode" entries (one per
// repeated KEY= line) for DeviceAllow, whose presentation order systemd
// does not promise. A repeated entry is refused.
var (
	jailKVMWordSetProperties = []string{"CapabilityBoundingSet", "AmbientCapabilities", "RestrictAddressFamilies", "ReadOnlyPaths", "ReadWritePaths"}
	jailKVMLineSetProperties = []string{"DeviceAllow"}
)

// jailKVMCheckShow compares normalized `systemctl show -p ...` output
// (KEY=VALUE lines; a repeated key's values are joined by newlines) with
// want; every key in want must be present and equal.
func jailKVMCheckShow(want map[string]string, show string) error {
	got := map[string]string{}
	for line := range strings.SplitSeq(show, "\n") {
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("systemctl show: %q is not KEY=VALUE", line)
		}
		if prev, seen := got[k]; seen {
			v = prev + "\n" + v
		}
		got[k] = v
	}
	norm := func(k, v string) (string, error) {
		switch {
		case slices.Contains(jailKVMWordSetProperties, k):
			set, err := jailKVMSetOf(k, strings.Fields(strings.ToLower(v)))
			return strings.Join(set, " "), err
		case slices.Contains(jailKVMLineSetProperties, k):
			set, err := jailKVMSetOf(k, strings.Split(v, "\n"))
			return strings.Join(set, "\n"), err
		case k == "SystemCallFilter" && v == "~":
			// systemd prints an unset filter as an empty denylist; any
			// other deny or allow list stays and is compared as is.
			return "", nil
		}
		return v, nil
	}
	for _, k := range slices.Sorted(maps.Keys(want)) {
		v, ok := got[k]
		if !ok {
			return fmt.Errorf("systemctl show lacks %s", k)
		}
		gv, err := norm(k, v)
		if err != nil {
			return fmt.Errorf("systemctl show: %w", err)
		}
		wv, err := norm(k, want[k])
		if err != nil {
			return err
		}
		if gv != wv {
			return fmt.Errorf("systemctl show %s = %q, want %q", k, v, want[k])
		}
	}
	return nil
}

// jailKVMStartupPhases is production main's startup order (cmd/bbraft-node,
// internal/node): exact slog msg texts.
var jailKVMStartupPhases = []string{"jail admitted", "cpu realized", "vm networking ready", "node: qemu-img self-check passed", "bbraft-node ready"}

var jailKVMMsg = regexp.MustCompile(`(?:^| )msg=(?:"((?:[^"\\]|\\.)*)"|(\S+))`)

// jailKVMCheckStartupLog checks one InvocationID's journal lines (slog text
// format): jail admitted -> cpu realized (jailed=true) -> vm networking ready
// -> node: qemu-img self-check passed -> bbraft-node ready, each exactly
// once and in that order, with no unjailed "cpu realized" and no user-mode
// networking line.
func jailKVMCheckStartupLog(lines []string) error {
	var seen []int
	for _, line := range lines {
		m := jailKVMMsg.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		msg := m[1] + m[2]
		if strings.HasPrefix(msg, "vm networking: QEMU user-mode") {
			return fmt.Errorf("startup log: user-mode networking: %q", line)
		}
		i := slices.Index(jailKVMStartupPhases, msg)
		if i < 0 {
			continue
		}
		if msg == "cpu realized" && !slices.Contains(strings.Fields(line), "jailed=true") {
			return fmt.Errorf("startup log: unjailed cpu probe: %q", line)
		}
		seen = append(seen, i)
	}
	want := []int{0, 1, 2, 3, 4}
	if !slices.Equal(seen, want) {
		names := make([]string, len(seen))
		for i, p := range seen {
			names[i] = jailKVMStartupPhases[p]
		}
		return fmt.Errorf("startup log phases %q, want exactly %q in order", names, jailKVMStartupPhases)
	}
	return nil
}

var jailKVMCIDAttr = regexp.MustCompile(`(?:^| )cid=(\S+)`)

// jailKVMStartupExcludeMsg is startupLocalCID's log message.
const jailKVMStartupExcludeMsg = "node: excluding local vsock cid from allocation"

// jailKVMCheckStartupCID checks one invocation's journal against the local
// CID the harness probed itself (probeLocalVsockCID as root, outside the
// unit's device policy; 0 when /dev/vsock is absent). When that CID is in
// the node's default allocation range (DefaultMinCID..DefaultMaxCID) the
// exclusion must be logged exactly once, naming that CID, before
// "bbraft-node ready"; otherwise it must not be logged at all.
func jailKVMCheckStartupCID(lines []string, local uint32) error {
	var cids []string
	ready := -1
	for i, line := range lines {
		m := jailKVMMsg.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		switch m[1] + m[2] {
		case "bbraft-node ready":
			if ready < 0 {
				ready = i
			}
		case jailKVMStartupExcludeMsg:
			if ready >= 0 {
				return fmt.Errorf("startup log: CID exclusion after ready: %q", line)
			}
			c := jailKVMCIDAttr.FindStringSubmatch(line)
			if c == nil {
				return fmt.Errorf("startup log: CID exclusion without a cid: %q", line)
			}
			cids = append(cids, c[1])
		}
	}
	if ready < 0 {
		return errors.New("startup log: no bbraft-node ready")
	}
	var want []string
	if local >= DefaultMinCID && local <= DefaultMaxCID {
		want = []string{strconv.FormatUint(uint64(local), 10)}
	}
	if !slices.Equal(cids, want) {
		return fmt.Errorf("startup log: excluded CIDs %q, want %q (probed local CID %d, range %d..%d)", cids, want, local, DefaultMinCID, DefaultMaxCID)
	}
	return nil
}

// jailKVMIdentity is a filesystem, namespace or cgroup inode identity.
type jailKVMIdentity struct{ Dev, Ino uint64 }

// jailKVMOwned is one object this attempt created, with the identity
// captured right after creating it. Kind is "file", "dir", "netns" or
// "cgroup"; Path is absolute.
type jailKVMOwned struct {
	Kind, Path string
	ID         jailKVMIdentity
}

// jailKVMObserved is the cleanup-time observation of one owned object.
type jailKVMObserved struct {
	Exists bool
	ID     jailKVMIdentity
	Err    error
}

// jailKVMTeardown is everything the cleanup decision needs.
type jailKVMTeardown struct {
	// UnitStopped is proven (ActiveState inactive or failed, no MainPID).
	UnitStopped bool
	// SubtreeEmpty is the result of jailKVMSubtreeEmpty on the unit cgroup.
	SubtreeEmpty error
	// WorkersDone is the bounded join of every registered listener worker
	// (jailKVMListenWorker.Wait); a worker that has not finished may still
	// hold the namespace, so nothing is removed.
	WorkersDone error
	// Recovery is the outcome of the one bounded restart+API recovery run
	// when the stopped unit's subtree still held processes (nil when it did
	// not run or succeeded); any failure preserves everything.
	Recovery error
	// Owned is in creation order; Observed[i] belongs to Owned[i].
	Owned    []jailKVMOwned
	Observed []jailKVMObserved
}

// jailKVMPlan is the cleanup decision: either remove exactly Remove (in
// reverse creation order, only present still-matching objects) or preserve
// everything for inspection with the reasons.
type jailKVMPlan struct {
	Remove   []jailKVMOwned
	Preserve []string
}

// jailKVMCleanupPlan decides cleanup all-or-nothing: any unproven stop, a
// populated or unreadable subtree, an identity mismatch or an unreadable
// object preserves everything. A recorded object that is already absent is
// not removed and is not uncertainty.
func jailKVMCleanupPlan(td jailKVMTeardown) jailKVMPlan {
	var why []string
	if !td.UnitStopped {
		why = append(why, "the unit is not proven stopped")
	}
	if td.SubtreeEmpty != nil {
		why = append(why, "the unit cgroup subtree is not proven empty: "+td.SubtreeEmpty.Error())
	}
	if td.WorkersDone != nil {
		why = append(why, "a netns listener worker is not proven finished: "+td.WorkersDone.Error())
	}
	if td.Recovery != nil {
		why = append(why, "the teardown recovery failed: "+td.Recovery.Error())
	}
	if len(td.Observed) != len(td.Owned) {
		why = append(why, fmt.Sprintf("%d observations for %d owned objects", len(td.Observed), len(td.Owned)))
	}
	var remove []jailKVMOwned
	for i := len(td.Owned) - 1; i >= 0 && i < len(td.Observed); i-- {
		o, obs := td.Owned[i], td.Observed[i]
		switch {
		case obs.Err != nil:
			why = append(why, fmt.Sprintf("%s: unreadable: %v", o.Path, obs.Err))
		case !obs.Exists:
		case obs.ID != o.ID:
			why = append(why, fmt.Sprintf("%s: identity %v, recorded %v", o.Path, obs.ID, o.ID))
		default:
			remove = append(remove, o)
		}
	}
	if len(why) > 0 {
		return jailKVMPlan{Preserve: why}
	}
	return jailKVMPlan{Remove: remove}
}

// jailKVMListingSlack is how many entries (the cgroup interface files) one
// cgroup directory may list beyond maxCgroups; a larger listing is refused,
// never accepted as a partial page.
const jailKVMListingSlack = 256

// jailKVMReadBounded reads r to EOF, keeping at most limit bytes: more is an
// error, and so is any read error other than EOF (never a short success).
func jailKVMReadBounded(r io.Reader, limit int) (string, error) {
	buf := make([]byte, limit+1)
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if n > limit {
			// Checked before EOF: a reader may return its last data and EOF
			// together.
			return "", fmt.Errorf("more than %d bytes", limit)
		}
		if errors.Is(err, io.EOF) {
			return string(buf[:n]), nil
		}
		if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("more than %d bytes", limit)
}

// jailKVMEvidenceLimit bounds one cgroup pseudo-file read.
const jailKVMEvidenceLimit = 64 << 10

// jailKVMReadEvidence reads the regular file name under dirfd without
// following a symlink and without blocking, bounded to jailKVMEvidenceLimit
// bytes (more is an error).
func jailKVMReadEvidence(dirfd int, display, name string) (string, error) {
	fd, err := unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", fmt.Errorf("cgroup %s: open %s: %w", display, name, err)
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
		return "", fmt.Errorf("cgroup %s: %s is not a regular file (%v)", display, name, err)
	}
	data, err := jailKVMReadBounded(f, jailKVMEvidenceLimit)
	if err != nil {
		return "", fmt.Errorf("cgroup %s: read %s: %w", display, name, err)
	}
	return data, nil
}

// errJailKVMPopulated marks a well-formed cgroup that holds processes, as
// opposed to a malformed, unreadable or unbounded subtree.
var errJailKVMPopulated = errors.New("the subtree holds processes")

// jailKVMSubtreeEmpty proves a cgroup v2 subtree holds no process: the root
// and every descendant cgroup report exactly one well-formed "populated 0"
// line in cgroup.events and an empty cgroup.procs. Directories and evidence
// files are opened without following symlinks (a symlinked descendant or
// pseudo-file is refused) and each opened directory must be the inode its
// name had; the walk checks ctx before each cgroup and is bounded to
// maxCgroups directories (more is an error).
func jailKVMSubtreeEmpty(ctx context.Context, root string, maxCgroups int) error {
	dirFlags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	rootFD, err := unix.Open(root, dirFlags, 0)
	if err != nil {
		return fmt.Errorf("cgroup %s: %w", root, err)
	}
	type dir struct {
		fd      int
		display string
	}
	queue := []dir{{rootFD, root}}
	defer func() {
		for _, d := range queue {
			unix.Close(d.fd)
		}
	}()
	firstPopulated := ""
	for n := 0; len(queue) > 0; n++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cgroup subtree %s: %w", root, context.Cause(ctx))
		}
		if n >= maxCgroups {
			return fmt.Errorf("cgroup subtree %s has more than %d cgroups", root, maxCgroups)
		}
		d := queue[0]
		err := func() error {
			events, err := jailKVMReadEvidence(d.fd, d.display, "cgroup.events")
			if err != nil {
				return err
			}
			var populated []string
			for line := range strings.SplitSeq(events, "\n") {
				fields := strings.Fields(line)
				if len(fields) == 0 || fields[0] != "populated" {
					continue
				}
				if len(fields) != 2 || (fields[1] != "0" && fields[1] != "1") {
					return fmt.Errorf("cgroup %s: %q", d.display, line)
				}
				populated = append(populated, fields[1])
			}
			if len(populated) != 1 {
				return fmt.Errorf("cgroup %s: %d populated lines in cgroup.events, want exactly one", d.display, len(populated))
			}
			procs, err := jailKVMReadEvidence(d.fd, d.display, "cgroup.procs")
			if err != nil {
				return err
			}
			pids := strings.Fields(procs)
			for i, pid := range pids {
				if n, err := strconv.ParseUint(pid, 10, 32); err != nil || n == 0 || strconv.FormatUint(n, 10) != pid || slices.Contains(pids[:i], pid) {
					return fmt.Errorf("cgroup %s: malformed cgroup.procs %q", d.display, strings.TrimSpace(procs))
				}
			}
			// Populated evidence is only collected here: the walk goes on,
			// so malformed or unreadable evidence anywhere still wins.
			if populated[0] == "1" && firstPopulated == "" {
				firstPopulated = d.display + ": populated 1"
			}
			if len(pids) > 0 && firstPopulated == "" {
				firstPopulated = fmt.Sprintf("%s holds %q", d.display, strings.Join(pids, " "))
			}
			listFD, err := unix.Dup(d.fd)
			if err != nil {
				return err
			}
			list := os.NewFile(uintptr(listFD), d.display)
			// The complete listing, page by page to EOF; over the bound is
			// refused.
			var entries []os.DirEntry
			for {
				page, err := list.ReadDir(256)
				entries = append(entries, page...)
				if len(entries) > maxCgroups+jailKVMListingSlack {
					list.Close()
					return fmt.Errorf("cgroup %s lists more than %d entries", d.display, maxCgroups+jailKVMListingSlack)
				}
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					list.Close()
					return fmt.Errorf("cgroup %s: list: %w", d.display, err)
				}
			}
			list.Close()
			for _, e := range entries {
				var st unix.Stat_t
				if err := unix.Fstatat(d.fd, e.Name(), &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
					return fmt.Errorf("cgroup %s: %s: %w", d.display, e.Name(), err)
				}
				switch st.Mode & unix.S_IFMT {
				case unix.S_IFLNK:
					return fmt.Errorf("cgroup %s: symlink %s", d.display, e.Name())
				case unix.S_IFDIR:
					fd, err := unix.Openat(d.fd, e.Name(), dirFlags, 0)
					if err != nil {
						return fmt.Errorf("cgroup %s: %s: %w", d.display, e.Name(), err)
					}
					var opened unix.Stat_t
					if err := unix.Fstat(fd, &opened); err != nil || opened.Dev != st.Dev || opened.Ino != st.Ino {
						unix.Close(fd)
						return fmt.Errorf("cgroup %s: %s changed while it was opened", d.display, e.Name())
					}
					queue = append(queue, dir{fd, filepath.Join(d.display, e.Name())})
				}
			}
			return nil
		}()
		unix.Close(d.fd)
		queue = queue[1:]
		if err != nil {
			return err
		}
	}
	// Only a completely valid, bounded subtree reports "populated".
	if firstPopulated != "" {
		return fmt.Errorf("cgroup %s: %w", firstPopulated, errJailKVMPopulated)
	}
	return nil
}

// jailKVMNetnsOps are the namespace operations of jailKVMListenInNetns, so
// the restore-failure path is testable without privilege.
type jailKVMNetnsOps struct {
	// OpenCurrent opens /proc/thread-self/ns/net of the calling (locked)
	// thread; OpenTarget opens the target namespace path.
	OpenCurrent func() (fd int, err error)
	OpenTarget  func(path string) (fd int, err error)
	Setns       func(fd int) error
	Close       func(fd int) error
	Listen      func(network, addr string) (net.Listener, error)
	// Unlock is runtime.UnlockOSThread; it runs only after a proven restore.
	Unlock func()
	// BeforeSelect, when set, runs in the caller just before it waits for
	// the worker (tests make the result and an ended ctx ready together).
	BeforeSelect func(workerDone <-chan struct{})
}

// jailKVMRealNetnsOps is jailKVMNetnsOps on the real kernel.
func jailKVMRealNetnsOps() jailKVMNetnsOps {
	open := func(p string) (int, error) { return unix.Open(p, unix.O_RDONLY|unix.O_CLOEXEC, 0) }
	return jailKVMNetnsOps{
		OpenCurrent: func() (int, error) { return open("/proc/thread-self/ns/net") },
		OpenTarget:  open,
		Setns:       func(fd int) error { return unix.Setns(fd, unix.CLONE_NEWNET) },
		Close:       unix.Close,
		Listen:      net.Listen,
		Unlock:      runtime.UnlockOSThread,
	}
}

// jailKVMListenWorker owns the goroutine of one jailKVMListenInNetns call.
// Done closes only after the worker closed both namespace descriptors and,
// when the caller had already given up, closed any late listener. The
// attempt registers the worker before proceeding; cleanup must Wait for it
// (bounded) before deleting the namespace, and preserve the attempt if it
// does not finish.
type jailKVMListenWorker struct {
	done      chan struct{}
	mu        sync.Mutex
	abandoned bool
	result    chan jailKVMListenResult
	err       error
}

type jailKVMListenResult struct {
	lis net.Listener
	err error
}

// Done is closed when the worker has fully finished.
func (w *jailKVMListenWorker) Done() <-chan struct{} { return w.done }

// Wait waits up to timeout for the worker; an error means its completion is
// unproven (the namespace must be preserved).
func (w *jailKVMListenWorker) Wait(timeout time.Duration) error {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-w.done:
		return nil
	case <-t.C:
		return fmt.Errorf("netns listener worker did not finish within %v", timeout)
	}
}

// Err is the worker's own outcome once Done is closed (nil after success).
func (w *jailKVMListenWorker) Err() error {
	select {
	case <-w.done:
		return w.err
	default:
		return errors.New("netns listener worker is still running")
	}
}

// jailKVMListenInNetns creates a TCP listener inside the namespace at path
// on a dedicated locked goroutine: capture the thread's namespace, setns to
// the target, listen, setns back. Only a proven restore unlocks the thread;
// a failed restore closes the listener and returns from the still-locked
// goroutine so Go discards that thread. A ctx already ended starts nothing.
// The call is bounded by timeout; cancellation and the timeout keep their
// identity (errors.Is context.Canceled / context.DeadlineExceeded), a
// listener produced after the caller gave up is closed by the worker, and a
// listener whose ctx ended just before returning is closed. The returned
// worker is never nil.
func jailKVMListenInNetns(ctx context.Context, path, addr string, timeout time.Duration, ops jailKVMNetnsOps) (net.Listener, *jailKVMListenWorker, error) {
	w := &jailKVMListenWorker{done: make(chan struct{}), result: make(chan jailKVMListenResult, 1)}
	if err := ctx.Err(); err != nil {
		w.err = context.Cause(ctx)
		close(w.done)
		return nil, w, fmt.Errorf("netns: listen in %s: %w", path, context.Cause(ctx))
	}
	ctx, cancel := context.WithTimeoutCause(ctx, timeout,
		fmt.Errorf("netns: listening in %s did not finish within %v: %w", path, timeout, context.DeadlineExceeded))
	defer cancel()
	go func() {
		r := jailKVMListenWork(path, addr, ops) // closes both namespace fds before returning
		w.mu.Lock()
		w.err = r.err
		if w.abandoned {
			if r.lis != nil {
				_ = r.lis.Close()
			}
		} else {
			w.result <- r
		}
		w.mu.Unlock()
		close(w.done)
	}()
	if ops.BeforeSelect != nil {
		ops.BeforeSelect(w.done)
	}
	select {
	case r := <-w.result:
		<-w.done
		if r.err != nil {
			return nil, w, r.err
		}
		if err := ctx.Err(); err != nil {
			_ = r.lis.Close()
			return nil, w, fmt.Errorf("netns: listen in %s: %w", path, context.Cause(ctx))
		}
		return r.lis, w, nil
	case <-ctx.Done():
		w.mu.Lock()
		w.abandoned = true
		w.mu.Unlock()
		select {
		case r := <-w.result:
			if r.lis != nil {
				_ = r.lis.Close()
			}
		default:
		}
		return nil, w, fmt.Errorf("netns: listen in %s: %w", path, context.Cause(ctx))
	}
}

// jailKVMListenWork is the locked-thread body of jailKVMListenInNetns.
func jailKVMListenWork(path, addr string, ops jailKVMNetnsOps) jailKVMListenResult {
	runtime.LockOSThread()
	cur, err := ops.OpenCurrent()
	if err != nil {
		ops.Unlock()
		return jailKVMListenResult{err: fmt.Errorf("netns: open the thread namespace: %w", err)}
	}
	defer ops.Close(cur)
	target, err := ops.OpenTarget(path)
	if err != nil {
		ops.Unlock()
		return jailKVMListenResult{err: fmt.Errorf("netns: open %s: %w", path, err)}
	}
	defer ops.Close(target)
	if err := ops.Setns(target); err != nil {
		if rerr := ops.Setns(cur); rerr != nil {
			// Thread state unknown: return still locked.
			return jailKVMListenResult{err: errors.Join(fmt.Errorf("netns: enter %s: %w", path, err), fmt.Errorf("netns: restore: %w", rerr))}
		}
		ops.Unlock()
		return jailKVMListenResult{err: fmt.Errorf("netns: enter %s: %w", path, err)}
	}
	lis, lerr := ops.Listen("tcp", addr)
	if rerr := ops.Setns(cur); rerr != nil {
		if lis != nil {
			_ = lis.Close()
		}
		// The thread is in the target namespace: return still locked so the
		// runtime discards it.
		return jailKVMListenResult{err: fmt.Errorf("netns: restore after listening: %w", rerr)}
	}
	ops.Unlock()
	if lerr != nil {
		return jailKVMListenResult{err: fmt.Errorf("netns: listen %s: %w", addr, lerr)}
	}
	return jailKVMListenResult{lis: lis}
}

// jailKVMCheckProbeStatus checks a cross-uid probe's /proc/self/status after
// its root re-exec dropped the bounding set, groups, gid and uid: Uid and
// Gid all equal uid, Groups empty, CapInh/CapPrm/CapEff/CapBnd/CapAmb zero
// and NoNewPrivs 1.
func jailKVMCheckProbeStatus(status string, uid uint32) error {
	fields := map[string]string{}
	for line := range strings.SplitSeq(status, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			fields[k] = strings.TrimSpace(v)
		}
	}
	want := strconv.FormatUint(uint64(uid), 10)
	for _, key := range []string{"Uid", "Gid"} {
		ids := strings.Fields(fields[key])
		if len(ids) != 4 || slices.ContainsFunc(ids, func(s string) bool { return s != want }) {
			return fmt.Errorf("probe %s = %q, want all %s", key, fields[key], want)
		}
	}
	if g, ok := fields["Groups"]; !ok || g != "" {
		return fmt.Errorf("probe Groups = %q (present %v), want empty", g, ok)
	}
	if fields["NoNewPrivs"] != "1" {
		return fmt.Errorf("probe NoNewPrivs = %q, want 1", fields["NoNewPrivs"])
	}
	for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
		v, err := strconv.ParseUint(fields[key], 16, 64)
		if err != nil || v != 0 {
			return fmt.Errorf("probe %s = %q, want 0", key, fields[key])
		}
	}
	return nil
}

// jailKVMProbeCase is one cross-uid access check. Root pins Path as an
// O_PATH descriptor (proving it exists) and passes only that descriptor to
// the probe; after dropping privilege the probe reopens /proc/self/fd/N with
// Op's access ("reopen-rw", "reopen-r" or "reopen-dir"), so the kernel checks
// the target inode's own permission for As without walking (or chmod'ing)
// its 0700 ancestors, and no privileged open file description is reused.
// Want is "ok", "EACCES" or "EPERM".
type jailKVMProbeCase struct {
	As   uint32
	Op   string
	Path string
	Want string
}

// jailKVMQEMU is one jailed QEMU as observed from root, from its durable
// jail record cross-checked with the VM manifest and cgroup membership.
type jailKVMQEMU struct {
	Sandbox     string
	Attempt     string
	UID         uint32
	PID         int
	StartTicks  uint64
	Cgroup      string
	Overlay     string
	ControlDir  string
	JailRoot    string
	OverlayID   jailKVMIdentity
	ControlDirI jailKVMIdentity
	Resources   storage.Resources
}

// jailKVMCrossUIDCases is the nonvacuous cross-uid table for two VMs: A's
// uid reopens its own overlay read-write and its control dir (positive
// controls), B's uid gets EACCES on A's overlay (rw and r) and control dir,
// and the same for the reverse.
func jailKVMCrossUIDCases(a, b jailKVMQEMU) []jailKVMProbeCase {
	pair := func(own, other jailKVMQEMU) []jailKVMProbeCase {
		return []jailKVMProbeCase{
			{As: own.UID, Op: "reopen-rw", Path: own.Overlay, Want: "ok"},
			{As: own.UID, Op: "reopen-dir", Path: own.ControlDir, Want: "ok"},
			{As: other.UID, Op: "reopen-rw", Path: own.Overlay, Want: "EACCES"},
			{As: other.UID, Op: "reopen-r", Path: own.Overlay, Want: "EACCES"},
			{As: other.UID, Op: "reopen-dir", Path: own.ControlDir, Want: "EACCES"},
		}
	}
	return append(pair(a, b), pair(b, a)...)
}

// jailKVMCheckAdopted proves restart adoption: the same pid, start ticks,
// uid and cgroup before and after.
func jailKVMCheckAdopted(before, after jailKVMQEMU) error {
	if before.PID == 0 || before.PID != after.PID || before.StartTicks != after.StartTicks ||
		before.UID != after.UID || before.Cgroup != after.Cgroup {
		return fmt.Errorf("restart did not adopt the same QEMU: before %+v, after %+v", before, after)
	}
	return nil
}

// jailKVMCheckNewLink proves the restart rebound to a new link: the old
// link's Done is closed and the InvocationID changed (both nonempty).
func jailKVMCheckNewLink(oldDone <-chan struct{}, oldInvocation, newInvocation string) error {
	select {
	case <-oldDone:
	default:
		return errors.New("the old link is still open after the restart")
	}
	if oldInvocation == "" || newInvocation == "" || oldInvocation == newInvocation {
		return fmt.Errorf("InvocationID %q -> %q: the restart did not start a new invocation", oldInvocation, newInvocation)
	}
	return nil
}

// jailKVMProcSample is one observation of a unit's main process: the unit's
// InvocationID and MainPID, the identity and link text of /proc/<pid>/exe,
// the process start time (/proc/<pid>/stat field 22) and, when read, its
// /proc/<pid>/status fields. Err says why the process facts were unreadable.
type jailKVMProcSample struct {
	Invocation string
	PID        int
	Exe        jailKVMIdentity
	ExeLink    string
	StartTime  string
	Status     map[string]string
	Err        error
}

// jailKVMMainOps reads the facts jailKVMAwaitMainExe samples.
type jailKVMMainOps struct {
	Show      func(ctx context.Context) (inv string, pid int, err error)
	Exe       func(pid int) (jailKVMIdentity, string, error)
	StartTime func(pid int) (string, error)
	Status    func(pid int) (string, error)
}

// jailKVMMainProof is the evidence of a main process that runs the staged
// binary: the first sample (typically systemd-executor before its execve)
// and the matched one, whose Status is the only one ever asserted.
type jailKVMMainProof struct {
	Initial, Matched jailKVMProcSample
	Samples          int
}

// String describes a sample for gate diagnostics (no status values beyond
// identity and capabilities).
func (s jailKVMProcSample) String() string {
	return fmt.Sprintf("inv=%s pid=%d exe=%q dev=%d ino=%d start=%s CapBnd=%s CapPrm=%s CapEff=%s NoNewPrivs=%s err=%v",
		s.Invocation, s.PID, s.ExeLink, s.Exe.Dev, s.Exe.Ino, s.StartTime,
		s.Status["CapBnd"], s.Status["CapPrm"], s.Status["CapEff"], s.Status["NoNewPrivs"], s.Err)
}

// jailKVMAwaitMainExe waits (bounded by timeout) until the unit's main
// process runs the staged binary: with Type=simple, systemctl start returns
// at fork, and the MainPID first runs systemd-executor, which applies the
// sandbox and only then execs ExecStart. Before a match a sample is only
// recorded (the first one with its status, as evidence of the executor);
// nothing about it is asserted. On a match the status is read and the
// InvocationID, MainPID, exe identity and start time are re-proven after
// it, so the returned Matched.Status belongs to the staged binary's process.
// A changed InvocationID or a MainPID of 0 is refused at once; a
// never-matching exe ends in a timeout naming the initial and last samples.
func jailKVMAwaitMainExe(ctx context.Context, wantInv string, want jailKVMIdentity, timeout, interval time.Duration, ops jailKVMMainOps) (jailKVMMainProof, error) {
	var proof jailKVMMainProof
	if err := ctx.Err(); err != nil {
		return proof, fmt.Errorf("main process: %w", context.Cause(ctx))
	}
	ctx, cancel := context.WithTimeoutCause(ctx, timeout,
		fmt.Errorf("main process: /proc/<MainPID>/exe did not become the staged binary within %v: %w", timeout, context.DeadlineExceeded))
	defer cancel()
	sample := func() (jailKVMProcSample, error) {
		inv, pid, err := ops.Show(ctx)
		if err != nil {
			return jailKVMProcSample{}, fmt.Errorf("main process: show: %w", err)
		}
		s := jailKVMProcSample{Invocation: inv, PID: pid}
		if inv != wantInv {
			return s, fmt.Errorf("main process: InvocationID %q, want %q (the unit restarted)", inv, wantInv)
		}
		if pid <= 0 {
			return s, fmt.Errorf("main process: invocation %s has no MainPID (it exited)", inv)
		}
		if s.Exe, s.ExeLink, s.Err = ops.Exe(pid); s.Err == nil {
			s.StartTime, s.Err = ops.StartTime(pid)
		}
		return s, nil
	}
	var last jailKVMProcSample
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		s, err := sample()
		if err != nil {
			return proof, fmt.Errorf("%w (initial %v)", err, proof.Initial)
		}
		proof.Samples++
		matched := s.Err == nil && s.Exe == want
		if proof.Samples == 1 {
			// Evidence only: the status of whatever runs first.
			if st, err := ops.Status(s.PID); err == nil {
				s.Status = jailKVMParseStatus(st)
			}
			proof.Initial = s
		}
		if matched {
			st, err := ops.Status(s.PID)
			if err != nil {
				return proof, fmt.Errorf("main process: status of %v: %w", s, err)
			}
			after, err := sample()
			if err != nil {
				return proof, fmt.Errorf("%w (after the status read of %v)", err, s)
			}
			if after.Err != nil || after.PID != s.PID || after.Exe != s.Exe || after.StartTime != s.StartTime {
				return proof, fmt.Errorf("main process changed around its status read: before %v, after %v", s, after)
			}
			s.Status = jailKVMParseStatus(st)
			proof.Matched = s
			return proof, nil
		}
		last = s
		select {
		case <-ctx.Done():
			return proof, fmt.Errorf("%w (initial %v; last %v)", context.Cause(ctx), proof.Initial, last)
		case <-tick.C:
		}
	}
}

// jailKVMParseStatus splits /proc/<pid>/status into its fields.
func jailKVMParseStatus(s string) map[string]string {
	out := map[string]string{}
	for line := range strings.SplitSeq(s, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			out[k] = strings.TrimSpace(v)
		}
	}
	return out
}

// jailKVMBoundaryCapMask is jailKVMBoundaryCaps as a capability bit mask.
func jailKVMBoundaryCapMask() uint64 {
	var mask uint64
	for _, c := range []int{unix.CAP_CHOWN, unix.CAP_DAC_OVERRIDE, unix.CAP_FOWNER, unix.CAP_KILL, unix.CAP_MKNOD, unix.CAP_NET_ADMIN,
		unix.CAP_SETGID, unix.CAP_SETPCAP, unix.CAP_SETUID, unix.CAP_SYS_ADMIN, unix.CAP_SYS_CHROOT, unix.CAP_SYS_PTRACE} {
		mask |= 1 << c
	}
	return mask
}

// jailKVMCheckMainStatus checks the matched node main process's status: a
// root process exec'ed under the bounding set holds exactly it (bounding,
// permitted and effective; the node never drops its own), nothing
// inheritable or ambient, NoNewPrivs set and every Uid 0.
func jailKVMCheckMainStatus(st map[string]string) error {
	mask := jailKVMBoundaryCapMask()
	for key, want := range map[string]uint64{"CapBnd": mask, "CapPrm": mask, "CapEff": mask, "CapInh": 0, "CapAmb": 0} {
		v, ok := st[key]
		got, err := strconv.ParseUint(v, 16, 64)
		if !ok || err != nil || got != want {
			return fmt.Errorf("node main %s = %q, want %016x", key, v, want)
		}
	}
	if st["NoNewPrivs"] != "1" {
		return fmt.Errorf("node main NoNewPrivs %q, want 1", st["NoNewPrivs"])
	}
	if uid := strings.Fields(st["Uid"]); len(uid) != 4 || slices.ContainsFunc(uid, func(u string) bool { return u != "0" }) {
		return fmt.Errorf("node main Uid %q, want root (0 0 0 0)", st["Uid"])
	}
	return nil
}

// jailKVMStatStartTime returns the start time (field 22) of a
// /proc/<pid>/stat line. The comm (field 2) is parenthesized and may itself
// contain ") ", so the fields are counted after its last ") ".
func jailKVMStatStartTime(stat string) (string, error) {
	_, rest, ok := strings.CutLast(stat, ") ")
	if !ok {
		return "", errors.New("/proc stat: no comm")
	}
	f := strings.Fields(rest)
	if len(f) < 20 {
		return "", fmt.Errorf("/proc stat: %d fields after comm, want at least 20", len(f))
	}
	if _, err := strconv.ParseUint(f[19], 10, 64); err != nil {
		return "", fmt.Errorf("/proc stat: start time %q: %w", f[19], err)
	}
	return f[19], nil
}

// jailKVMReadyOps reads what jailKVMAwaitReady polls: the unit's current
// InvocationID, ActiveState and NRestarts, and one invocation's journal.
type jailKVMReadyOps struct {
	Show    func(ctx context.Context) (inv, state, restarts string, err error)
	Journal func(ctx context.Context, inv string) ([]string, error)
}

// jailKVMAwaitReady polls (bounded by timeout) until the journal of exactly
// invocation inv carries "bbraft-node ready", and returns that journal.
// Every Show must still be inv, with the NRestarts of the first Show and an
// ActiveState of activating or active: Restart=on-failure brings a crashed
// node back as another invocation of an "active" unit, which is refused at
// once instead of being polled until the deadline. A ready journal counts
// only when a Show after reading it still confirms inv. Journal read errors
// are retried; the deadline and every refusal carry inv's journal tail.
func jailKVMAwaitReady(ctx context.Context, inv string, timeout, interval time.Duration, ops jailKVMReadyOps) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("startup: %w", context.Cause(ctx))
	}
	ctx, cancel := context.WithTimeoutCause(ctx, timeout,
		fmt.Errorf("startup: invocation %s did not log bbraft-node ready within %v: %w", inv, timeout, context.DeadlineExceeded))
	defer cancel()
	var lines []string
	tail := func() string { return strings.Join(lines[max(0, len(lines)-40):], "\n") }
	baseline := ""
	confirm := func() error {
		gotInv, state, restarts, err := ops.Show(ctx)
		if err != nil {
			return fmt.Errorf("startup: show: %w", err)
		}
		if _, perr := strconv.ParseUint(restarts, 10, 32); perr != nil {
			return fmt.Errorf("startup: NRestarts %q: %w", restarts, perr)
		}
		if baseline == "" {
			baseline = restarts
		}
		switch {
		case gotInv != inv:
			return fmt.Errorf("startup: the unit is now invocation %s (NRestarts %s), not %s: it restarted", gotInv, restarts, inv)
		case restarts != baseline:
			return fmt.Errorf("startup: NRestarts went from %s to %s during invocation %s", baseline, restarts, inv)
		case state != "active" && state != "activating":
			return fmt.Errorf("startup: invocation %s is %s before becoming ready", inv, state)
		}
		return nil
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if err := confirm(); err != nil {
			return nil, fmt.Errorf("%w\n%s", err, tail())
		}
		if got, err := ops.Journal(ctx, inv); err == nil {
			lines = got
			if slices.ContainsFunc(lines, func(l string) bool { return strings.Contains(l, `msg="bbraft-node ready"`) }) {
				if err := confirm(); err != nil {
					return nil, fmt.Errorf("%w (after its ready line)\n%s", err, tail())
				}
				return lines, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w\n%s", context.Cause(ctx), tail())
		case <-tick.C:
		}
	}
}

// jailKVMWaitNewLink returns the node's next link that is not old (nil old
// takes the first). nodelink.Hub.Wait returns the current map entry, which
// right after a restart may still be the old link: the waiter then waits
// for the old link's Done, backs off and asks again, bounded by ctx.
func jailKVMWaitNewLink(ctx context.Context, old <-chan struct{}, wait func(context.Context) (jailKVMLink, error), backoff time.Duration) (jailKVMLink, error) {
	for {
		if err := ctx.Err(); err != nil {
			return jailKVMLink{}, fmt.Errorf("no new link: %w", context.Cause(ctx))
		}
		l, err := wait(ctx)
		if err != nil {
			return jailKVMLink{}, err
		}
		if old == nil || l.Done != old {
			// Never hand over a link once ctx ended.
			if err := ctx.Err(); err != nil {
				return jailKVMLink{}, fmt.Errorf("no new link: %w", context.Cause(ctx))
			}
			return l, nil
		}
		select {
		case <-old:
		case <-ctx.Done():
			return jailKVMLink{}, fmt.Errorf("the old link stayed open: %w", context.Cause(ctx))
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return jailKVMLink{}, fmt.Errorf("no new link: %w", context.Cause(ctx))
		}
	}
}

// jailKVMRecoverOps are the steps of the teardown's one recovery.
type jailKVMRecoverOps struct {
	Start   func(ctx context.Context) error
	Connect func(ctx context.Context) (nodev1.NodeClient, error)
	Stop    func(ctx context.Context) (stopped bool, err error)
	Subtree func(ctx context.Context) error
}

// jailKVMRecoverOnce is the teardown's one bounded recovery for a stopped
// unit whose subtree still holds processes (KillMode=process leaves jailed
// QEMUs running): start the unit, reach it through a new connection, delete
// every VM through the API and prove the list empty; then, whatever failed,
// stop the unit again and re-prove the stop and the subtree. It returns the
// new stop proof, the new subtree result and every failure joined.
func jailKVMRecoverOnce(ctx context.Context, ops jailKVMRecoverOps) (stopped bool, subtree, err error) {
	if err := ctx.Err(); err != nil {
		return false, nil, fmt.Errorf("recovery: %w", context.Cause(ctx))
	}
	var errs []error
	if err := ops.Start(ctx); err != nil {
		errs = append(errs, fmt.Errorf("recovery: start: %w", err))
	} else if client, err := ops.Connect(ctx); err != nil {
		errs = append(errs, fmt.Errorf("recovery: connect: %w", err))
	} else if err := jailKVMDeleteAll(ctx, client); err != nil {
		errs = append(errs, fmt.Errorf("recovery: %w", err))
	}
	stopped, serr := ops.Stop(ctx)
	if serr != nil {
		errs = append(errs, fmt.Errorf("recovery: stop: %w", serr))
	}
	return stopped, ops.Subtree(ctx), errors.Join(errs...)
}

// jailKVMDeleteAll deletes every listed VM through the API and proves the
// list empty afterwards.
func jailKVMDeleteAll(ctx context.Context, c nodev1.NodeClient) error {
	list, err := c.ListVMs(ctx, &nodev1.ListVMsRequest{})
	if err != nil {
		return fmt.Errorf("ListVMs: %w", err)
	}
	var errs []error
	for _, vm := range list.GetVms() {
		if _, err := c.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: vm.GetSandboxId()}); err != nil {
			errs = append(errs, fmt.Errorf("DeleteVM %s: %w", vm.GetSandboxId(), err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	after, err := c.ListVMs(ctx, &nodev1.ListVMsRequest{})
	if err != nil {
		return fmt.Errorf("ListVMs after delete: %w", err)
	}
	if n := len(after.GetVms()); n != 0 {
		return fmt.Errorf("the node still lists %d VMs after deleting them", n)
	}
	return nil
}

// jailKVMFD is one observed descriptor of a jailed QEMU: its number, the
// readlink of /proc/<pid>/fd/N, and the stat through that link.
type jailKVMFD struct {
	Num  int
	Link string
	ID   jailKVMIdentity
	Mode uint32
	Rdev uint64
}

// jailKVMFDPolicy is the fixed descriptor map a jailed QEMU must hold and
// what it must never hold.
type jailKVMFDPolicy struct {
	// Rdev maps an fd to the character device it must be (tap, vhost-net,
	// vhost-vsock).
	Rdev map[int]uint64
	// ID maps an fd to the exact file it must be (kernel, initrd, base,
	// overlay).
	ID map[int]jailKVMIdentity
	// ForbiddenIDs are host objects no jailed descriptor may be (the data
	// root, the jailer state, records and jails dirs, the config dir and
	// token).
	ForbiddenIDs map[jailKVMIdentity]string
	// ForbiddenDev is the cgroup2 filesystem: no descriptor may live on it.
	ForbiddenDev uint64
	// NullRdev is /dev/null's rdev: stdin must be /dev/null, stdout and
	// stderr /dev/null or a pipe (the inherited stdio). No other pipe may
	// exist: the setup's release and status pipes were its only pipes.
	NullRdev uint64
	MaxFDs   int
}

// jailKVMCheckRotatedHostKey requires both accesses to carry a valid
// ed25519 OpenSSH host key and the rotated one to differ from the old one
// after canonicalization (comments and spacing ignored).
func jailKVMCheckRotatedHostKey(old, rotated *nodev1.SSHAccess) error {
	canon := func(what string, a *nodev1.SSHAccess) (string, error) {
		_, c, err := kvmNetKnownHostsEntry(a.GetHost(), a.GetPort(), a.GetHostKey())
		if err != nil {
			return "", fmt.Errorf("%s host key: %w", what, err)
		}
		if !strings.HasPrefix(c, "ssh-ed25519 ") {
			return "", fmt.Errorf("%s host key %q is not ed25519", what, c)
		}
		return c, nil
	}
	before, err := canon("old", old)
	if err != nil {
		return err
	}
	after, err := canon("resumed", rotated)
	if err != nil {
		return err
	}
	if before == after {
		return fmt.Errorf("the resumed VM kept its host key %s: it was not rotated", before)
	}
	return nil
}

// jailKVMCheckFDs checks a jailed QEMU's descriptors: each fixed role fd is
// exactly its device or file, and no descriptor is a forbidden host object,
// lives on the cgroup filesystem or is the jailer's setup config memfd.
func jailKVMCheckFDs(fds []jailKVMFD, p jailKVMFDPolicy) error {
	if len(fds) > p.MaxFDs {
		return fmt.Errorf("qemu holds %d descriptors, more than %d", len(fds), p.MaxFDs)
	}
	byNum := map[int]jailKVMFD{}
	for _, f := range fds {
		byNum[f.Num] = f
		if what, bad := p.ForbiddenIDs[f.ID]; bad {
			return fmt.Errorf("qemu fd %d (%s) is the %s", f.Num, f.Link, what)
		}
		if f.ID.Dev == p.ForbiddenDev {
			return fmt.Errorf("qemu fd %d (%s) is on the cgroup filesystem", f.Num, f.Link)
		}
		if strings.HasPrefix(f.Link, "/memfd:bbraft-jailer-config") {
			return fmt.Errorf("qemu fd %d is the jailer's setup config (%s)", f.Num, f.Link)
		}
	}
	isNull := func(f jailKVMFD) bool { return f.Mode&unix.S_IFMT == unix.S_IFCHR && f.Rdev == p.NullRdev }
	for n := range 3 {
		f, ok := byNum[n]
		if !ok || !(isNull(f) || (n > 0 && f.Mode&unix.S_IFMT == unix.S_IFIFO)) {
			return fmt.Errorf("qemu stdio fd %d is %+v, want /dev/null%s", n, f, map[bool]string{true: " or the inherited pipe"}[n > 0])
		}
	}
	for _, f := range fds {
		if f.Num > 2 && f.Mode&unix.S_IFMT == unix.S_IFIFO {
			return fmt.Errorf("qemu fd %d is a pipe (%s): no setup pipe may survive the exec", f.Num, f.Link)
		}
	}
	for _, n := range slices.Sorted(maps.Keys(p.Rdev)) {
		f, ok := byNum[n]
		if !ok || f.Mode&unix.S_IFMT != unix.S_IFCHR || f.Rdev != p.Rdev[n] {
			return fmt.Errorf("qemu fd %d is %+v, want the character device %d:%d", n, f, unix.Major(p.Rdev[n]), unix.Minor(p.Rdev[n]))
		}
	}
	for _, n := range slices.Sorted(maps.Keys(p.ID)) {
		if f, ok := byNum[n]; !ok || f.ID != p.ID[n] {
			return fmt.Errorf("qemu fd %d is %+v, want the file %v", n, f, p.ID[n])
		}
	}
	return nil
}

// jailKVMCrossCheckQEMU proves the observed QEMU is the durable jail
// record's process: a vm record of sandbox on a non-root uid, its pid alive
// with the recorded start ticks on this boot, the VM manifest's running
// QEMU naming the same pid, start ticks, boot and attempt and the same
// control dir, and the pid a member of exactly <unit>/vms/qemu-<sandbox>.
func jailKVMCrossCheckQEMU(rec jailer.Record, meta storage.VMMeta, bootID, startTicks, cgroupFile, procs, service, sandbox string) error {
	switch {
	case rec.Kind != jailer.KindVM || rec.SandboxID != sandbox || rec.Attempt == "" || rec.PID <= 0 || rec.UID == 0:
		return fmt.Errorf("jail record %+v is not a running vm record of %s", rec, sandbox)
	case rec.BootID != bootID:
		return fmt.Errorf("jail record of %s is from boot %q, not %q", sandbox, rec.BootID, bootID)
	case startTicks != strconv.FormatUint(rec.StartTicks, 10):
		return fmt.Errorf("pid %d starts at %s, the record says %d (pid reused?)", rec.PID, startTicks, rec.StartTicks)
	}
	q := meta.QEMU
	if meta.SandboxID != sandbox || q == nil || q.State != storage.QEMURunning || q.PID != rec.PID || q.StartTicks != rec.StartTicks ||
		q.BootID != bootID || q.JailAttempt != rec.Attempt {
		return fmt.Errorf("vm manifest of %s (%+v) does not name the jail record's QEMU %d/%d/%s", sandbox, q, rec.PID, rec.StartTicks, rec.Attempt)
	}
	if meta.Control == nil || meta.Control.Dev != rec.ControlDev || meta.Control.Ino != rec.ControlIno {
		return fmt.Errorf("vm manifest control dir %v is not the jail record's %d:%d", meta.Control, rec.ControlDev, rec.ControlIno)
	}
	if want := "0::/system.slice/" + service + ".service/vms/" + "qemu-" + sandbox + "\n"; cgroupFile != want {
		return fmt.Errorf("pid %d is in cgroup %q, want %q", rec.PID, cgroupFile, want)
	}
	if !slices.Contains(strings.Fields(procs), strconv.Itoa(rec.PID)) {
		return fmt.Errorf("pid %d is not a member of qemu-%s (%q)", rec.PID, sandbox, procs)
	}
	return nil
}

// jailKVMCheckQEMUStatus checks a jailed QEMU's /proc status: its pool uid
// (inside lo..hi) for every Uid and Gid, no groups, all five capability sets
// zero, NoNewPrivs 1 (jailKVMCheckProbeStatus) and a seccomp filter
// (Seccomp 2, QEMU's -sandbox).
func jailKVMCheckQEMUStatus(status string, uid, lo, hi uint32) error {
	if uid < lo || uid > hi {
		return fmt.Errorf("qemu uid %d is outside the jail range %d-%d", uid, lo, hi)
	}
	if err := jailKVMCheckProbeStatus(status, uid); err != nil {
		return fmt.Errorf("qemu: %w", err)
	}
	if st := jailKVMParseStatus(status); st["Seccomp"] != "2" {
		return fmt.Errorf("qemu Seccomp = %q, want 2 (filter mode)", st["Seccomp"])
	}
	return nil
}

// jailKVMRootMountReadOnly checks a process's mountinfo: its effective root
// is the top of the stack of mounts at "/", found by mount-id/parent
// topology (never by line order), and that mount's per-mount options are
// read-only. Malformed lines (no " - " separator or fields after it,
// non-numeric ids), duplicate mount ids, a branch, two unrelated stacks or a
// cycle among the "/" mounts are refused.
func jailKVMRootMountReadOnly(mountinfo string) error {
	type mount struct {
		id, parent uint64
		root, opts string
	}
	ids := map[uint64]bool{}
	var roots []mount
	for line := range strings.SplitSeq(strings.TrimRight(mountinfo, "\n"), "\n") {
		f := strings.Fields(line)
		sep := slices.Index(f, "-")
		if len(f) < 6 || sep < 6 || len(f) < sep+4 {
			return fmt.Errorf("malformed mountinfo line %q", line)
		}
		id, err1 := strconv.ParseUint(f[0], 10, 64)
		parent, err2 := strconv.ParseUint(f[1], 10, 64)
		if err1 != nil || err2 != nil {
			return fmt.Errorf("malformed mountinfo ids in %q", line)
		}
		if ids[id] {
			return fmt.Errorf("mountinfo lists mount id %d twice", id)
		}
		ids[id] = true
		if f[4] == "/" {
			roots = append(roots, mount{id: id, parent: parent, root: f[3], opts: f[5]})
		}
	}
	if len(roots) == 0 {
		return errors.New("no mount at the process root")
	}
	children := map[uint64]int{}
	byID := map[uint64]mount{}
	for _, m := range roots {
		byID[m.id] = m
		if m.parent == m.id {
			return fmt.Errorf("mount %d is its own parent", m.id)
		}
	}
	for _, m := range roots {
		if _, ok := byID[m.parent]; ok {
			children[m.parent]++
		}
	}
	var tops []mount
	for _, m := range roots {
		switch children[m.id] {
		case 0:
			tops = append(tops, m)
		case 1:
		default:
			return fmt.Errorf("%d mounts are stacked directly on root mount %d", children[m.id], m.id)
		}
	}
	if len(tops) != 1 {
		return fmt.Errorf("%d unrelated stacks at the process root, want exactly one", len(tops))
	}
	// The stack below the top must be a chain that ends outside the "/"
	// mounts and holds every one of them (no cycle, nothing left over).
	seen := 0
	for m, ok := tops[0], true; ok; m, ok = byID[m.parent] {
		if seen++; seen > len(roots) {
			return errors.New("the root mounts form a cycle")
		}
	}
	if seen != len(roots) {
		return errors.New("the root mounts do not form one stack")
	}
	top := tops[0]
	if !slices.Contains(strings.Split(top.opts, ","), "ro") {
		return fmt.Errorf("the root mount %d (%s) is not read-only (%s)", top.id, top.root, top.opts)
	}
	return nil
}

// jailKVMCheckLimits compares a jail cgroup's limit files, as the kernel
// reads them back, with the limits the node derives for the VM
// (jailLimits): cpu.max, memory.max, pids.max and exactly one io.max line
// for the jail IO device with every unset rate "max".
func jailKVMCheckLimits(files map[string]string, want jailer.Limits) error {
	get := func(name string) (string, error) {
		v, ok := files[name]
		if !ok {
			return "", fmt.Errorf("no %s", name)
		}
		return strings.TrimSpace(v), nil
	}
	for name, expect := range map[string]string{
		"cpu.max":    strconv.FormatUint(uint64(want.CPUQuotaPercent)*1000, 10) + " 100000",
		"memory.max": strconv.FormatUint(want.MemoryBytes, 10),
		"pids.max":   strconv.FormatUint(uint64(want.Pids), 10),
	} {
		got, err := get(name)
		if err != nil {
			return err
		}
		if got != expect {
			return fmt.Errorf("%s = %q, want %q", name, got, expect)
		}
	}
	io, err := get("io.max")
	if err != nil {
		return err
	}
	lines := strings.Split(io, "\n")
	f := strings.Fields(lines[0])
	dev := fmt.Sprintf("%d:%d", want.IO.Device.Major, want.IO.Device.Minor)
	if len(lines) != 1 || len(f) != 5 || f[0] != dev {
		return fmt.Errorf("io.max = %q, want one line for %s", io, dev)
	}
	rate := func(v uint64) string {
		if v == 0 {
			return "max"
		}
		return strconv.FormatUint(v, 10)
	}
	expect := []string{dev, "rbps=" + rate(want.IO.ReadBPS), "wbps=" + rate(want.IO.WriteBPS), "riops=" + rate(want.IO.ReadIOPS), "wiops=" + rate(want.IO.WriteIOPS)}
	if !slices.Equal(f, expect) {
		return fmt.Errorf("io.max = %q, want %q", io, strings.Join(expect, " "))
	}
	return nil
}

// jailKVMCheckNamespaces requires a jailed QEMU's mount, pid and ipc
// namespaces (readlink of /proc/<pid>/ns/*) to be its own, not the node
// main's.
func jailKVMCheckNamespaces(qemu, main map[string]string) error {
	for _, ns := range []string{"mnt", "pid", "ipc"} {
		q, m := qemu[ns], main[ns]
		if q == "" || m == "" {
			return fmt.Errorf("%s namespace unread (qemu %q, main %q)", ns, q, m)
		}
		if q == m {
			return fmt.Errorf("qemu shares the node main's %s namespace %s", ns, q)
		}
	}
	return nil
}

// jailKVMProbeResult is the cross-uid probe child's report.
type jailKVMProbeResult struct {
	Status string `json:"status"`
	Result string `json:"result"`
}

// jailKVMProbeMarker prefixes the child's one report line.
const jailKVMProbeMarker = "JAILKVM-PROBE-RESULT "

// jailKVMParseProbeOutput finds the child's single report line and decodes
// it strictly; the result must be one of the classified outcomes.
func jailKVMParseProbeOutput(out string) (jailKVMProbeResult, error) {
	var reports []string
	for line := range strings.SplitSeq(out, "\n") {
		if rest, ok := strings.CutPrefix(line, jailKVMProbeMarker); ok {
			reports = append(reports, rest)
		}
	}
	if len(reports) != 1 {
		return jailKVMProbeResult{}, fmt.Errorf("probe output has %d reports, want exactly one:\n%s", len(reports), out)
	}
	var r jailKVMProbeResult
	if err := json.Unmarshal([]byte(reports[0]), &r, json.RejectUnknownMembers(true)); err != nil {
		return jailKVMProbeResult{}, fmt.Errorf("probe report: %w", err)
	}
	if !slices.Contains([]string{"ok", "EACCES", "EPERM"}, r.Result) {
		return jailKVMProbeResult{}, fmt.Errorf("probe result %q is not ok, EACCES or EPERM", r.Result)
	}
	return r, nil
}

// jailKVMDirectConfig is the Direct helper's direct.json: the rendered
// ExecStart flags verbatim and the unix socket to serve on.
type jailKVMDirectConfig struct {
	Flags  map[string]string `json:"flags"`
	Socket string            `json:"socket"`
}

// jailKVMHostFacts mirrors bbraft-node's host facts (NumCPU, MemTotal MiB,
// available GiB of the data dir's filesystem).
type jailKVMHostFacts struct{ VCPUs, MemoryMiB, DiskGiB uint64 }

// jailKVMDirectSettings is the Direct helper's complete configuration.
type jailKVMDirectSettings struct {
	NodeID, DataDir, ImageDir, DefaultImage, CPUModel, Socket string
	Nested                                                    qemu.NestedMode
	BootTimeout                                               time.Duration
	Capacity                                                  *nodev1.Resources
	HostVCPUs                                                 uint32
	Network                                                   nodenet.Config
	UIDs                                                      jailer.UIDRange
	Cgroup, QEMUBinary, ToolBinary                            string
	Firmware, LibraryDirs                                     []string
	IO                                                        jailer.IOLimits
}

// jailKVMDirectFlags is every flag the role renders and what the Direct
// helper does with it: required ones must be present; "unused" ones (the
// link) are accepted and ignored. A flag outside this table is refused, so
// a new rendered flag can never be silently dropped by the Direct path.
func jailKVMDirectFlags() map[string]string {
	return map[string]string{
		"-server.addr": "unused", "-node.token-file": "unused",
		"-node.id": "required", "-node.data-dir": "required", "-node.image-dir": "required", "-node.default-image": "required",
		"-node.cpu-model": "required", "-node.cpu-nested": "required", "-node.boot-timeout": "required",
		"-node.capacity-vcpus": "required", "-node.capacity-memory-mib": "required", "-node.capacity-disk-gib": "required",
		"-node.cpu-overcommit": "required", "-node.memory-overcommit": "required", "-node.disk-overcommit": "required",
		"-node.reserve-vcpus": "required", "-node.reserve-memory-mib": "required", "-node.reserve-disk-gib": "required",
		"-node.network-mode": "required", "-node.network-bridge": "required", "-node.network-prefix": "required",
		"-node.network-gateway": "required", "-node.network-resolvers": "optional", "-node.network-management": "optional",
		"-node.ssh-ingress-addr": "required", "-node.ssh-ingress-iface": "optional", "-node.ssh-port-range": "required",
		"-node.jail-uid-range": "required", "-node.jail-cgroup": "required", "-node.jail-qemu-binary": "required",
		"-node.jail-tool-binary": "required", "-node.jail-firmware": "optional", "-node.jail-library-dirs": "optional",
		"-node.jail-io-device": "required", "-node.jail-io-read-bps": "optional", "-node.jail-io-write-bps": "optional",
		"-node.jail-io-read-iops": "optional", "-node.jail-io-write-iops": "optional",
	}
}

// jailKVMDirectPlan maps the rendered flags to the Direct helper's settings
// with bbraft-node's own rules: tap networking (nodenet.Config.Validate),
// the jail plan, and capacity from the host facts at ratio 1.0 minus the
// reserves (the role renders capacity 0 = detect and ratios 1.0; any other
// value is refused, never approximated). The socket must be
// <data dir>/direct.sock.
func jailKVMDirectPlan(cfg jailKVMDirectConfig, facts jailKVMHostFacts) (jailKVMDirectSettings, error) {
	f, table := cfg.Flags, jailKVMDirectFlags()
	for _, name := range slices.Sorted(maps.Keys(f)) {
		if _, ok := table[name]; !ok {
			return jailKVMDirectSettings{}, fmt.Errorf("direct: rendered flag %s has no Direct mapping", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(table)) {
		if _, ok := f[name]; !ok && table[name] == "required" {
			return jailKVMDirectSettings{}, fmt.Errorf("direct: rendered flag %s is missing", name)
		}
	}
	clean := func(name string) (string, error) {
		v := f[name]
		if v == "" || !filepath.IsAbs(v) || filepath.Clean(v) != v {
			return "", fmt.Errorf("direct: %s %q is not an absolute clean path", name, v)
		}
		return v, nil
	}
	list := func(name string) ([]string, error) {
		v, ok := f[name]
		if !ok || v == "" {
			return nil, nil
		}
		var out []string
		for p := range strings.SplitSeq(v, ",") {
			if p == "" || !filepath.IsAbs(p) || filepath.Clean(p) != p {
				return nil, fmt.Errorf("direct: %s entry %q is not an absolute clean path", name, p)
			}
			out = append(out, p)
		}
		return out, nil
	}
	u64 := func(name string) (uint64, error) {
		v, ok := f[name]
		if !ok {
			return 0, nil
		}
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("direct: %s %q: %w", name, v, err)
		}
		return n, nil
	}
	var s jailKVMDirectSettings
	var err error
	s.NodeID, s.DefaultImage, s.CPUModel = f["-node.id"], f["-node.default-image"], f["-node.cpu-model"]
	if s.NodeID == "" || s.DefaultImage == "" || s.CPUModel == "" {
		return s, errors.New("direct: node id, default image and cpu model must be set")
	}
	if s.DataDir, err = clean("-node.data-dir"); err != nil {
		return s, err
	}
	if s.ImageDir, err = clean("-node.image-dir"); err != nil {
		return s, err
	}
	if cfg.Socket != s.DataDir+"/direct.sock" {
		return s, fmt.Errorf("direct: socket %q, want %s/direct.sock", cfg.Socket, s.DataDir)
	}
	s.Socket = cfg.Socket
	if s.Nested, err = qemu.ParseNestedMode(f["-node.cpu-nested"]); err != nil {
		return s, fmt.Errorf("direct: %w", err)
	}
	if s.BootTimeout, err = time.ParseDuration(f["-node.boot-timeout"]); err != nil || s.BootTimeout <= 0 {
		return s, fmt.Errorf("direct: -node.boot-timeout %q is not a positive duration", f["-node.boot-timeout"])
	}
	// Capacity: detect (0) at ratio 1.0, minus the reserve; the exact
	// arithmetic of bbraft-node's offerResource for these values.
	offer := func(capFlag, ratioFlag, resFlag string, detected uint64) (uint64, error) {
		if f[capFlag] != "0" {
			return 0, fmt.Errorf("direct: %s %q: only 0 (detect) is supported", capFlag, f[capFlag])
		}
		if r, err := strconv.ParseFloat(f[ratioFlag], 64); err != nil || r != 1 {
			return 0, fmt.Errorf("direct: %s %q: only 1.0 is supported", ratioFlag, f[ratioFlag])
		}
		reserve, err := u64(resFlag)
		if err != nil {
			return 0, err
		}
		if detected == 0 || reserve >= detected {
			return 0, fmt.Errorf("direct: host %d leaves nothing after %s=%d", detected, resFlag, reserve)
		}
		return detected - reserve, nil
	}
	vcpus, err := offer("-node.capacity-vcpus", "-node.cpu-overcommit", "-node.reserve-vcpus", facts.VCPUs)
	if err != nil {
		return s, err
	}
	memory, err := offer("-node.capacity-memory-mib", "-node.memory-overcommit", "-node.reserve-memory-mib", facts.MemoryMiB)
	if err != nil {
		return s, err
	}
	disk, err := offer("-node.capacity-disk-gib", "-node.disk-overcommit", "-node.reserve-disk-gib", facts.DiskGiB)
	if err != nil {
		return s, err
	}
	if facts.VCPUs > math.MaxUint32 {
		return s, fmt.Errorf("direct: host CPU count %d", facts.VCPUs)
	}
	s.Capacity = &nodev1.Resources{Vcpus: uint32(vcpus), MemoryMib: memory, DiskGib: disk}
	s.HostVCPUs = uint32(facts.VCPUs)
	// Networking: tap only, validated by nodenet itself.
	if f["-node.network-mode"] != "tap" {
		return s, fmt.Errorf("direct: -node.network-mode %q: only tap", f["-node.network-mode"])
	}
	n := nodenet.Config{Bridge: f["-node.network-bridge"], IngressIface: f["-node.ssh-ingress-iface"], StateDir: s.DataDir + "/net"}
	if n.Prefix, err = netip.ParsePrefix(f["-node.network-prefix"]); err != nil {
		return s, fmt.Errorf("direct: -node.network-prefix: %w", err)
	}
	if n.Gateway, err = netip.ParseAddr(f["-node.network-gateway"]); err != nil {
		return s, fmt.Errorf("direct: -node.network-gateway: %w", err)
	}
	if n.IngressAddr, err = netip.ParseAddr(f["-node.ssh-ingress-addr"]); err != nil {
		return s, fmt.Errorf("direct: -node.ssh-ingress-addr: %w", err)
	}
	if v := f["-node.network-resolvers"]; v != "" {
		for a := range strings.SplitSeq(v, ",") {
			addr, err := netip.ParseAddr(a)
			if err != nil {
				return s, fmt.Errorf("direct: -node.network-resolvers: %w", err)
			}
			n.Resolvers = append(n.Resolvers, addr)
		}
	}
	if v := f["-node.network-management"]; v != "" {
		for p := range strings.SplitSeq(v, ",") {
			prefix, err := netip.ParsePrefix(p)
			if err != nil {
				return s, fmt.Errorf("direct: -node.network-management: %w", err)
			}
			n.Management = append(n.Management, prefix)
		}
	}
	lo, hi, ok := strings.Cut(f["-node.ssh-port-range"], "-")
	minPort, err1 := strconv.ParseUint(lo, 10, 16)
	maxPort, err2 := strconv.ParseUint(hi, 10, 16)
	if !ok || err1 != nil || err2 != nil || minPort == 0 || minPort > maxPort {
		return s, fmt.Errorf("direct: -node.ssh-port-range %q", f["-node.ssh-port-range"])
	}
	n.SSHPorts = nodenet.PortRange{Min: uint16(minPort), Max: uint16(maxPort)}
	if err := n.Validate(); err != nil {
		return s, fmt.Errorf("direct: %w", err)
	}
	s.Network = n
	// The jail plan, with bbraft-node's rules.
	lo, hi, ok = strings.Cut(f["-node.jail-uid-range"], "-")
	minUID, err1 := strconv.ParseUint(lo, 10, 32)
	maxUID, err2 := strconv.ParseUint(hi, 10, 32)
	if !ok || err1 != nil || err2 != nil || minUID == 0 || maxUID <= minUID || maxUID >= 1<<31 {
		return s, fmt.Errorf("direct: -node.jail-uid-range %q", f["-node.jail-uid-range"])
	}
	s.UIDs = jailer.UIDRange{Min: uint32(minUID), Max: uint32(maxUID)}
	if s.Cgroup, err = clean("-node.jail-cgroup"); err != nil {
		return s, err
	}
	if filepath.Base(s.Cgroup) != "vms" {
		return s, fmt.Errorf("direct: -node.jail-cgroup %q is not <service cgroup>/vms", s.Cgroup)
	}
	if s.QEMUBinary, err = clean("-node.jail-qemu-binary"); err != nil {
		return s, err
	}
	if s.ToolBinary, err = clean("-node.jail-tool-binary"); err != nil {
		return s, err
	}
	if s.Firmware, err = list("-node.jail-firmware"); err != nil {
		return s, err
	}
	if s.LibraryDirs, err = list("-node.jail-library-dirs"); err != nil {
		return s, err
	}
	maj, minText, ok := strings.Cut(f["-node.jail-io-device"], ":")
	ma, err1 := strconv.ParseUint(maj, 10, 32)
	mi, err2 := strconv.ParseUint(minText, 10, 32)
	if !ok || err1 != nil || err2 != nil || ma == 0 {
		return s, fmt.Errorf("direct: -node.jail-io-device %q", f["-node.jail-io-device"])
	}
	s.IO.Device = jailer.BlockDevice{Major: uint32(ma), Minor: uint32(mi)}
	for name, dst := range map[string]*uint64{"-node.jail-io-read-bps": &s.IO.ReadBPS, "-node.jail-io-write-bps": &s.IO.WriteBPS,
		"-node.jail-io-read-iops": &s.IO.ReadIOPS, "-node.jail-io-write-iops": &s.IO.WriteIOPS} {
		if *dst, err = u64(name); err != nil {
			return s, err
		}
	}
	if s.IO.ReadBPS|s.IO.WriteBPS|s.IO.ReadIOPS|s.IO.WriteIOPS == 0 {
		return s, errors.New("direct: at least one -node.jail-io-* rate is required")
	}
	return s, nil
}

// jailKVMReadDirectConfig reads direct.json strictly: opened without
// following a symlink and without blocking (a FIFO is refused), a regular
// file owned by wantUID with mode exactly 0600 and one link, still the
// named inode after the open, at most 64 KiB, decoded completely with no
// unknown members or trailing data.
func jailKVMReadDirectConfig(path string, wantUID uint32) (jailKVMDirectConfig, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return jailKVMDirectConfig{}, fmt.Errorf("direct config %s: %w", path, err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	var st, named unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return jailKVMDirectConfig{}, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != wantUID || st.Mode&0o7777 != 0o600 || st.Nlink != 1 {
		return jailKVMDirectConfig{}, fmt.Errorf("direct config %s: mode %#o uid %d nlink %d, want a regular 0600 file of uid %d with one link", path, st.Mode, st.Uid, st.Nlink, wantUID)
	}
	if err := unix.Lstat(path, &named); err != nil || named.Dev != st.Dev || named.Ino != st.Ino {
		return jailKVMDirectConfig{}, fmt.Errorf("direct config %s was replaced while it was opened", path)
	}
	raw, err := io.ReadAll(io.LimitReader(f, 64<<10+1))
	if err != nil {
		return jailKVMDirectConfig{}, err
	}
	if len(raw) > 64<<10 {
		return jailKVMDirectConfig{}, fmt.Errorf("direct config %s exceeds 64 KiB", path)
	}
	var cfg jailKVMDirectConfig
	if err := json.Unmarshal(raw, &cfg, json.RejectUnknownMembers(true)); err != nil {
		return jailKVMDirectConfig{}, fmt.Errorf("direct config %s: %w", path, err)
	}
	// Re-prove after the bounded decode: the descriptor and the name are
	// still that inode, with the same mode, owner and single link.
	var again, renamed unix.Stat_t
	if err := errors.Join(unix.Fstat(fd, &again), unix.Lstat(path, &renamed)); err != nil ||
		again.Dev != st.Dev || again.Ino != st.Ino || again.Mode != st.Mode || again.Uid != st.Uid || again.Nlink != 1 ||
		renamed.Dev != st.Dev || renamed.Ino != st.Ino {
		return jailKVMDirectConfig{}, fmt.Errorf("direct config %s changed while it was read", path)
	}
	if cfg.Flags == nil || cfg.Socket == "" {
		return jailKVMDirectConfig{}, fmt.Errorf("direct config %s has no flags or socket", path)
	}
	return cfg, nil
}

// jailKVMPersistedRecord is the jailer's durable record file shape
// (records/<id>.json): the record and its resource ownership.
type jailKVMPersistedRecord struct {
	Record    jailer.Record  `json:"record"`
	Ownership jsontext.Value `json:"ownership"`
}

// jailKVMDecodeJailRecord decodes one durable jail record file strictly: no
// unknown members in the envelope or the record and no trailing data; the
// ownership stays opaque (its own members are not checked) but must be a
// JSON object with at least one member. It requires a complete running vm
// identity of sandbox before anything reads /proc: schema 2, vm kind, an
// attempt, a non-root uid, a pid, start ticks, a boot id, and the full
// control, log and overlay identities (device and inode each).
func jailKVMDecodeJailRecord(raw []byte, sandbox string) (jailer.Record, error) {
	var p jailKVMPersistedRecord
	if err := json.Unmarshal(raw, &p, json.RejectUnknownMembers(true)); err != nil {
		return jailer.Record{}, fmt.Errorf("jail record of %s: %w", sandbox, err)
	}
	var ownership map[string]jsontext.Value
	if err := json.Unmarshal(p.Ownership, &ownership); err != nil || len(ownership) == 0 {
		return jailer.Record{}, fmt.Errorf("jail record of %s has no non-empty ownership object (%v)", sandbox, err)
	}
	r := p.Record
	if r.SchemaVersion != 2 || r.Kind != jailer.KindVM || r.SandboxID != sandbox || r.Attempt == "" || r.UID == 0 ||
		r.PID <= 0 || r.StartTicks == 0 || r.BootID == "" || r.ControlDev == 0 || r.ControlIno == 0 ||
		r.LogDev == 0 || r.LogIno == 0 || r.TargetDev == 0 || r.TargetIno == 0 {
		return jailer.Record{}, fmt.Errorf("jail record of %s is not a complete running vm identity: %+v", sandbox, r)
	}
	return r, nil
}
