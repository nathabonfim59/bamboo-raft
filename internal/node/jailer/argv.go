package jailer

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// SandboxPolicy is the only accepted -sandbox value: QEMU's seccomp filter
// with obsolete syscalls, privilege elevation, spawning and resource control
// denied. It matches the node's existing QEMU policy.
const SandboxPolicy = "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny"

// Fixed option values. The QMP socket and serial log are the bind-mounted
// control resources; the netdev uses the A8 TAP/vhost-net fds 3 and 4.
const (
	argMachine = "q35,accel=kvm"
	argNetdev  = "tap,id=net0,fd=3,vhost=on,vhostfd=4"
	argObject  = "rng-random,filename=/dev/urandom,id=rng0"
	argChardev = "file,id=serial0,path=" + GuestSerialPath + ",append=on"
	argSerial  = "chardev:serial0"
	argQMP     = "unix:" + GuestQMPPath + ",server=on,wait=off"
	argDisplay = "none"
)

type optionRule struct {
	value    bool
	required bool
	multiple bool
}

// supportedOption is the complete outer-option allowlist. Options are spelled
// with exactly one dash and no "=value" suffix; anything else, including a
// positional argument, is rejected. It is a function so the allowlist is not
// mutable package state.
func supportedOption(name string) (optionRule, bool) {
	switch name {
	case "-name", "-machine", "-cpu", "-m", "-smp", "-kernel", "-initrd",
		"-netdev", "-chardev", "-serial", "-qmp", "-display", "-sandbox":
		return optionRule{value: true, required: true}, true
	case "-nodefaults", "-no-user-config", "-no-reboot":
		return optionRule{required: true}, true
	case "-blockdev", "-device":
		return optionRule{value: true, required: true, multiple: true}, true
	case "-append", "-object":
		return optionRule{value: true}, true
	case "-S":
		return optionRule{}, true
	}
	return optionRule{}, false
}

func requiredOptions() []string {
	return []string{
		"-name", "-machine", "-cpu", "-nodefaults", "-no-user-config", "-no-reboot",
		"-m", "-smp", "-kernel", "-initrd", "-blockdev", "-device", "-netdev",
		"-chardev", "-serial", "-qmp", "-display", "-sandbox",
	}
}

// decimal reports whether v is 1..maxDigits digits without a leading zero.
func decimal(v string, maxDigits int) bool {
	if v == "" || len(v) > maxDigits || v[0] == '0' {
		return false
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func identChars(v string, extra string) bool {
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune(extra, r):
		default:
			return false
		}
	}
	return true
}

// validCPU accepts a model and comma-separated feature/property tokens. No
// token may be empty or carry a path; "help" listings are refused.
func validCPU(v string) bool {
	if v == "" || v == "help" || v == "?" || !identChars(v[:1], "") {
		return false
	}
	for token := range strings.SplitSeq(v, ",") {
		if token == "" || !identChars(token, "_.+=-") {
			return false
		}
	}
	return true
}

func validNodeName(v string) bool {
	if v == "" || len(v) > 32 || !identChars(v[:1], "") || (v[0] >= '0' && v[0] <= '9') {
		return false
	}
	return identChars(v, "_-")
}

// validateArgs confines QEMU's argv to the forms A10 renders, with every
// host-path-bearing value bound to a declared descriptor or control path:
//
//	-name guest=<id>,process=<id>        exactly SandboxID
//	-machine q35,accel=kvm
//	-cpu <model>[,<feature>...]          no paths, no help
//	-nodefaults -no-user-config -no-reboot
//	-m <n>M   -smp <n>
//	-kernel /proc/self/fd/<kernel>       -initrd /proc/self/fd/<initrd>
//	-append <guest kernel command line>  optional
//	-blockdev (one per Disks entry, oldest first; see validateBlockdev)
//	-netdev tap,id=net0,fd=3,vhost=on,vhostfd=4
//	-device virtio-blk-pci,drive=<overlay node>[,write-cache=on|off][,werror=..][,rerror=..]
//	-device virtio-net-pci,netdev=net0[,mac=<mac>][,romfile=]
//	-device vhost-vsock-pci,guest-cid=<n>,vhostfd=<vhost-vsock>
//	-device virtio-rng-pci,rng=rng0      requires the -object below
//	-device virtio-balloon-pci[,free-page-reporting=on|off][,deflate-on-oom=on|off]
//	-device vmgenid
//	-object rng-random,filename=/dev/urandom,id=rng0
//	-chardev file,id=serial0,path=/serial.log,append=on
//	-serial chardev:serial0
//	-qmp unix:/control/qmp.sock,server=on,wait=off
//	-display none
//	-sandbox SandboxPolicy
//
// Every option except -blockdev and -device occurs at most once; required
// ones exactly once. QEMU's ",," comma escape, JSON option syntax and
// duplicate keys are rejected so no later value can override a checked one.
func validateArgs(spec LaunchSpec, qcow []bool) error {
	counts := make(map[string]int)
	var blockdevs, devices []string
	for i := 0; i < len(spec.Args); i++ {
		option := spec.Args[i]
		rule, ok := supportedOption(option)
		if !ok {
			return fmt.Errorf("jailer: unsupported QEMU argument %q", option)
		}
		counts[option]++
		if counts[option] > 1 && !rule.multiple {
			return fmt.Errorf("jailer: QEMU option %s occurs more than once", option)
		}
		if !rule.value {
			continue
		}
		if i+1 >= len(spec.Args) {
			return fmt.Errorf("jailer: QEMU option %s has no value", option)
		}
		i++
		value := spec.Args[i]
		if strings.Contains(value, ",,") || strings.HasPrefix(value, "{") {
			return fmt.Errorf("jailer: QEMU option %s uses escaped or JSON syntax", option)
		}
		var err error
		switch option {
		case "-name":
			want := "guest=" + spec.SandboxID + ",process=" + spec.SandboxID
			if spec.Maintenance {
				want = MaintenanceName(spec.SandboxID)
			}
			err = expectValue(option, value, want)
		case "-machine":
			err = expectValue(option, value, argMachine)
		case "-cpu":
			if !validCPU(value) {
				err = fmt.Errorf("jailer: unsupported -cpu value %q", value)
			}
		case "-m":
			if mib, ok := strings.CutSuffix(value, "M"); !ok || !decimal(mib, 9) {
				err = fmt.Errorf("jailer: unsupported -m value %q", value)
			}
		case "-smp":
			if !decimal(value, 4) {
				err = fmt.Errorf("jailer: unsupported -smp value %q", value)
			}
		case "-kernel":
			err = expectValue(option, value, fdPath(spec.fdOf(KernelName)))
		case "-initrd":
			err = expectValue(option, value, fdPath(spec.fdOf(InitrdName)))
		case "-append":
			// Guest-only kernel command line; QEMU does not interpret it.
		case "-blockdev":
			blockdevs = append(blockdevs, value)
		case "-device":
			devices = append(devices, value)
		case "-netdev":
			err = expectValue(option, value, argNetdev)
		case "-object":
			err = expectValue(option, value, argObject)
		case "-chardev":
			err = expectValue(option, value, argChardev)
		case "-serial":
			err = expectValue(option, value, argSerial)
		case "-qmp":
			err = expectValue(option, value, argQMP)
		case "-display":
			err = expectValue(option, value, argDisplay)
		case "-sandbox":
			err = expectValue(option, value, SandboxPolicy)
		}
		if err != nil {
			return err
		}
	}
	required := requiredOptions()
	if spec.Maintenance {
		if err := checkMaintenanceOptions(counts); err != nil {
			return err
		}
		required = maintenanceRequiredOptions()
	}
	for _, option := range required {
		if counts[option] == 0 {
			return fmt.Errorf("jailer: required QEMU option %s is missing", option)
		}
	}
	if len(blockdevs) != len(spec.Disks) {
		return fmt.Errorf("jailer: %d -blockdev options for %d disks", len(blockdevs), len(spec.Disks))
	}
	nodes := make(map[string]struct{}, 2*len(blockdevs))
	previous := ""
	for i, value := range blockdevs {
		node, err := validateBlockdev(spec, i, qcow[i], value, previous, nodes)
		if err != nil {
			return err
		}
		previous = node
	}
	if spec.Maintenance {
		// checkMaintenanceOptions refused every -device.
		return nil
	}
	return validateDevices(spec, devices, previous, counts["-object"] == 1)
}

// ValidateArgv checks a VM argv (without argv0) against exactly the forms
// Start accepts for the fixed descriptor order tap, vhost-net, kernel,
// initrd, vhost-vsock, then the qcow2 disks named by disks (oldest first), and
// additionally requires the virtio-net option ROM disabled (romfile=). A
// restarted node uses it to prove a recovered process's complete argv.
func ValidateArgv(sandboxID string, args []string, disks []string) error {
	files := []Descriptor{{Name: TAPName}, {Name: VhostNetName}, {Name: KernelName}, {Name: InitrdName}, {Name: VhostVsockName}}
	for _, name := range disks {
		files = append(files, Descriptor{Name: name})
	}
	spec := LaunchSpec{SandboxID: sandboxID, Args: args, Files: files, Disks: disks}
	if len(disks) == 0 {
		return errors.New("jailer: disk chain is empty")
	}
	qcow := make([]bool, len(disks))
	for i := range qcow {
		qcow[i] = true
	}
	if err := validateArgs(spec, qcow); err != nil {
		return err
	}
	for i := 0; i+1 < len(args); i++ {
		if args[i] != "-device" || !strings.HasPrefix(args[i+1], "virtio-net-pci,") {
			continue
		}
		kv, err := parseKeyval("-device", args[i+1], "driver")
		if err != nil {
			return err
		}
		if rom, ok := kv["romfile"]; !ok || rom != "" {
			return errors.New("jailer: virtio-net-pci must disable its option ROM (romfile=)")
		}
	}
	return nil
}

func expectValue(option, value, want string) error {
	if value != want {
		return fmt.Errorf("jailer: unsupported %s value %q, want %q", option, value, want)
	}
	return nil
}

func fdPath(fd int) string { return "/proc/self/fd/" + strconv.Itoa(fd) }

// parseKeyval splits a QEMU option value into unique key=value pairs. When
// implied is nonempty, the first element is its value (the -device driver).
func parseKeyval(option, value, implied string) (map[string]string, error) {
	parts := strings.Split(value, ",")
	out := make(map[string]string, len(parts))
	for i, part := range parts {
		key, val, ok := strings.Cut(part, "=")
		if i == 0 && implied != "" {
			if ok {
				return nil, fmt.Errorf("jailer: %s must start with its %s", option, implied)
			}
			key, val = implied, part
		} else if !ok || key == "" {
			return nil, fmt.Errorf("jailer: %s element %q is not key=value", option, part)
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("jailer: %s repeats key %q", option, key)
		}
		out[key] = val
	}
	return out, nil
}

type keyCheck func(string) error

func oneOf(values ...string) keyCheck {
	return func(v string) error {
		for _, want := range values {
			if v == want {
				return nil
			}
		}
		return fmt.Errorf("value %q is not one of %v", v, values)
	}
}

func exactly(want string) keyCheck { return oneOf(want) }

func checkKeys(option string, kv map[string]string, allowed map[string]keyCheck, required ...string) error {
	for _, key := range required {
		if _, ok := kv[key]; !ok {
			return fmt.Errorf("jailer: %s requires %s", option, key)
		}
	}
	for key, value := range kv {
		check, ok := allowed[key]
		if !ok {
			return fmt.Errorf("jailer: %s key %q is not supported", option, key)
		}
		if err := check(value); err != nil {
			return fmt.Errorf("jailer: %s %s: %w", option, key, err)
		}
	}
	return nil
}

// validateBlockdev accepts one explicit node of the disk chain:
//
//	driver=qcow2|raw,node-name=<n>,file.driver=file,file.node-name=<n>,
//	file.filename=/proc/self/fd/<disk i>[,file.cache.direct=on|off]
//	[,file.cache.no-flush=on|off][,lazy-refcounts=on|off (qcow2)]
//	[,read-only=on|off][,backing=<node of disk i-1>]
//
// driver must match the probed image format. A qcow2 overlay must name its
// predecessor as backing, so QEMU never opens the stored backing name; the
// base has no backing key (its header names none). All but the overlay are
// read-only=on.
func validateBlockdev(spec LaunchSpec, i int, isQCOW bool, value, previous string, nodes map[string]struct{}) (string, error) {
	kv, err := parseKeyval("-blockdev", value, "")
	if err != nil {
		return "", err
	}
	driver := "raw"
	if isQCOW {
		driver = "qcow2"
	}
	nodeName := func(v string) error {
		if !validNodeName(v) {
			return fmt.Errorf("invalid node name %q", v)
		}
		if _, dup := nodes[v]; dup {
			return fmt.Errorf("node name %q is used twice", v)
		}
		nodes[v] = struct{}{}
		return nil
	}
	onOff := oneOf("on", "off")
	allowed := map[string]keyCheck{
		"driver":              exactly(driver),
		"node-name":           nodeName,
		"read-only":           onOff,
		"file.driver":         exactly("file"),
		"file.node-name":      nodeName,
		"file.filename":       exactly(fdPath(spec.fdOf(spec.Disks[i]))),
		"file.cache.direct":   onOff,
		"file.cache.no-flush": onOff,
	}
	required := []string{"driver", "node-name", "file.driver", "file.node-name", "file.filename"}
	if isQCOW {
		allowed["lazy-refcounts"] = onOff
		if i > 0 {
			allowed["backing"] = exactly(previous)
			required = append(required, "backing")
		}
	}
	if i < len(spec.Disks)-1 {
		allowed["read-only"] = exactly("on")
		required = append(required, "read-only")
	}
	if err := checkKeys("-blockdev", kv, allowed, required...); err != nil {
		return "", err
	}
	return kv["node-name"], nil
}

// validateDevices accepts exactly one disk, net and vsock device and at most
// one each of rng, balloon and vmgenid. ROM files are only disabled
// (romfile=), never loaded from a caller path.
func validateDevices(spec LaunchSpec, devices []string, overlay string, haveRNG bool) error {
	onOff := oneOf("on", "off")
	mac := func(v string) error {
		hw, err := net.ParseMAC(v)
		if err != nil || len(hw) != 6 {
			return fmt.Errorf("invalid MAC %q", v)
		}
		return nil
	}
	cid := func(v string) error {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil || n < 3 || n >= 0xffffffff {
			return fmt.Errorf("invalid guest cid %q", v)
		}
		return nil
	}
	rules := map[string]struct {
		allowed  map[string]keyCheck
		required []string
		need     bool
	}{
		"virtio-blk-pci": {allowed: map[string]keyCheck{
			"drive":       exactly(overlay),
			"write-cache": onOff,
			"werror":      oneOf("stop", "report", "ignore", "enospc"),
			"rerror":      oneOf("stop", "report", "ignore"),
		}, required: []string{"drive"}, need: true},
		"virtio-net-pci": {allowed: map[string]keyCheck{
			"netdev":  exactly("net0"),
			"mac":     mac,
			"romfile": exactly(""),
		}, required: []string{"netdev"}, need: true},
		"vhost-vsock-pci": {allowed: map[string]keyCheck{
			"guest-cid": cid,
			"vhostfd":   exactly(strconv.Itoa(spec.fdOf(VhostVsockName))),
		}, required: []string{"guest-cid", "vhostfd"}, need: true},
		"virtio-rng-pci": {allowed: map[string]keyCheck{"rng": exactly("rng0")}, required: []string{"rng"}},
		"virtio-balloon-pci": {allowed: map[string]keyCheck{
			"free-page-reporting": onOff,
			"deflate-on-oom":      onOff,
		}},
		"vmgenid": {allowed: map[string]keyCheck{}},
	}
	seen := make(map[string]bool, len(rules))
	for _, value := range devices {
		kv, err := parseKeyval("-device", value, "driver")
		if err != nil {
			return err
		}
		driver := kv["driver"]
		rule, ok := rules[driver]
		if !ok {
			return fmt.Errorf("jailer: unsupported -device driver %q", driver)
		}
		if seen[driver] {
			return fmt.Errorf("jailer: -device %s occurs more than once", driver)
		}
		seen[driver] = true
		delete(kv, "driver")
		if err := checkKeys("-device "+driver, kv, rule.allowed, rule.required...); err != nil {
			return err
		}
	}
	for driver, rule := range rules {
		if rule.need && !seen[driver] {
			return fmt.Errorf("jailer: required -device %s is missing", driver)
		}
	}
	if seen["virtio-rng-pci"] != haveRNG {
		return errors.New("jailer: virtio-rng-pci and its rng-random object must be used together")
	}
	return nil
}
