package nodenet

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// Host devices the node opens before launching QEMU. They are variables so a
// test can point at a fixture, and so nodehost checks use exactly the same
// strings.
var (
	tunDevice      = "/dev/net/tun"
	vhostNetDevice = "/dev/vhost-net"
)

// Tapper opens the host devices one VM needs. The manager passes the returned
// descriptors to the caller, which forwards them to QEMU; nobody else opens
// the device.
type Tapper interface {
	// OpenTap attaches to the named TAP, creating it if needed, and returns a
	// descriptor suitable for QEMU's "-netdev tap,fd=N". The device is not made
	// persistent here: [Tapper.PersistTap] does that only after the manager has
	// marked the link as owned, so a crash can never leave an unmarked TAP.
	OpenTap(ctx context.Context, name string) (*os.File, error)
	// PersistTap makes an open TAP survive its last descriptor, which is what a
	// node restart adopts. The manager calls it after the ownership alias
	// exists.
	PersistTap(ctx context.Context, tap *os.File) error
	// OpenVhostNet returns a /dev/vhost-net descriptor for the TAP.
	OpenVhostNet(ctx context.Context) (*os.File, error)
}

// LinuxTaps is the real [Tapper].
type LinuxTaps struct{}

// OpenTap implements [Tapper]. It opens the interface with
// IFF_TAP|IFF_NO_PI|IFF_VNET_HDR and verifies the kernel kept the virtio net
// header flag.
//
// IFF_VNET_HDR must be set here, at creation: for a descriptor supplied with
// "-netdev tap,fd=N" QEMU only probes the flag (net/tap-linux.c,
// tap_probe_vnet_hdr reads TUNGETIFF) and does not enable it itself, and
// vhost-net requires it. The device is deliberately not persistent yet; see
// PersistTap.
//
// Attaching to a name that already exists as a TAP is the adoption path and
// succeeds (the manager has already proven ownership through the alias); a
// name taken by another kind of link fails with the kernel's error.
func (LinuxTaps) OpenTap(ctx context.Context, name string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validIfaceName(name); err != nil {
		return nil, fmt.Errorf("nodenet: tap name: %w", err)
	}
	ifr, err := unix.NewIfreq(name)
	if err != nil {
		return nil, fmt.Errorf("nodenet: tap %q: %w", name, err)
	}
	ifr.SetUint16(unix.IFF_TAP | unix.IFF_NO_PI | unix.IFF_VNET_HDR)

	fd, err := unix.Open(tunDevice, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("nodenet: open %s: %w", tunDevice, err)
	}
	if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, ifr); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("nodenet: attach tap %q: %w", name, err)
	}
	if got := ifr.Name(); got != name {
		unix.Close(fd)
		return nil, fmt.Errorf("nodenet: tap name %q was replaced by %q", name, got)
	}
	flags, err := tapFlags(fd)
	if err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("nodenet: read tap %q flags: %w", name, err)
	}
	if flags&unix.IFF_VNET_HDR == 0 {
		unix.Close(fd)
		return nil, fmt.Errorf("nodenet: tap %q was created without IFF_VNET_HDR; vhost-net cannot work", name)
	}
	return os.NewFile(uintptr(fd), name), nil
}

// PersistTap implements [Tapper].
func (LinuxTaps) PersistTap(ctx context.Context, tap *os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tap == nil {
		return errors.New("nodenet: persist tap: no descriptor")
	}
	fd := int(tap.Fd())
	if fd < 0 {
		return fmt.Errorf("nodenet: persist tap %s: descriptor is closed", tap.Name())
	}
	if err := unix.IoctlSetInt(fd, unix.TUNSETPERSIST, 1); err != nil {
		return fmt.Errorf("nodenet: make tap %s persistent: %w", tap.Name(), err)
	}
	return nil
}

// tapFlags returns the interface flags the kernel reports for an attached tun
// descriptor (TUNGETIFF).
func tapFlags(fd int) (uint16, error) {
	ifr, err := unix.NewIfreq("")
	if err != nil {
		return 0, err
	}
	if err := unix.IoctlIfreq(fd, unix.TUNGETIFF, ifr); err != nil {
		return 0, err
	}
	return ifr.Uint16(), nil
}

// OpenVhostNet implements [Tapper].
func (LinuxTaps) OpenVhostNet(ctx context.Context) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(vhostNetDevice, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("nodenet: open %s: %w", vhostNetDevice, err)
	}
	return f, nil
}
