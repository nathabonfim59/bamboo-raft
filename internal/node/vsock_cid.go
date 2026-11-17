package node

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"

	"golang.org/x/sys/unix"
)

// localVsockDevice is the character device that answers
// VM_SOCKETS_GET_LOCAL_CID.
const localVsockDevice = "/dev/vsock"

// localVsockCID probes the host's own vsock CID with
// VM_SOCKETS_GET_LOCAL_CID. A missing device reports zero and no error;
// every other failure (permissions, I/O, an unsupported ioctl) is an error.
func localVsockCID(ctx context.Context) (uint32, error) {
	return probeLocalVsockCID(ctx, localVsockDevice)
}

// probeLocalVsockCID reads the local vsock CID from path. The path is a
// parameter so tests can prove the missing-device and ioctl-failure paths
// without opening the real device. Only the open's own not-found is absence:
// every other open error and every ioctl or close error is returned. Close
// errors are joined with the probe error instead of being dropped.
func probeLocalVsockCID(ctx context.Context, path string) (cid uint32, retErr error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open local vsock device %s: %w", path, err)
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	cid, err = unix.IoctlGetUint32(int(f.Fd()), unix.IOCTL_VM_SOCKETS_GET_LOCAL_CID)
	if err != nil {
		return 0, fmt.Errorf("ioctl VM_SOCKETS_GET_LOCAL_CID on %s: %w", path, err)
	}
	return cid, nil
}

// localCIDProbeOrDefault resolves the per-Config test seam to the production
// probe when unset.
func localCIDProbeOrDefault(probe func(context.Context) (uint32, error)) func(context.Context) (uint32, error) {
	if probe != nil {
		return probe
	}
	return localVsockCID
}

// startupLocalCID probes the host's local vsock CID exactly once and returns
// the CID the allocator must skip, or zero when there is nothing to exclude.
// The production probe reports a missing device as zero; every non-nil probe
// error fails startup instead of guessing. A CID outside min..max is
// harmless, and only a real exclusion is logged.
func startupLocalCID(ctx context.Context, log *slog.Logger, min, max uint32, probe func(context.Context) (uint32, error)) (uint32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	cid, err := localCIDProbeOrDefault(probe)(ctx)
	if err != nil {
		return 0, fmt.Errorf("probe local vsock cid: %w", err)
	}
	if cid < min || cid > max {
		return 0, nil
	}
	log.Info("node: excluding local vsock cid from allocation", "cid", cid)
	return cid, nil
}
