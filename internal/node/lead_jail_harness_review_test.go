package node

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestLeadJailBoundaryAncillarySettings(t *testing.T) {
	u, err := jailKVMParseUnit(jailKVMSampleUnit)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		"ProtectKernelTunables": "true", "ProtectKernelModules": "true", "ProtectKernelLogs": "true", "ProtectClock": "true",
		"RestrictSUIDSGID": "true", "LockPersonality": "true", "RestrictAddressFamilies": "AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_VSOCK",
		"LimitCORE": "0", "LimitNOFILE": "65536", "TasksMax": "infinity", "TimeoutStopSec": "30",
	} {
		u.Service[k] = []string{v}
	}
	want := jailKVMSampleWant
	want.NetnsPath = "/run/netns/test"
	if _, err := jailKVMImmutable(u, want); err != nil {
		t.Fatalf("full positive control: %v", err)
	}
	for _, tc := range []struct{ key, value string }{
		{"ProtectKernelTunables", "false"}, {"ProtectKernelModules", "false"}, {"ProtectKernelLogs", "false"}, {"ProtectClock", "false"},
		{"RestrictSUIDSGID", "false"}, {"LockPersonality", "false"}, {"RestrictAddressFamilies", "AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_VSOCK AF_PACKET"},
		{"LimitCORE", "infinity"}, {"ReadOnlyPaths", ""}, {"ReadWritePaths", "/"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			weak := jailKVMUnit{Service: map[string][]string{}, ExecStart: slices.Clone(u.ExecStart)}
			for k, v := range u.Service {
				weak.Service[k] = slices.Clone(v)
			}
			weak.Service[tc.key] = []string{tc.value}
			if _, err := jailKVMImmutable(weak, want); err == nil {
				t.Fatalf("weakened %s=%s was accepted as the boundary", tc.key, tc.value)
			}
		})
	}
}

func TestLeadJailListenerHonestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	f := &jailKVMFakeNetns{}
	ops := f.ops()
	done := make(chan struct{})
	finish := sync.OnceFunc(func() { close(done) })
	closeFD := ops.Close
	ops.Close = func(fd int) error {
		err := closeFD(fd)
		if fd == 100 {
			finish()
		}
		return err
	}
	lis, _, err := jailKVMListenInNetns(ctx, "/run/netns/owned", "127.0.0.1:7701", time.Second, ops)
	if lis != nil {
		lis.Close()
	}
	if !errors.Is(err, context.Canceled) {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("old helper worker did not finish")
		}
		t.Errorf("pre-canceled listener error=%v, want Canceled", err)
	}
}
