package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// queuedExec returns one scripted stream or transport error per Exec call, so
// powerOffGuest's sync-then-poweroff sequence can be driven without a guest.
type queuedExec struct {
	streams  []*scriptedStream
	errs     []error
	commands []string
}

func (q *queuedExec) Exec(ctx context.Context, in *guestv1.ExecRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
	index := len(q.commands)
	q.commands = append(q.commands, in.GetCommand())
	var err error
	if index < len(q.errs) {
		err = q.errs[index]
	}
	if err != nil {
		return nil, err
	}
	stream := q.streams[index]
	stream.ctx = ctx
	return stream, nil
}

func okStream() *scriptedStream {
	return &scriptedStream{events: []*nodev1.ExecEvent{startedEvent(1), exitEvent(int32Ptr(0))}}
}

func exitOneStream() *scriptedStream {
	return &scriptedStream{events: []*nodev1.ExecEvent{startedEvent(1), exitEvent(int32Ptr(1))}}
}

func TestPowerOffGuestRequiresZeroExit(t *testing.T) {
	tests := []struct {
		name    string
		streams []*scriptedStream
		errs    []error
		wantErr string // empty: success
		wantCmd int
	}{
		{
			name:    "sync exit 1 fails and stops before poweroff",
			streams: []*scriptedStream{exitOneStream(), okStream()},
			wantErr: "sync before poweroff did not exit successfully",
			wantCmd: 1,
		},
		{
			name:    "poweroff exit 1 fails",
			streams: []*scriptedStream{okStream(), exitOneStream()},
			wantErr: "poweroff request did not exit successfully",
			wantCmd: 2,
		},
		{
			name:    "transport loss during poweroff is tolerated",
			streams: []*scriptedStream{okStream(), nil},
			errs:    []error{nil, status.Error(codes.Unavailable, "guest powered off")},
			wantCmd: 2,
		},
		{
			name:    "plain poweroff transport error is fatal",
			streams: []*scriptedStream{okStream(), nil},
			errs:    []error{nil, errors.New("dial failed")},
			wantErr: "poweroff request:",
			wantCmd: 2,
		},
		{
			name: "protocol violation on poweroff is fatal",
			streams: []*scriptedStream{okStream(), {
				events: []*nodev1.ExecEvent{startedEvent(2)},
			}},
			wantErr: "ended without an exit event",
			wantCmd: 2,
		},
		{
			name:    "clean sync and poweroff succeed",
			streams: []*scriptedStream{okStream(), okStream()},
			wantCmd: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &queuedExec{streams: tc.streams, errs: tc.errs}
			err := powerOffGuest(t.Context(), client)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got success", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(client.commands) != tc.wantCmd {
				t.Fatalf("sent %d commands (%v), want %d", len(client.commands), client.commands, tc.wantCmd)
			}
			if tc.wantCmd == 2 {
				if got := client.commands[0]; got != "sync" {
					t.Fatalf("first command is %q, want sync", got)
				}
				if !strings.Contains(client.commands[1], "systemctl poweroff") {
					t.Fatalf("second command is %q, want a systemctl poweroff request", client.commands[1])
				}
			}
		})
	}
}

func TestShutdownTransportErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"unavailable", status.Error(codes.Unavailable, "gone"), true},
		{"canceled", status.Error(codes.Canceled, "canceled"), true},
		{"unknown status", status.Error(codes.Unknown, "unknown"), true},
		{"internal status", status.Error(codes.Internal, "internal"), true},
		{"deadline", status.Error(codes.DeadlineExceeded, "deadline"), true},
		{"invalid argument status", status.Error(codes.InvalidArgument, "bad"), false},
		{"plain error", errors.New("plain"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shutdownTransportError(tc.err); got != tc.want {
				t.Fatalf("shutdownTransportError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
