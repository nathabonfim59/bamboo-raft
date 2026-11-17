package api

// Diagnostic-label regressions: scheduler sentinels must be reported by a
// fixed name even when wrapped or joined, and hostile wrapper text must
// never reach the log value.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// coreHostileError carries attacker-controlled text around a real cause.
type coreHostileError struct {
	text  string
	cause error
}

func (e coreHostileError) Error() string { return e.text }
func (e coreHostileError) Unwrap() error { return e.cause }

func TestCoreErrorNameSchedulerSentinels(t *testing.T) {
	const secret = "postgres://user:pass@host/db?token=SECRET"
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{"no node", scheduler.ErrNoNode, "no_ready_machine"},
		{"wrapped no node", fmt.Errorf("probe: %w", scheduler.ErrNoNode), "no_ready_machine"},
		{"no capacity", scheduler.ErrNoCapacity, "no_capacity"},
		{"wrapped no capacity", fmt.Errorf("place: %w", scheduler.ErrNoCapacity), "no_capacity"},
		{"joined capacity", errors.Join(scheduler.ErrNoCapacity, errors.New(secret)), "no_capacity"},
		{"hostile capacity wrapper", coreHostileError{text: secret, cause: scheduler.ErrNoCapacity}, "no_capacity"},
		{
			name: "hostile joined no node deadline",
			err:  coreHostileError{text: secret, cause: errors.Join(scheduler.ErrNoNode, context.DeadlineExceeded)},
			want: "no_ready_machine",
		},
		{"hostile unknown", coreHostileError{text: secret}, fmt.Sprintf("%T", coreHostileError{})},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := coreErrorName(test.err)
			// Check nondisclosure before printing the returned label.
			if strings.Contains(got, secret) || strings.Contains(got, "scheduler:") {
				t.Fatal("diagnostic label leaked the cause")
			}
			if got != test.want {
				t.Errorf("coreErrorName = %q, want %q", got, test.want)
			}
		})
	}
}
