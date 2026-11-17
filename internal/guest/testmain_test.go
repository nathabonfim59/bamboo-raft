package guest_test

import (
	"os"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
)

// TestMain lets this test binary act as the credential-dropped file helper
// when the service re-executes it, or as a deliberately misbehaving helper
// fixture for protocol tests. Tests force the helper path even though they
// run unprivileged.
func TestMain(m *testing.M) {
	if name := os.Getenv(helperFixtureEnv); name != "" {
		os.Exit(runHelperFixture(name))
	}
	if guest.FileHelperRequested() {
		os.Exit(guest.FileHelperMain())
	}
	os.Exit(m.Run())
}
