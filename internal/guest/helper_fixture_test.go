package guest_test

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// helperFixtureEnv selects a misbehaving helper child for protocol tests. It
// is only read by the test binary's TestMain; production never sets it.
const helperFixtureEnv = "BBRAFT_GUEST_FILE_HELPER_FIXTURE"

// helperPIDFileEnv asks a fixture to record its pid so a test can prove the
// parent reaped it.
const helperPIDFileEnv = "BBRAFT_GUEST_FILE_HELPER_PIDFILE"

const fixtureInfoFrame = 5

// fixtureDataFrame is a ReadFile data frame kind.
const fixtureDataFrame = 7

const (
	fixtureListHeader = 11
	fixtureListBatch  = 12
	fixtureEnd        = 8
)

// fixtureEntry is one valid list entry DTO for path/name.
func fixtureEntry(name, entryPath string) string {
	return `{"path":"` + entryPath + `","name":"` + name + `","type":1,"size":1,"mode":420,"modifiedAtUnixMs":1}`
}

func writeFixturePID() {
	if p := os.Getenv(helperPIDFileEnv); p != "" {
		os.WriteFile(p, []byte(strconv.Itoa(os.Getpid())), 0o644)
	}
}

// readFixtureRequest consumes one parent request frame so a fixture can
// witness that the parent reached the readiness wait.
func readFixtureRequest() {
	var hdr [4]byte
	if _, err := io.ReadFull(os.Stdin, hdr[:]); err != nil {
		return
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n < 1 || n > 2<<20 {
		return
	}
	buf := make([]byte, n)
	io.ReadFull(os.Stdin, buf)
}

// Frame kinds as the helper protocol defines them; duplicated here because
// the fixtures live in the external test package.
const (
	fixtureFrameResult = 6
	fixtureFrameError  = 9
)

func writeFixtureFrame(kind byte, payload []byte) {
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)+1))
	hdr[4] = kind
	os.Stdout.Write(hdr[:])
	os.Stdout.Write(payload)
}

// runHelperFixture emits a chosen malformed or hostile helper exchange.
func runHelperFixture(name string) int {
	switch name {
	case "error-ok":
		// Pre-fix this decoded to status.Error(OK, ...) == nil, so a helper
		// error became a silent success with no output.
		writeFixtureFrame(fixtureFrameError, []byte(`{"code":0,"message":"ok"}`))
		return 0
	case "error-then-crash":
		// Pre-fix the operation error was returned even though the helper
		// then crashed.
		writeFixtureFrame(fixtureFrameError, []byte(`{"code":5,"message":"not found"}`))
		return 3
	case "result-then-trailing":
		writeFixtureFrame(fixtureFrameResult, []byte(`{"path":"/tmp","name":"tmp","type":2,"mode":511,"modifiedAtUnixMs":1}`))
		writeFixtureFrame(fixtureFrameResult, []byte(`{"path":"/tmp","name":"tmp","type":2,"mode":511,"modifiedAtUnixMs":1}`))
		return 0
	case "hang-after-result":
		writeFixtureFrame(fixtureFrameResult, []byte(`{"path":"/tmp","name":"tmp","type":2,"mode":511,"modifiedAtUnixMs":1}`))
		time.Sleep(30 * time.Second)
		return 0
	case "oversized-frame":
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], (2<<20)+1)
		os.Stdout.Write(hdr[:])
		return 0
	case "exit-now":
		return 0
	case "read-info-send-error":
		writeFixturePID()
		writeFixtureFrame(fixtureInfoFrame, []byte(`{"path":"/tmp/x","name":"x","type":1,"size":3,"mode":420,"modifiedAtUnixMs":1}`))
		time.Sleep(30 * time.Second)
		return 0
	case "read-data-send-error":
		writeFixturePID()
		writeFixtureFrame(fixtureInfoFrame, []byte(`{"path":"/tmp/x","name":"x","type":1,"size":3,"mode":420,"modifiedAtUnixMs":1}`))
		writeFixtureFrame(fixtureDataFrame, []byte("abc"))
		time.Sleep(30 * time.Second)
		return 0
	case "put-header-reject":
		writeFixturePID()
		// The parent must surface this instead of an EPIPE from data writes.
		writeFixtureFrame(fixtureFrameError, []byte(`{"code":3,"message":"header rejected by child"}`))
		return 0
	case "list-bad-count":
		writeFixtureFrame(fixtureListHeader, []byte(`{"path":"/tmp","count":0}`))
		writeFixtureFrame(fixtureListBatch, []byte(`[`+fixtureEntry("a", "/tmp/a")+`]`))
		writeFixtureFrame(fixtureEnd, []byte(`{"count":1}`))
		return 0
	case "list-out-of-order":
		writeFixtureFrame(fixtureListHeader, []byte(`{"path":"/tmp","count":2}`))
		writeFixtureFrame(fixtureListBatch, []byte(`[`+fixtureEntry("b", "/tmp/b")+`,`+fixtureEntry("a", "/tmp/a")+`]`))
		writeFixtureFrame(fixtureEnd, []byte(`{"count":2}`))
		return 0
	case "list-wrong-path":
		writeFixtureFrame(fixtureListHeader, []byte(`{"path":"/tmp","count":1}`))
		writeFixtureFrame(fixtureListBatch, []byte(`[`+fixtureEntry("a", "/other/a")+`]`))
		writeFixtureFrame(fixtureEnd, []byte(`{"count":1}`))
		return 0
	case "list-over-limit":
		writeFixtureFrame(fixtureListHeader, []byte(`{"path":"/tmp","count":2}`))
		writeFixtureFrame(fixtureListBatch, []byte(`[`+fixtureEntry("a", "/tmp/a")+`,`+fixtureEntry("b", "/tmp/b")+`]`))
		writeFixtureFrame(fixtureEnd, []byte(`{"count":2}`))
		return 0
	case "list-bad-end":
		writeFixtureFrame(fixtureListHeader, []byte(`{"path":"/tmp","count":1}`))
		writeFixtureFrame(fixtureListBatch, []byte(`[`+fixtureEntry("a", "/tmp/a")+`]`))
		writeFixtureFrame(fixtureEnd, []byte(`{"count":2}`))
		return 0
	case "list-oversized-batch":
		writeFixturePID()
		writeFixtureFrame(fixtureListHeader, []byte(`{"path":"/tmp","count":1}`))
		writeFixtureFrame(fixtureListBatch, []byte(strings.Repeat("x", 300<<10)))
		// Stay alive: the parent must kill and reap this child when the
		// batch is rejected.
		time.Sleep(30 * time.Second)
		return 0
	case "result-invalid-info":
		writeFixtureFrame(fixtureFrameResult, []byte(`{"path":"/tmp/a","name":"wrong","type":1,"mode":420,"modifiedAtUnixMs":1}`))
		return 0
	case "root-internal-error":
		readFixtureRequest()
		writeFixtureFrame(fixtureFrameError, []byte(`{"code":13,"message":"file helper: cannot open the logical root /home/user: bad file descriptor"}`))
		return 0
	case "root-denied":
		// Consume the request first, like the real startup path, then report
		// a typed root-open failure.
		readFixtureRequest()
		writeFixtureFrame(fixtureFrameError, []byte(`{"code":3,"message":"file helper: permission denied on the logical root /home/user"}`))
		return 0
	case "put-ready-hang":
		readFixtureRequest()
		writeFixturePID()
		time.Sleep(30 * time.Second)
		return 0
	default:
		os.Stderr.WriteString("unknown helper fixture\n")
		return 2
	}
}
