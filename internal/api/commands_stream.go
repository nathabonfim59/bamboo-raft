package api

import (
	"cmp"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net/http"
	"slices"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// commandNDJSONMediaType is the pinned media type of streamed command frames.
const commandNDJSONMediaType = "application/x-ndjson"

// serveStream runs a streamed command as newline-delimited CommandStreamFrame
// lines: exactly one started frame, output, then exactly one exit or error
// frame. Every frame is flushed before the next is written.
//
// Node Exec events have a defined frame order (node.proto), and each stream's
// chunks keep that order. UTF-8 completion replaces that byte order across
// streams: complete runes are emitted as soon as their continuation arrives,
// so a later other-stream frame can precede a rune split across two events.
// At exit or failure the remaining incomplete tails are emitted in their
// original event order, with invalid bytes replaced by U+FFFD.
//
// The first node event is read before the HTTP headers go out, so a failure or
// malformed beginning still answers a normal API error envelope. After the
// started frame, failures become the single terminal error frame because the
// status line is already visible to the client.
//
// The node enforces the requested timeout. The local watchdog fires only after
// the timeout plus a small grace, so a healthy node's timeout exit frame always
// wins; a hung node still leaves one terminal timed-out frame.
func (h *commandsHandler) serveStream(w http.ResponseWriter, r *http.Request, client nodev1.NodeClient, execRequest *nodev1.ExecRequest) {
	ctx, cancel := h.executionContext(r, execRequest)
	defer cancel()
	events, err := client.Exec(ctx, execRequest)
	if err != nil {
		h.writeNodeError(w, r, err)
		return
	}
	first, err := events.Recv()
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.writeNodeError(w, r, err)
		return
	}
	if first.GetStarted() == nil {
		h.writeNodeProtocolError(w, r)
		return
	}

	w.Header().Set("Content-Type", commandNDJSONMediaType)
	encoder := jsontext.NewEncoder(w)
	controller := http.NewResponseController(w)
	if err := writeCommandFrame(encoder, controller, CommandStreamFrame{Type: CommandFrameStarted}); err != nil {
		cancel()
		return
	}

	var stdout, stderr commandUTF8Stream
	var sequence uint64
	for {
		event, recvErr := events.Recv()
		if recvErr != nil {
			if r.Context().Err() != nil {
				return
			}
			if commandWatchdogExpired(ctx) {
				if err := h.flushStreamOutput(encoder, controller, &stdout, &stderr); err != nil {
					cancel()
					return
				}
				_ = writeCommandFrame(encoder, controller, commandExitFrame(&nodev1.ExecEvent_Exit{TimedOut: true}))
				return
			}
			h.logger.WarnContext(r.Context(), "api: command stream failed", "error", commandErrorName(recvErr))
			if err := h.flushStreamOutput(encoder, controller, &stdout, &stderr); err != nil {
				cancel()
				return
			}
			if err := writeCommandFrame(encoder, controller, commandStreamErrorFrame(recvErr)); err != nil {
				cancel()
			}
			return
		}
		switch e := event.GetEvent().(type) {
		case *nodev1.ExecEvent_Started_:
			// A second started means the node lost frame order. The
			// client already saw the first one, so only an error frame
			// can report it.
			cancel()
			h.writeStreamProtocolError(encoder, controller)
			return
		case *nodev1.ExecEvent_Stdout:
			sequence++
			if data := stdout.Write(e.Stdout, sequence); data != "" {
				if err := writeCommandFrame(encoder, controller, commandOutputFrame(CommandFrameStdout, data)); err != nil {
					cancel()
					return
				}
			}
		case *nodev1.ExecEvent_Stderr:
			sequence++
			if data := stderr.Write(e.Stderr, sequence); data != "" {
				if err := writeCommandFrame(encoder, controller, commandOutputFrame(CommandFrameStderr, data)); err != nil {
					cancel()
					return
				}
			}
		case *nodev1.ExecEvent_Exit_:
			if e.Exit == nil {
				cancel()
				h.writeStreamProtocolError(encoder, controller)
				return
			}
			if err := h.flushStreamOutput(encoder, controller, &stdout, &stderr); err != nil {
				cancel()
				return
			}
			if err := writeCommandFrame(encoder, controller, commandExitFrame(e.Exit)); err != nil {
				cancel()
			}
			return
		default:
			// An empty or unknown event is a node protocol failure.
			cancel()
			h.writeStreamProtocolError(encoder, controller)
			return
		}
	}
}

// flushStreamOutput emits the remaining incomplete UTF-8 tails in the order
// their first bytes arrived. Complete runes were already emitted as soon as
// their continuation arrived, so only the final invalid/incomplete bytes of
// each stream are left here; invalid bytes become U+FFFD.
func (h *commandsHandler) flushStreamOutput(encoder *jsontext.Encoder, controller *http.ResponseController, stdout, stderr *commandUTF8Stream) error {
	type tail struct {
		kind   CommandFrameType
		stream *commandUTF8Stream
		order  uint64
	}
	tails := make([]tail, 0, 2)
	if stdout.order != 0 {
		tails = append(tails, tail{kind: CommandFrameStdout, stream: stdout, order: stdout.order})
	}
	if stderr.order != 0 {
		tails = append(tails, tail{kind: CommandFrameStderr, stream: stderr, order: stderr.order})
	}
	slices.SortFunc(tails, func(a, b tail) int { return cmp.Compare(a.order, b.order) })
	for _, pending := range tails {
		if data := pending.stream.Flush(); data != "" {
			if err := writeCommandFrame(encoder, controller, commandOutputFrame(pending.kind, data)); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeStreamProtocolError emits the single terminal error frame for a
// malformed node stream after the started frame went out.
func (h *commandsHandler) writeStreamProtocolError(encoder *jsontext.Encoder, controller *http.ResponseController) {
	h.logger.Warn("api: command node stream is malformed after started")
	_ = writeCommandFrame(encoder, controller, commandProtocolErrorFrame())
}

// writeCommandFrame marshals one frame and flushes it. The encoder terminates
// every top-level value with a newline, which is the NDJSON frame separator. A
// failed flush is a writer failure: the caller cancels the remote RPC.
func writeCommandFrame(encoder *jsontext.Encoder, controller *http.ResponseController, frame CommandStreamFrame) error {
	if err := json.MarshalEncode(encoder, frame); err != nil {
		return err
	}
	if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// commandOutputFrame is one stdout or stderr chunk.
func commandOutputFrame(kind CommandFrameType, data string) CommandStreamFrame {
	return CommandStreamFrame{Type: kind, Data: data}
}

// commandExitFrame is the terminal frame of a command that reached an exit
// event. A timeout or signal carries an explicit null exitCode.
func commandExitFrame(exit *nodev1.ExecEvent_Exit) CommandStreamFrame {
	timedOut := exit.GetTimedOut()
	exitCode := Null[int]()
	success := false
	if !timedOut && exit.ExitCode != nil {
		exitCode = Some(int(*exit.ExitCode))
		success = *exit.ExitCode == 0 && exit.GetSignal() == "" && !exit.GetOomKilled()
	}
	return CommandStreamFrame{
		Type:     CommandFrameExit,
		ExitCode: exitCode,
		Success:  Some(success),
		TimedOut: Some(timedOut),
	}
}

// commandStreamErrorFrame is the terminal frame for a node failure after the
// started frame. A command may already have run, so it is never retryable.
func commandStreamErrorFrame(err error) CommandStreamFrame {
	apiErr := commandNodeError(err)
	return CommandStreamFrame{
		Type:      CommandFrameError,
		Error:     apiErr.Code,
		Message:   apiErr.Message,
		Retryable: Some(false),
	}
}

// commandProtocolErrorFrame is the terminal frame for a malformed node stream.
func commandProtocolErrorFrame() CommandStreamFrame {
	return CommandStreamFrame{
		Type:      CommandFrameError,
		Error:     apierr.CodeSandboxDirectFailed,
		Message:   "the sandbox machine sent an invalid stream",
		Retryable: Some(false),
	}
}
