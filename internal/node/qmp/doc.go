// Package qmp is a small hand-written client for the QEMU Machine Protocol
// (QMP) over a unix socket (no libvirt).
//
// One Client owns one connection. Dial reads QEMU's greeting, negotiates
// capabilities and starts a reader and a writer goroutine. Execute sends one
// command and waits for the reply with the matching id; concurrent calls are
// safe and replies may arrive in any order. Unsolicited events are delivered
// on the channel returned by Events, which must have exactly one consumer.
//
// Failures are terminal for the connection: a read or write error, an
// oversized inbound frame, or an event-queue overflow closes the connection
// and fails every pending Execute with an error that wraps ErrClosed. QMP
// server errors are returned as *Error and leave the connection usable.
//
// Cancellation is honoured while waiting for the writer, while the writer is
// blocked on a non-reading peer, and while waiting for a reply. A cancelled
// request is removed from the pending table and a late reply is ignored. A
// cancellation that interrupts a partially written frame poisons the
// connection, because the stream framing can no longer be trusted.
//
// The package logs command names, never arguments, replies or events: QMP
// arguments can contain host paths and other details that do not belong in
// node logs.
package qmp
