// Package guestv1_test pins the guest contract's mirror of the frozen
// nodev1 surface: the same operations minus sandbox routing fields,
// guest-owned request messages, nodev1 response types, and the guest-only
// SSH result (the node supplies host and port).
package guestv1_test

import (
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	guestv1 "github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	nodev1 "github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// A server written before the Phase 1 methods still satisfies GuestServer:
// the embedded defaults cover the new RPCs.
var _ guestv1.GuestServer = stubServer{}

type stubServer struct {
	guestv1.UnimplementedGuestServer
}

func TestServiceShape(t *testing.T) {
	type want struct {
		in, out      protoreflect.FullName
		clientStream bool
		serverStream bool
	}
	methods := map[protoreflect.Name]want{
		"Ping":          {"bbraft.guest.v1.PingRequest", "bbraft.guest.v1.PingResponse", false, false},
		"Exec":          {"bbraft.guest.v1.ExecRequest", "bbraft.node.v1.ExecEvent", false, true},
		"PutFile":       {"bbraft.guest.v1.PutFileRequest", "bbraft.node.v1.PutFileResponse", true, false},
		"Freeze":        {"bbraft.guest.v1.FreezeRequest", "bbraft.guest.v1.FreezeResponse", false, false},
		"Thaw":          {"bbraft.guest.v1.ThawRequest", "bbraft.guest.v1.ThawResponse", false, false},
		"StartProcess":  {"bbraft.guest.v1.StartProcessRequest", "bbraft.node.v1.Process", false, false},
		"GetProcess":    {"bbraft.guest.v1.GetProcessRequest", "bbraft.node.v1.ProcessOutput", false, false},
		"AttachProcess": {"bbraft.guest.v1.AttachProcessRequest", "bbraft.node.v1.ProcessEvent", false, true},
		"KillProcess":   {"bbraft.guest.v1.KillProcessRequest", "bbraft.node.v1.KillProcessResponse", false, false},
		"ReadFile":      {"bbraft.guest.v1.ReadFileRequest", "bbraft.node.v1.ReadFileResponse", false, true},
		"StatFile":      {"bbraft.guest.v1.StatFileRequest", "bbraft.node.v1.FileInfo", false, false},
		"ListFiles":     {"bbraft.guest.v1.ListFilesRequest", "bbraft.node.v1.ListFilesResponse", false, false},
		"DeleteFile":    {"bbraft.guest.v1.DeleteFileRequest", "bbraft.node.v1.DeleteFileResponse", false, false},
		"MakeDir":       {"bbraft.guest.v1.MakeDirRequest", "bbraft.node.v1.FileInfo", false, false},
		"MoveFile":      {"bbraft.guest.v1.MoveFileRequest", "bbraft.node.v1.MoveFileResponse", false, false},
		"ChmodFile":     {"bbraft.guest.v1.ChmodFileRequest", "bbraft.node.v1.FileInfo", false, false},
		"AddSSHKey":     {"bbraft.guest.v1.AddSSHKeyRequest", "bbraft.guest.v1.AddSSHKeyResponse", false, false},
	}

	svc := guestv1.File_bbraft_guest_v1_guest_proto.Services().ByName("Guest")
	if svc == nil {
		t.Fatal("service bbraft.guest.v1.Guest not found")
	}
	if got := svc.Methods().Len(); got != len(methods) {
		t.Errorf("Guest has %d methods, want %d", got, len(methods))
	}
	for name, w := range methods {
		m := svc.Methods().ByName(name)
		if m == nil {
			t.Errorf("Guest.%s: missing", name)
			continue
		}
		if got := m.Input().FullName(); got != w.in {
			t.Errorf("Guest.%s input %s, want %s", name, got, w.in)
		}
		if got := m.Output().FullName(); got != w.out {
			t.Errorf("Guest.%s output %s, want %s", name, got, w.out)
		}
		if m.IsStreamingClient() != w.clientStream || m.IsStreamingServer() != w.serverStream {
			t.Errorf("Guest.%s streaming client=%v server=%v, want client=%v server=%v",
				name, m.IsStreamingClient(), m.IsStreamingServer(), w.clientStream, w.serverStream)
		}
	}
}

// TestRequestsMirrorNodeWithoutSandboxID checks every guest request is a
// field-for-field mirror of its nodev1 counterpart with sandbox_id removed.
func TestRequestsMirrorNodeWithoutSandboxID(t *testing.T) {
	guest := guestv1.File_bbraft_guest_v1_guest_proto.Messages()
	node := nodev1.File_bbraft_node_v1_node_proto.Messages()

	// guest message → nodev1 counterpart; ".Header" is the nested message.
	names := []string{
		"ExecRequest", "PutFileRequest", "PutFileRequest.Header",
		"StartProcessRequest", "GetProcessRequest", "AttachProcessRequest",
		"KillProcessRequest", "ReadFileRequest", "StatFileRequest",
		"ListFilesRequest", "DeleteFileRequest", "MakeDirRequest",
		"MoveFileRequest", "ChmodFileRequest", "AddSSHKeyRequest",
	}
	for _, name := range names {
		g, n := messageByPath(guest, name), messageByPath(node, name)
		if g == nil || n == nil {
			t.Errorf("%s: guest=%v node=%v, want both present", name, g != nil, n != nil)
			continue
		}
		if got, want := sigs(g), sigs(n); !slices.Equal(got, want) {
			t.Errorf("%s fields %v, want nodev1 minus sandbox_id %v", name, got, want)
		}
	}
}

// TestNoSandboxRouting guards the contract rule that no guest message carries
// sandbox routing fields.
func TestNoSandboxRouting(t *testing.T) {
	msgs := guestv1.File_bbraft_guest_v1_guest_proto.Messages()
	for i := range msgs.Len() {
		m := msgs.Get(i)
		if f := m.Fields().ByName("sandbox_id"); f != nil {
			t.Errorf("%s has sandbox_id field %d", m.FullName(), f.Number())
		}
	}
}

// TestGetProcessRequestOffsetsAreOptional: the tail/offset window split needs
// explicit presence, not zero-means-unset.
func TestGetProcessRequestOffsetsAreOptional(t *testing.T) {
	m := guestv1.File_bbraft_guest_v1_guest_proto.Messages().ByName("GetProcessRequest")
	for _, name := range []protoreflect.Name{"stdout_offset", "stderr_offset"} {
		if f := m.Fields().ByName(name); f == nil || !f.HasPresence() {
			t.Errorf("GetProcessRequest.%s: presence=%v, want explicit optional", name, f != nil && f.HasPresence())
		}
	}
}

// TestAddSSHKeyResponseShape: the guest returns only what it knows; the node
// adds host and port.
func TestAddSSHKeyResponseShape(t *testing.T) {
	m := guestv1.File_bbraft_guest_v1_guest_proto.Messages().ByName("AddSSHKeyResponse")
	if got, want := sigs(m), []string{"user:string", "host_key:string"}; !slices.Equal(got, want) {
		t.Errorf("AddSSHKeyResponse fields %v, want %v", got, want)
	}
}

// messageByPath resolves "Name" or "Name.Nested" against a file's messages.
func messageByPath(msgs protoreflect.MessageDescriptors, path string) protoreflect.MessageDescriptor {
	name, rest, _ := strings.Cut(path, ".")
	m := msgs.ByName(protoreflect.Name(name))
	if m == nil || rest == "" {
		return m
	}
	return messageByPath(m.Messages(), rest)
}

// sigs returns one signature per field, skipping sandbox_id, so a guest
// message can be diffed against its nodev1 parent.
func sigs(md protoreflect.MessageDescriptor) []string {
	fs := md.Fields()
	out := make([]string, 0, fs.Len())
	for i := range fs.Len() {
		f := fs.Get(i)
		if f.Name() == "sandbox_id" {
			continue
		}
		out = append(out, sig(f))
	}
	return out
}

func sig(f protoreflect.FieldDescriptor) string {
	s := string(f.Name()) + ":"
	switch {
	case f.IsMap():
		s += "map<" + f.MapKey().Kind().String() + "," + f.MapValue().Kind().String() + ">"
	case f.IsList():
		s += "repeated " + f.Kind().String()
	case f.Kind() == protoreflect.MessageKind || f.Kind() == protoreflect.GroupKind:
		s += "message " + string(f.Message().Name())
	default:
		s += f.Kind().String()
	}
	if f.HasPresence() {
		s += ",explicit"
	}
	if o := f.ContainingOneof(); o != nil {
		s += ",oneof:" + string(o.Name())
	}
	return s
}
