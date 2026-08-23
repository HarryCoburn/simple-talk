package server

import (
	"encoding/json"
	"net"
	"slices"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// Contains helper functions for testing.
type testHelper struct {
	Client  *Client        // server-side client
	Peer    *protocol.Conn // Far side of the pipe
	InConn  net.Conn
	OutConn net.Conn
}

// Builds a chat frame
func mustChatFrame(t *testing.T, from string, text string) protocol.Frame {
	t.Helper()
	f, err := protocol.NewChatFrame(from, text)
	if err != nil {
		t.Fatalf("Could not build a chat frame: %v", err)
	}
	return f
}

// Unpacks a frame that is expected to be a chat, returning its text.
func chatText(t *testing.T, f protocol.Frame) string {
	t.Helper()
	if f.Kind != protocol.KindChat {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindChat, f.Kind)
	}
	var chat protocol.Chat
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		t.Fatalf("Could not unpack the chat payload: %v", err)
	}
	return chat.Text
}

// Unpacks a frame that is expected to be a system, returning its text.
func systemText(t *testing.T, f protocol.Frame) string {
	t.Helper()
	if f.Kind != protocol.KindSystem {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindSystem, f.Kind)
	}
	var chat protocol.System
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		t.Fatalf("Could not unpack the chat payload: %v", err)
	}
	return chat.Text
}

func mustRegister(t *testing.T, hub *Hub, c *Client) {
	t.Helper()
	if err := hub.Register(c); err != nil {
		t.Fatalf("Could not register %q: %v", c.Name, err)
	}
}

func newTestHub(t *testing.T) (*Hub, func()) {
	chatHub := newHub()

	// Start the chathub goroutine
	go chatHub.run()
	stop := func() {
		chatHub.Stop()
		chatHub.Wait()
	}
	t.Cleanup(stop)
	return chatHub, stop
}

func setUpTest(t *testing.T) testHelper {
	in, out := net.Pipe()
	conn := protocol.NewConn(in)
	chatClient := newClient(conn, "test")
	peer := protocol.NewConn(out)
	t.Cleanup(func() {
		out.Close()
		in.Close()
		conn.Close()
	})
	return testHelper{
		Client:  chatClient,
		Peer:    peer,
		InConn:  in,
		OutConn: out}
}

// wantRoster checks exactly who the hub is holding. clientNames is the accessor
// the /who command reads, so this asserts the same hub state a user would see,
// and it reports names rather than a bare count so a failure says who is there.
func wantRoster(t *testing.T, h *Hub, want ...string) {
	t.Helper()
	got := h.clientNames()
	if len(want) == 0 && len(got) == 0 {
		return
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("The hub holds %v, wanted %v", got, want)
	}
}
