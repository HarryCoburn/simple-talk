package server

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// Contains helper functions for testing.
type testHelper struct {
	Session *Session       // the server-side session
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

func mustRegister(t *testing.T, hub *Hub, c *Session) {
	t.Helper()
	if err := hub.registerSession(c); err != nil {
		t.Fatalf("Could not register %q: %v", c.Name, err)
	}
}

func newTestHub(t *testing.T) (*Hub, func()) {
	chatHub := newHub(serverVersion)

	// Start the chathub goroutine
	go chatHub.run()
	stop := func() {
		chatHub.stop()
		chatHub.wait()
	}
	t.Cleanup(stop)
	return chatHub, stop
}

func setUpTest(t *testing.T) testHelper {
	in, out := net.Pipe()
	conn := protocol.NewConn(in)
	chatSession := newSession(conn, "test")
	peer := protocol.NewConn(out)
	t.Cleanup(func() {
		out.Close()
		in.Close()
		conn.Close()
	})
	return testHelper{
		Session: chatSession,
		Peer:    peer,
		InConn:  in,
		OutConn: out}
}

// wantRoster checks exactly who the hub is holding. sessionNames is the accessor
// the /who command reads, so this asserts the same hub state a user would see,
// and it reports names rather than a bare count so a failure says who is there.
func wantRoster(t *testing.T, h *Hub, want ...string) {
	t.Helper()
	got, err := h.sessionNames()
	if err != nil {
		t.Fatalf("Could not read the roster: %v", err)
	}
	if len(want) == 0 && len(got) == 0 {
		return
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("The hub holds %v, wanted %v", got, want)
	}
}

// nextReply takes the one frame a command queued for its caller. Replies are
// delivered through the hub, so the caller has to be registered to receive one.
func nextReply(t *testing.T, c *Session) protocol.Frame {
	t.Helper()
	select {
	case f := <-c.Out:
		return f
	case <-time.After(2 * time.Second):
		t.Fatalf("%s never received a reply", c.Name)
		return protocol.Frame{}
	}
}

// wantNoReply checks that nothing was queued for a session.
func wantNoReply(t *testing.T, c *Session) {
	t.Helper()
	select {
	case f := <-c.Out:
		t.Errorf("%s received an unexpected %q frame: %s", c.Name, f.Kind, f.Payload)
	case <-time.After(50 * time.Millisecond):
	}
}

// errorText unpacks a frame expected to be an error, returning its message.
func errorText(t *testing.T, f protocol.Frame) string {
	t.Helper()
	if f.Kind != protocol.KindError {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindError, f.Kind)
	}
	var e protocol.Error
	if err := json.Unmarshal(f.Payload, &e); err != nil {
		t.Fatalf("Could not unpack the error payload: %v", err)
	}
	return e.Message
}

// joinRoom registers a session under the given name and returns it. The Out
// buffer is roomy so a queued reply never trips the drop-a-stalled-client path.
func joinRoom(t *testing.T, hub *Hub, name string) *Session {
	t.Helper()
	c := &Session{Name: name, Out: make(chan protocol.Frame, 8)}
	mustRegister(t, hub, c)
	return c
}

// lockedBuffer collects log output written from another goroutine.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureLog points the standard logger at a buffer for the duration of one
// test. A recovered panic is reported through log, so this is how a test reads
// it, and it keeps the stack trace out of the test output.
func captureLog(t *testing.T) *lockedBuffer {
	t.Helper()
	buf := &lockedBuffer{}
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return buf
}
