package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
	"github.com/HarryCoburn/simple-talk/internal/validate"
)

// Tests for the server side of the handshake: the version and name a new connection must present.

func TestHandleNewConnClosesWhenHubIsDone(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })

	chatHub := newHub(context.Background(), protocol.ProtocolVersion)
	chatHub.stop()

	go buildNewSession(context.Background(), chatHub, serverSide)
	if err := clientSide.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a deadline: %v", err)
	}

	peer := protocol.NewConn(clientSide)
	if err := peer.SendHandshake("Harry", protocol.ProtocolVersion); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}

	// register observes the closed Done, so the selects are deterministic:
	// handleNewConn bails out and closes the connection. A shutting-down server
	// has nobody to explain itself to, so this is a silent hangup, not an error
	// frame.
	if _, err := peer.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("Wanted io.EOF after a post-shutdown handshake, got %v", err)
	}
}

// A refused name is the client's problem to fix, so the server says why before
// hanging up instead of dropping the connection silently.
func TestHandleNewConnReportsATakenName(t *testing.T) {
	chatHub, _ := newTestHub(t)
	mustRegister(t, chatHub, &Session{Name: "Harry", Out: make(chan protocol.Frame, 1)})

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })
	go buildNewSession(context.Background(), chatHub, serverSide)

	if err := clientSide.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a deadline: %v", err)
	}
	peer := protocol.NewConn(clientSide)
	if err := peer.SendHandshake("Harry", protocol.ProtocolVersion); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}

	f, err := peer.Recv()
	if err != nil {
		t.Fatalf("Did not receive the rejection: %v", err)
	}
	if f.Kind != protocol.KindError {
		t.Fatalf("Wanted a %q frame for a taken name, got %q", protocol.KindError, f.Kind)
	}
	if _, err := peer.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("Wanted the server to hang up after rejecting, got %v", err)
	}
	wantRoster(t, chatHub, "Harry")
}

// The connect announcement used to be an unguarded send, so a hub that stopped
// between registering and announcing parked this goroutine forever.
func TestHandleNewConnExitsWhenHubStopsMidHandshake(t *testing.T) {
	chatHub, stop := newTestHub(t)

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })

	exited := make(chan struct{})
	go func() { buildNewSession(context.Background(), chatHub, serverSide); close(exited) }()

	peer := protocol.NewConn(clientSide)
	if err := peer.SendHandshake("Harry", protocol.ProtocolVersion); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}
	// The connection ctx is Background, and newTestHub derives the hub's from
	// its own, so stop() cancels the hub alone. Only the broadcastFrame guard
	// can release this goroutine -- which is the point of the test.
	//
	// Take the ack, then stop the hub. Nothing drains Broadcast after that, so
	// the announcement has only the Done arm left to take.
	if _, err := peer.Recv(); err != nil {
		t.Fatalf("Did not receive the handshake ack: %v", err)
	}
	stop()

	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("handleNewConn did not exit after the hub stopped")
	}
}

// The server is the authority on names: a hand-written client that skips the
// prompt's rules is rejected at the handshake.
func TestVerifyNameRejectsBadNames(t *testing.T) {
	cases := []struct {
		desc string
		name string
	}{
		{"blank", ""},
		{"only spaces", "   "},
		{"a newline, which would forge chat lines", "Alice\n<Bob> hi"},
		{"an ANSI escape, which writes to other terminals", "Ali\x1b[31mce"},
		{"a zero width rune, which hides a duplicate", "Ali​ce"},
		{"far too long", strings.Repeat("a", validate.MaxNameRunes+1)},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			in, out := net.Pipe()
			t.Cleanup(func() { in.Close(); out.Close() })
			server := protocol.NewConn(in)
			peer := protocol.NewConn(out)

			sendHandshakeInBackground(t, peer, tc.name, protocol.ProtocolVersion)

			got, err := verifyName(server, protocol.ProtocolVersion)
			if err == nil {
				t.Fatalf("verifyName accepted %q as %q, wanted a rejection", tc.name, got)
			}
		})
	}
}

// The version is checked before the name: a client built against a different
// protocol cannot be talked to, whatever it calls itself.
func TestVerifyNameRejectsAVersionMismatch(t *testing.T) {
	cases := []struct {
		desc    string
		version string
	}{
		{"an older client", "0.0.0"},
		{"a newer client", "0.0.2"},
		{"a client that sends no version at all", ""},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			in, out := net.Pipe()
			t.Cleanup(func() { in.Close(); out.Close() })
			server := protocol.NewConn(in)
			peer := protocol.NewConn(out)

			sendHandshakeInBackground(t, peer, "Harry", tc.version)

			got, err := verifyName(server, protocol.ProtocolVersion)
			if err == nil {
				t.Fatalf("verifyName accepted version %q as %q, wanted a rejection", tc.version, got)
			}
			if !errors.Is(err, ErrVersionMismatch) {
				t.Errorf("verifyName rejected version %q with %v, wanted %v", tc.version, err, ErrVersionMismatch)
			}
		})
	}
}

// The version checked is the one the server was built with, not a constant
// baked into the check, so a server running an older build turns away a client
// on today's version.
func TestVerifyNameChecksTheVersionItWasGiven(t *testing.T) {
	in, out := net.Pipe()
	t.Cleanup(func() { in.Close(); out.Close() })
	server := protocol.NewConn(in)
	peer := protocol.NewConn(out)

	sendHandshakeInBackground(t, peer, "Harry", protocol.ProtocolVersion)

	if got, err := verifyName(server, "9.9.9"); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("verifyName returned %q, %v against a server on 9.9.9, wanted %v", got, err, ErrVersionMismatch)
	}
}

// A matching version is waved through and the name is returned as validated.
func TestVerifyNameAcceptsAMatchingVersion(t *testing.T) {
	in, out := net.Pipe()
	t.Cleanup(func() { in.Close(); out.Close() })
	server := protocol.NewConn(in)
	peer := protocol.NewConn(out)

	sendHandshakeInBackground(t, peer, "  Harry  ", protocol.ProtocolVersion)

	got, err := verifyName(server, protocol.ProtocolVersion)
	if err != nil {
		t.Fatalf("verifyName rejected a matching version: %v", err)
	}
	if got != "Harry" {
		t.Errorf("verifyName returned %q, wanted the cleaned %q", got, "Harry")
	}
}

// A mismatched client is told why it was turned away rather than being dropped
// on a bare EOF, and the reason names the version it needs to reach.
func TestHandleNewConnReportsAVersionMismatch(t *testing.T) {
	chatHub, _ := newTestHub(t)
	in, out := net.Pipe()
	t.Cleanup(func() { in.Close(); out.Close() })
	peer := protocol.NewConn(out)

	go buildNewSession(context.Background(), chatHub, in)
	sendHandshakeInBackground(t, peer, "Harry", "0.0.0")

	f, err := peer.Recv()
	if err != nil {
		t.Fatalf("Wanted an error frame back before the hang up, got: %v", err)
	}
	if f.Kind != protocol.KindError {
		t.Fatalf("Wanted a %q frame back, got %q", protocol.KindError, f.Kind)
	}
	var got protocol.Error
	if err := json.Unmarshal(f.Payload, &got); err != nil {
		t.Fatalf("Could not unpack the error payload: %v", err)
	}
	if !strings.Contains(got.Message, protocol.ProtocolVersion) {
		t.Errorf("The reason was %q, wanted it to name the server version %q", got.Message, protocol.ProtocolVersion)
	}

	// The rejected client is hung up on rather than left half connected.
	if _, err := peer.Recv(); err == nil {
		t.Error("The connection stayed open after a version mismatch, wanted it closed")
	}
}

// A name the rules reject is told why, the same as a taken name, rather than
// being dropped on a bare EOF the client can only report as a receive error.
func TestHandleNewConnReportsAnInvalidName(t *testing.T) {
	cases := []struct {
		desc string
		name string
		want error
	}{
		{"blank", "", validate.ErrNameEmpty},
		{"only spaces", "   ", validate.ErrNameEmpty},
		{"a newline, which would forge chat lines", "Alice\n<Bob> hi", validate.ErrNameNotAllowed},
		{"far too long", strings.Repeat("a", validate.MaxNameRunes+1), validate.ErrNameTooLong},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			chatHub, _ := newTestHub(t)
			in, out := net.Pipe()
			t.Cleanup(func() { in.Close(); out.Close() })
			peer := protocol.NewConn(out)

			go buildNewSession(context.Background(), chatHub, in)
			sendHandshakeInBackground(t, peer, tc.name, protocol.ProtocolVersion)

			f, err := peer.Recv()
			if err != nil {
				t.Fatalf("Wanted an error frame back before the hang up, got: %v", err)
			}
			if f.Kind != protocol.KindError {
				t.Fatalf("Wanted a %q frame back, got %q", protocol.KindError, f.Kind)
			}
			var got protocol.Error
			if err := json.Unmarshal(f.Payload, &got); err != nil {
				t.Fatalf("Could not unpack the error payload: %v", err)
			}
			if got.Message != tc.want.Error() {
				t.Errorf("The reason was %q, wanted %q", got.Message, tc.want)
			}

			// The rejected name is never echoed back: it reaches a terminal.
			if tc.name != "" && strings.Contains(got.Message, tc.name) {
				t.Errorf("The reason %q quoted the rejected name", got.Message)
			}

			// The rejected client is hung up on rather than left half connected.
			if _, err := peer.Recv(); err == nil {
				t.Error("The connection stayed open after an invalid name, wanted it closed")
			}
		})
	}
}

// A shutdown has to reach a client that connected and then said nothing. The
// handshake read deadline is thirty seconds out, so without the connection
// context this goroutine and its socket would outlive the server by that long.
func TestHandleNewConnClosesWhenTheConnectionCtxIsCancelled(t *testing.T) {
	chatHub, _ := newTestHub(t)

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })

	connCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	exited := make(chan struct{})
	go func() { defer close(exited); buildNewSession(connCtx, chatHub, serverSide) }()

	// The client sends no handshake at all, so buildNewSession is parked in
	// Recv with only the deadline to save it. The hub stays up throughout:
	// cancelling the connection is the only thing under test.
	peer := protocol.NewConn(clientSide)
	cancel()

	if err := clientSide.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a read deadline: %v", err)
	}
	if _, err := peer.Recv(); err == nil {
		t.Error("The connection stayed open after its context was cancelled, wanted it closed")
	}

	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("buildNewSession did not exit after its context was cancelled")
	}
}
