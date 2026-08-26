package client

import (
	"strings"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// Tests for the handshake: picking a username the server will accept.
//
// The name rules themselves are internal/validate's to test; negotiateName now
// calls validate.Name directly, so what is left to cover here is the prompting
// loop around it.

func TestSetUserNameReturnsTheAckedName(t *testing.T) {
	pipe := newTestPipe(t)
	sent := make(chan string, 1)

	go func() {
		f, err := pipe.Peer.Recv()
		if err != nil {
			sent <- "<recv error: " + err.Error() + ">"
			return
		}
		sent <- handshakeName(t, f)
		pipe.Peer.SendHandshakeAck("alice_2") // Server renames a duplicate
	}()

	var name string
	var err error
	out := captureStdout(t, func() {
		name, err = negotiateName(pipe.Client, scannerOf(" alice "), protocol.ProtocolVersion)
	})

	if err != nil {
		t.Fatalf("setUserName returned an unexpected error: %v", err)
	}
	if got := <-sent; got != "alice" {
		t.Errorf("Server received the name %q, wanted the cleaned %q", got, "alice")
	}
	if name != "alice_2" {
		t.Errorf("setUserName returned %q, wanted the server's name %q", name, "alice_2")
	}
	if !strings.Contains(out, userNamePrompt) {
		t.Errorf("The user was never prompted. Output was: %q", out)
	}
}

// The handshake carries the client's version so the server can turn away a
// client it cannot talk to. Whatever version the client is built with is the
// version that goes on the wire.
func TestSetUserNameSendsTheClientVersion(t *testing.T) {
	pipe := newTestPipe(t)
	versions := make(chan string, 1)

	go func() {
		f, err := pipe.Peer.Recv()
		if err != nil {
			close(versions)
			return
		}
		versions <- handshakeVersion(t, f)
		pipe.Peer.SendHandshakeAck("alice")
	}()

	var err error
	captureStdout(t, func() {
		_, err = negotiateName(pipe.Client, scannerOf("alice"), protocol.ProtocolVersion)
	})
	if err != nil {
		t.Fatalf("sendHandshake returned an unexpected error: %v", err)
	}
	if got := <-versions; got != protocol.ProtocolVersion {
		t.Errorf("Server received the version %q, wanted %q", got, protocol.ProtocolVersion)
	}
}

// A blank line is rejected locally: the server should only ever see the second,
// valid name, and the user should be prompted again.
func TestSetUserNameRepromptsOnBlankInput(t *testing.T) {
	pipe := newTestPipe(t)
	names := make(chan string, 1)

	go func() {
		f, err := pipe.Peer.Recv()
		if err != nil {
			close(names)
			return
		}
		names <- handshakeName(t, f)
		pipe.Peer.SendHandshakeAck("bob")
	}()

	var name string
	var err error
	out := captureStdout(t, func() {
		name, err = negotiateName(pipe.Client, scannerOf("   ", "bob"), protocol.ProtocolVersion)
	})

	if err != nil {
		t.Fatalf("setUserName returned an unexpected error: %v", err)
	}
	if name != "bob" {
		t.Errorf("setUserName returned %q, wanted %q", name, "bob")
	}
	if got := <-names; got != "bob" {
		t.Errorf("Server received %q, wanted the blank line to be filtered out", got)
	}
	if strings.Count(out, userNamePrompt) != 2 {
		t.Errorf("Wanted the prompt to appear twice, output was: %q", out)
	}
}

// A reply that is neither an ack nor an error leaves the handshake unfinished:
// the client has no name to run under, so it must not press on regardless.
func TestSetUserNameRejectsANonAckReply(t *testing.T) {
	pipe := newTestPipe(t)

	go func() {
		if _, err := pipe.Peer.Recv(); err != nil {
			return
		}
		pipe.Peer.SendSystem("welcome to the room") // a real frame, but not one that ends a handshake
	}()

	var name string
	var err error
	captureStdout(t, func() {
		name, err = negotiateName(pipe.Client, scannerOf("alice"), protocol.ProtocolVersion)
	})

	if err == nil {
		t.Fatalf("sendHandshake returned %q and no error, wanted an error for a non-ack reply", name)
	}
	if name != "" {
		t.Errorf("sendHandshake returned the name %q alongside an error, wanted an empty name", name)
	}
}

// A rejection carries a reason, and the user is told what it was.
func TestSetUserNameShowsTheServerReasonForRejection(t *testing.T) {
	pipe := newTestPipe(t)

	go func() {
		if _, err := pipe.Peer.Recv(); err != nil {
			return
		}
		pipe.Peer.SendError("that name is already taken")
	}()

	var err error
	captureStdout(t, func() {
		_, err = negotiateName(pipe.Client, scannerOf("alice"), protocol.ProtocolVersion)
	})

	if err == nil {
		t.Fatal("setUserName returned no error for a rejected name")
	}
	if err.Error() != "that name is already taken" {
		t.Errorf("Wanted the server's reason back, got %q", err.Error())
	}
}

// If the server hangs up before replying, the read fails and the error is shown.
func TestSetUserNameReportsAReceiveError(t *testing.T) {
	pipe := newTestPipe(t)

	go func() {
		pipe.Peer.Recv() // take the handshake, then hang up
		pipe.Peer.Close()
	}()

	var err error
	captureStdout(t, func() {
		_, err = negotiateName(pipe.Client, scannerOf("alice"), protocol.ProtocolVersion)
	})

	if err == nil {
		t.Fatal("setUserName returned no error, wanted one after the server hung up")
	}
}

// Closing stdin at the prompt (ctrl-D) is a normal exit, not a failure.
func TestSetUserNameHandlesClosedInput(t *testing.T) {
	pipe := newTestPipe(t)

	var name string
	var err error
	captureStdout(t, func() {
		name, err = negotiateName(pipe.Client, scannerOf(), protocol.ProtocolVersion)
	})

	if err != nil {
		t.Fatalf("setUserName returned an error for a normal stdin close: %v", err)
	}
	if name != "" {
		t.Errorf("setUserName returned %q, wanted an empty name", name)
	}
}
