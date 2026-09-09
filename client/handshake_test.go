package client

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

func handshakeSetup(t *testing.T) (testPipe, chan string, bytes.Buffer) {
	t.Helper()
	pipe := newTestPipe(t)
	sent := make(chan string, 1)
	w := bytes.Buffer{}
	return pipe, sent, w
}

func TestNegotiateName(t *testing.T) {
	// The name rules themselves are internal/validate's to test; negotiateName now
	// calls validate.Name directly, so what is left to cover here is the prompting
	// loop around it.
	t.Run("sent user name returns the acked name", func(t *testing.T) {
		pipe, sent, w := handshakeSetup(t)

		go func() {
			f, err := pipe.Peer.Recv()
			if err != nil {
				sent <- "<recv error: " + err.Error() + ">"
				return
			}
			sent <- handshakeName(t, f)
			pipe.Peer.SendHandshakeAck("alice_2") // Server renames a duplicate
		}()

		name, err := negotiateName(&w, pipe.Client, scannerOf(" alice "), protocol.ProtocolVersion)
		out := w.String()

		fmt.Printf("Received this in out: %q", out)

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
	})

	t.Run("protocol version numbers match", func(t *testing.T) {
		pipe, versions, w := handshakeSetup(t)

		go func() {
			f, err := pipe.Peer.Recv()
			if err != nil {
				close(versions)
				return
			}
			versions <- handshakeVersion(t, f)
			pipe.Peer.SendHandshakeAck("alice")
		}()

		_, err := negotiateName(&w, pipe.Client, scannerOf("alice"), protocol.ProtocolVersion)
		if err != nil {
			t.Fatalf("sendHandshake returned an unexpected error: %v", err)
		}
		if got := <-versions; got != protocol.ProtocolVersion {
			t.Errorf("Server received the version %q, wanted %q", got, protocol.ProtocolVersion)
		}
	})

	// A blank line is rejected locally: the server should only ever see the second,
	// valid name, and the user should be prompted again.
	t.Run("reprompts on blank input", func(t *testing.T) {
		pipe, names, w := handshakeSetup(t)

		go func() {
			f, err := pipe.Peer.Recv()
			if err != nil {
				close(names)
				return
			}
			names <- handshakeName(t, f)
			pipe.Peer.SendHandshakeAck("bob")
		}()

		name, err := negotiateName(&w, pipe.Client, scannerOf("   ", "bob"), protocol.ProtocolVersion)
		out := w.String()

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
	})

	// A reply that is neither an ack nor an error leaves the handshake unfinished:
	// the client has no name to run under, so it must not press on regardless.
	t.Run("rejects a non-ack reply", func(t *testing.T) {
		pipe, _, w := handshakeSetup(t)

		go func() {
			if _, err := pipe.Peer.Recv(); err != nil {
				return
			}
			pipe.Peer.SendSystem("welcome to the room") // a real frame, but not one that ends a handshake
		}()

		name, err := negotiateName(&w, pipe.Client, scannerOf("alice"), protocol.ProtocolVersion)

		if err == nil {
			t.Fatalf("sendHandshake returned %q and no error, wanted an error for a non-ack reply", name)
		}
		if name != "" {
			t.Errorf("sendHandshake returned the name %q alongside an error, wanted an empty name", name)
		}
	})

	// A rejection carries a reason, and the user is told what it was.
	t.Run("shows the server reason for rejection", func(t *testing.T) {
		pipe, _, w := handshakeSetup(t)

		go func() {
			if _, err := pipe.Peer.Recv(); err != nil {
				return
			}
			pipe.Peer.SendError("that name is already taken")
		}()

		_, err := negotiateName(&w, pipe.Client, scannerOf("alice"), protocol.ProtocolVersion)

		if err == nil {
			t.Fatal("setUserName returned no error for a rejected name")
		}
		if err.Error() != "that name is already taken" {
			t.Errorf("Wanted the server's reason back, got %q", err.Error())
		}
	})

	// If the server hangs up before replying, the read fails and the error is shown.
	t.Run("reports a receive error", func(t *testing.T) {
		pipe, _, w := handshakeSetup(t)

		go func() {
			pipe.Peer.Recv() // take the handshake, then hang up
			pipe.Peer.Close()
		}()

		_, err := negotiateName(&w, pipe.Client, scannerOf("alice"), protocol.ProtocolVersion)

		if err == nil {
			t.Fatal("setUserName returned no error, wanted one after the server hung up")
		}
	})

	// Closing stdin at the prompt (ctrl-D) is a normal exit, not a failure.
	t.Run("handle ctrl-D input", func(t *testing.T) {
		pipe, _, w := handshakeSetup(t)

		name, err := negotiateName(&w, pipe.Client, scannerOf(), protocol.ProtocolVersion)

		if err != nil {
			t.Fatalf("setUserName returned an error for a normal stdin close: %v", err)
		}
		if name != "" {
			t.Errorf("setUserName returned %q, wanted an empty name", name)
		}
	})
}
