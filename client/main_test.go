package main

import (
	"strings"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

func TestCleanUserName(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "plain name passes through", input: "alice", want: "alice"},
		{name: "surrounding whitespace is trimmed", input: "  alice  ", want: "alice"},
		{name: "tabs and newlines are trimmed", input: "\t alice \n", want: "alice"},
		{name: "inner spaces are kept", input: " alice smith ", want: "alice smith"},
		{name: "empty input is rejected", input: "", wantErr: true},
		{name: "whitespace-only input is rejected", input: "   \t ", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cleanUserName(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("cleanUserName(%q) = %q, wanted an error", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("cleanUserName(%q) returned an unexpected drror: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("cleanUserName(%q) = %q, wanted %q", tc.input, got, tc.want)
			}
		})
	}
}

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
		name, err = setUserName(pipe.Client, scannerOf(" alice "))
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
		name, err = setUserName(pipe.Client, scannerOf("   ", "bob"))
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

// Anything other than a handshake ack means the handshake did not complete.
func TestSetUserNameRejectsANonAckReply(t *testing.T) {
	pipe := newTestPipe(t)

	go func() {
		if _, err := pipe.Peer.Recv(); err != nil {
			return
		}
		pipe.Peer.SendError("name taken")
	}()

	var name string
	var err error
	captureStdout(t, func() {
		name, err = setUserName(pipe.Client, scannerOf("alice"))
	})

	if err == nil {
		t.Fatalf("setUserName returned %q and no error, wanted an error for a non-ack reply", name)
	}
	if name != "" {
		t.Errorf("setUserName returned the name %q alongside an error, wanted an empty name", name)
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
		_, err = setUserName(pipe.Client, scannerOf("alice"))
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
		_, err = setUserName(pipe.Client, scannerOf("alice"))
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
		name, err = setUserName(pipe.Client, scannerOf())
	})

	if err != nil {
		t.Fatalf("setUserName returned an error for a normal stdin close: %v", err)
	}
	if name != "" {
		t.Errorf("setUserName returned %q, wanted an empty name", name)
	}
}

func TestReceiveLoopPrintsChatMessages(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	go func() {
		pipe.Peer.SendChat("bob", "hello there")
		pipe.Peer.SendChat("carol", "hi bob")
		pipe.Peer.Close() // ends the loop
	}()

	out := captureStdout(t, func() {
		receiveLoop(pipe.Client, dead)
	})

	for _, want := range []string{"hello there", "hi bob"} {
		if !strings.Contains(out, want) {
			t.Errorf("Wanted the output to contain %q, got: %q", want, out)
		}
	}
	waitClosed(t, dead, "dead")
}

// Frames the client does not handle yet, and chat frames it cannot decode,
// are skipped without killing the loop.
func TestReceiveLoopSkipsFramesItCannotUse(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	go func() {
		pipe.Peer.SendError("something went wrong") // not a chat frame
		pipe.Peer.SendFrame(protocol.Frame{
			Kind:    protocol.KindChat,
			Payload: []byte(`"not a chat object"`), // undecodable payload
		})
		pipe.Peer.SendChat("bob", "still here")
		pipe.Peer.Close()
	}()

	out := captureStdout(t, func() {
		receiveLoop(pipe.Client, dead)
	})

	if strings.Contains(out, "something went wrong") {
		t.Errorf("Non-chat frames should not be printed as chat. Output was: %q", out)
	}
	if !strings.Contains(out, "still here") {
		t.Errorf("The loop stopped early: later messages never printed. Output was: %q", out)
	}
	waitClosed(t, dead, "dead")
}

// Losing the connection closes dead, which is what unblocks the send loop.
func TestReceiveLoopClosesDeadOnDisconnect(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	go pipe.Peer.Close()

	out := captureStdout(t, func() {
		receiveLoop(pipe.Client, dead)
	})

	waitClosed(t, dead, "dead")
	if !strings.Contains(out, "Disconnected") {
		t.Errorf("Wanted the user to be told about the disconnect, got: %q", out)
	}
}

func TestSendLoopSendsEachLineAsChat(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	type msg struct{ from, text string }
	got := make(chan msg, 2)
	go func() {
		defer close(dead) // stands in for receiveLoop noticing the close
		for i := 0; i < 2; i++ {
			f, err := pipe.Peer.Recv()
			if err != nil {
				return
			}
			from, text := chatFrom(t, f)
			got <- msg{from, text}
		}
		// Drain until the client closes so sendLoop's final Close is observed.
		for {
			if _, err := pipe.Peer.Recv(); err != nil {
				return
			}
		}
	}()

	captureStdout(t, func() {
		sendLoop(pipe.Client, "alice", scannerOf("hello", "world"), dead)
	})

	wanted := []msg{{"alice", "hello"}, {"alice", "world"}}
	for _, want := range wanted {
		select {
		case have := <-got:
			if have != want {
				t.Errorf("Server received %+v, wanted %+v", have, want)
			}
		default:
			t.Errorf("The server never received %+v", want)
		}
	}
}

// A server-side disconnect closes dead, and the send loop must give up rather
// than keep writing into a dead connection.
func TestSendLoopStopsOnceDeadIsClosed(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})
	close(dead)

	// Nothing reads the peer: if sendLoop tried to send, net.Pipe would block
	// and this test would time out.
	done := make(chan struct{})
	go func() {
		defer close(done)
		captureStdout(t, func() {
			sendLoop(pipe.Client, "alice", scannerOf("should not be sent"), dead)
		})
	}()

	waitClosed(t, done, "sendLoop")
}

// A send failure is reported and ends the loop instead of spinning.
func TestSendLoopReportsASendFailure(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})
	pipe.Peer.Close() // writes now fail immediately

	done := make(chan struct{})
	var out string
	go func() {
		defer close(done)
		out = captureStdout(t, func() {
			sendLoop(pipe.Client, "alice", scannerOf("hello", "world"), dead)
		})
	}()

	waitClosed(t, done, "sendLoop")
	if !strings.Contains(out, "Send failed") {
		t.Errorf("Wanted the user to be told the send failed, got: %q", out)
	}
}
