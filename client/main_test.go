package client

import (
	"slices"
	"strings"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

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

// System and error frames are surfaced to the user, each in its own form.
func TestReceiveLoopPrintsSystemAndErrorFrames(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	go func() {
		pipe.Peer.SendSystem("bob joined the room")
		pipe.Peer.SendError("unknown command")
		pipe.Peer.Close()
	}()

	out := captureStdout(t, func() {
		receiveLoop(pipe.Client, dead)
	})

	if !strings.Contains(out, "bob joined the room") {
		t.Errorf("Wanted the system message in the output, got: %q", out)
	}
	if !strings.Contains(out, "Error: unknown command") {
		t.Errorf("Wanted the error message labelled as an error, got: %q", out)
	}
	waitClosed(t, dead, "dead")
}

// Skip frames the client cannot handle and cannot decode
func TestReceiveLoopSkipsFramesItCannotUse(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	go func() {
		pipe.Peer.SendFrame(protocol.Frame{
			Kind:    protocol.KindCommand,
			Payload: []byte(`{"name":"who"`), // undecodable payload
		})
		pipe.Peer.SendFrame(protocol.Frame{
			Kind:    protocol.KindChat,
			Payload: []byte(`"not a chat object"`), // undecodable payload
		})
		pipe.Peer.SendFrame(protocol.Frame{
			Kind:    protocol.KindSystem,
			Payload: []byte(`["not a system object"]`),
		})
		pipe.Peer.SendFrame(protocol.Frame{
			Kind:    protocol.KindError,
			Payload: []byte(`42`),
		})
		pipe.Peer.SendChat("bob", "still here")
		pipe.Peer.Close()
	}()

	out := captureStdout(t, func() {
		receiveLoop(pipe.Client, dead)
	})

	if strings.Contains(out, "who") {
		t.Errorf("Unhandled frame kinds should not be printed. Output was: %q", out)
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

// A leading slash makes a line a command; the escape "//" makes it chat again.
func TestParseInput(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		wantName string
		wantArgs []string
		wantOK   bool
	}{
		{name: "plain text is not a command", input: "hello there", wantOK: false},
		{name: "empty line is not a command", input: "", wantOK: false},
		{name: "bare command", input: "/who", wantName: "who", wantArgs: []string{}, wantOK: true},
		{name: "command with arguments", input: "/msg bob hello", wantName: "msg", wantArgs: []string{"bob", "hello"}, wantOK: true},
		{name: "command name is lowercased", input: "/WHO", wantName: "who", wantArgs: []string{}, wantOK: true},
		{name: "argument case is preserved", input: "/msg Bob Hello", wantName: "msg", wantArgs: []string{"Bob", "Hello"}, wantOK: true},
		{name: "extra whitespace is collapsed", input: "/msg   bob    hi", wantName: "msg", wantArgs: []string{"bob", "hi"}, wantOK: true},
		{name: "a lone slash is not a command", input: "/", wantOK: false},
		{name: "a slash and whitespace is not a command", input: "/   \t ", wantOK: false},
		{name: "a doubled slash escapes to chat", input: "//who", wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name, args, ok := parseInput(tc.input)
			if ok != tc.wantOK {
				t.Fatalf("parseInput(%q) ok = %v, wanted %v", tc.input, ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if name != tc.wantName {
				t.Errorf("parseInput(%q) name = %q, wanted %q", tc.input, name, tc.wantName)
			}
			if !slices.Equal(args, tc.wantArgs) {
				t.Errorf("parseInput(%q) args = %q, wanted %q", tc.input, args, tc.wantArgs)
			}
		})
	}
}

func TestUnescapeInput(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "plain text is untouched", input: "hello", want: "hello"},
		{name: "a doubled slash loses one slash", input: "//who", want: "/who"},
		{name: "a tripled slash loses only one slash", input: "///who", want: "//who"},
		{name: "an inner slash is untouched", input: "and/or", want: "and/or"},
		{name: "empty input is untouched", input: "", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := unescapeInput(tc.input); got != tc.want {
				t.Errorf("unescapeInput(%q) = %q, wanted %q", tc.input, got, tc.want)
			}
		})
	}
}

// Slash lines go out as command frames, and an escaped slash goes out as chat
// with the escape stripped.
func TestSendLoopSendsCommandsAndEscapedChat(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	frames := make(chan protocol.Frame, 2)
	go func() {
		defer close(dead) // stands in for receiveLoop noticing the close
		for i := 0; i < 2; i++ {
			f, err := pipe.Peer.Recv()
			if err != nil {
				return
			}
			frames <- f
		}
		// Drain until the client closes so sendLoop's final Close is observed.
		for {
			if _, err := pipe.Peer.Recv(); err != nil {
				return
			}
		}
	}()

	captureStdout(t, func() {
		sendLoop(pipe.Client, "alice", scannerOf("/msg bob hi", "//not a command"), dead)
	})

	name, args := commandFrom(t, <-frames)
	if name != "msg" {
		t.Errorf("Server received the command %q, wanted %q", name, "msg")
	}
	if !slices.Equal(args, []string{"bob", "hi"}) {
		t.Errorf("Server received the args %q, wanted %q", args, []string{"bob", "hi"})
	}

	from, text := chatFrom(t, <-frames)
	if from != "alice" {
		t.Errorf("Server received chat from %q, wanted %q", from, "alice")
	}
	if text != "/not a command" {
		t.Errorf("Server received the text %q, wanted the escape stripped to %q", text, "/not a command")
	}
}
