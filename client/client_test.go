package client

import (
	"bytes"
	"errors"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

const ClosedPipeNotification = "Quitting client...\nYou have been disconnected.\n"

func runReceiveLoop(t *testing.T, send func(peer *protocol.Conn)) string {
	t.Helper()
	pipe := newTestPipe(t)
	dead := make(chan struct{})
	buf := bytes.Buffer{}

	go send(pipe.Peer)

	receiveLoop(&buf, pipe.Client, dead)
	waitClosed(t, dead, "dead")
	got := buf.String()

	return got
}

func check(t *testing.T, op string, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v", op, err)
	}
}

func TestReceiveLoop(t *testing.T) {

	t.Run("receive loop continues after malformed frames", func(t *testing.T) {
		got := runReceiveLoop(t, func(peer *protocol.Conn) {
			err := peer.SendFrame(protocol.Frame{
				Kind:    protocol.KindChat,
				Payload: []byte(`"not a chat object"`),
			})
			check(t, "SendFrame failed", err)

			err = peer.SendFrame(protocol.Frame{
				Kind:    protocol.KindChat,
				Payload: []byte(`{"from": "harry", "text": "test"}`),
			})
			check(t, "SendFrame failed", err)

			err = peer.Close()
			check(t, "Close failed", err)
		})

		want := "<harry> test\n" + ClosedPipeNotification
		if got != want {
			t.Errorf("receive loop did not continue correctly after malformed frames. Got %q wanted %q", got, want)
		}
	})

	t.Run("receive loop closes dead on disconnect", func(t *testing.T) {

		got := runReceiveLoop(t, func(peer *protocol.Conn) {
			err := peer.Close()
			check(t, "Close failed", err)
		})

		if !strings.Contains(got, ClosedPipeNotification) {
			t.Errorf("Disconnect string not sent, got: %q", got)
		}

	})
}

func TestFormatFrame(t *testing.T) {

	frameTests := []struct {
		name    string
		frame   protocol.Frame
		want    string
		wantErr bool
	}{
		// Clients should not receive commands
		{name: "malformed KindCommand", frame: protocol.Frame{
			Kind:    protocol.KindCommand,
			Payload: nil,
		}, want: "", wantErr: false},

		// Handshakes pass through unformatted
		{name: "wellformed KindHandshake", frame: protocol.Frame{
			Kind:    protocol.KindHandshake,
			Payload: nil,
		}, want: "", wantErr: false},
		{name: "wellformed KindHandshakeAck", frame: protocol.Frame{
			Kind:    protocol.KindHandshakeAck,
			Payload: nil,
		}, want: "", wantErr: false},

		// Actual error checking
		{name: "malformed KindChat", frame: protocol.Frame{
			Kind:    protocol.KindChat,
			Payload: []byte(`"not a chat object"`), // undecodable payload
		}, want: "", wantErr: true},
		{name: "malformed KindSystem", frame: protocol.Frame{
			Kind:    protocol.KindSystem,
			Payload: []byte(`["not a system object"]`),
		}, want: "", wantErr: true},
		{name: "malformed KindError", frame: protocol.Frame{
			Kind:    protocol.KindError,
			Payload: []byte(`42`),
		}, want: "", wantErr: true},

		// Formatting well-formed frames
		{name: "well-formed KindChat", frame: protocol.Frame{
			Kind:    protocol.KindChat,
			Payload: []byte(`{"from": "harry", "text": "test"}`),
		},
			want: "<harry> test\n", wantErr: false},
		{name: "well-formed KindSystem", frame: protocol.Frame{
			Kind:    protocol.KindSystem,
			Payload: []byte(`{"text": "test"}`),
		},
			want: "test\n", wantErr: false},
		{name: "well-formed KindError", frame: protocol.Frame{
			Kind:    protocol.KindError,
			Payload: []byte(`{"message": "test"}`),
		},
			want: "Error: test\n", wantErr: false},
	}

	for _, tt := range frameTests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := formatFrame(tt.frame)
			if got != tt.want {
				t.Errorf("wanted message %q got %q", tt.want, got)
			}
			if (gotErr != nil) != tt.wantErr {
				t.Errorf("wanted error presence to be %v got %v", tt.wantErr, gotErr)
			}
		})
	}
}

// SendLoop

func TestClassify(t *testing.T) {
	classifyTests := []struct {
		name string
		line string
		want inputIntent
	}{
		{name: "nothing intent due to nil string", line: "", want: inputIntent{Intent: intentNothing, Text: "", Cmd: "", Args: nil}},
		{name: "nothing intent due to whitespace string", line: "   ", want: inputIntent{Intent: intentNothing, Text: "", Cmd: "", Args: nil}},
		{name: "command intent", line: "/who", want: inputIntent{Intent: intentCommand, Text: "", Cmd: "who", Args: []string{}}},
		{name: "command intent with args", line: "/msg bob hi", want: inputIntent{Intent: intentCommand, Text: "", Cmd: "msg", Args: []string{"bob", "hi"}}},
		{name: "chat intent", line: "hello", want: inputIntent{Intent: intentChat, Text: "hello", Cmd: "", Args: nil}},
		{name: "chat intent with escaped command string", line: "//who", want: inputIntent{Intent: intentChat, Text: "/who", Cmd: "", Args: nil}},
		{name: "lone slash creates chat intent", line: "/", want: inputIntent{Intent: intentChat, Text: "/", Cmd: "", Args: nil}},
	}

	compareInputIntents := func(got, want inputIntent) bool {
		if got.Intent != want.Intent {
			return false
		}
		if got.Cmd != want.Cmd {
			return false
		}
		if got.Text != want.Text {
			return false
		}
		if !slices.Equal(got.Args, want.Args) {
			return false
		}
		return true
	}

	for _, tt := range classifyTests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.line)
			if !compareInputIntents(got, tt.want) {
				t.Errorf("got %+v want %+v", got, tt.want)
			}
		})
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

// Run dials where it is told. Before the address was a parameter it was the
// string "localhost:2069" inside Run, so this asserts the error names the
// address asked for: a Run still dialling the old constant would report 2069.
func TestRunDialsTheAddressItIsGiven(t *testing.T) {
	// Bind then release, so the address is well-formed and nothing answers on it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open a listener to borrow an address from: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	err = Run(addr)
	if err == nil {
		t.Fatal("Run returned nil against an address nothing is listening on, wanted an error")
	}
	// Assert on the cause, not the message. The message interpolates addr, so
	// it names the address asked for even if the dial went somewhere else --
	// checking it would pass against a Run that ignored its parameter entirely.
	// OpError.Addr is where the connection was actually attempted.
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("Run returned %v, wanted it to carry a *net.OpError", err)
	}
	if got := opErr.Addr.String(); got != addr {
		t.Errorf("Run dialled %s, wanted %s", got, addr)
	}
}
