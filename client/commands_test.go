package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

func TestParseInput(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		wantName string
		wantArgs []string
		wantOK   bool
	}{
		{"bare command", "/who", "who", []string{}, true},
		{"command with args", "/tell zoe hi there", "tell", []string{"zoe", "hi", "there"}, true},
		{"mixed case is lowered", "/WhO", "who", []string{}, true},
		{"extra spaces collapse", "/tell   zoe   hi", "tell", []string{"zoe", "hi"}, true},
		{"trailing space", "/who ", "who", []string{}, true},
		{"plain chat", "hello there", "", nil, false},
		{"slash later in line", "and/or", "", nil, false},
		{"escaped slash", "//not a command", "", nil, false},
		{"lone slash", "/", "", nil, false},
		{"slash and spaces", "/   ", "", nil, false},
		{"empty line", "", "", nil, false},
		{"pose is not a command", ":waves", "", nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotName, gotArgs, gotOK := parseInput(tc.line)
			if gotOK != tc.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tc.wantOK)
			}
			if gotName != tc.wantName {
				t.Errorf("name = %q, want %q", gotName, tc.wantName)
			}
			if tc.wantOK && !reflect.DeepEqual(gotArgs, tc.wantArgs) {
				t.Errorf("args = %#v, want %#v", gotArgs, tc.wantArgs)
			}
		})
	}
}

func TestUnescapeInput(t *testing.T) {
	tests := []struct{ in, want string }{
		{"//who", "/who"},
		{"///who", "//who"},
		{"hello", "hello"},
		{"/who", "/who"}, // A real command never reaches unescapeInput.
		{"", ""},
	}
	for _, tc := range tests {
		if got := unescapeInput(tc.in); got != tc.want {
			t.Errorf("unescapeInput(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// An error frame arriving after the handshake (an unknown command, say) must be
// shown to the user rather than logged as an unknown kind.
func TestReceiveLoopPrintsErrorFrames(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	go func() {
		if err := pipe.Peer.SendError("unknown command \"nope\""); err != nil {
			return
		}
		pipe.Peer.Close()
	}()

	out := captureStdout(t, func() { receiveLoop(pipe.Client, dead) })
	waitClosed(t, dead, "dead")

	if !strings.Contains(out, `unknown command "nope"`) {
		t.Errorf("Wanted the error surfaced to the user, got: %q", out)
	}
}

// sendLoop must split typed lines into command frames and chat frames, and must
// unescape a leading "//" before sending it as chat.
func TestSendLoopSplitsCommandsFromChat(t *testing.T) {
	pipe := newTestPipe(t)
	dead := make(chan struct{})

	got := make(chan protocol.Frame, 3)
	go func() {
		defer close(dead) // stands in for receiveLoop noticing the close
		for i := 0; i < 3; i++ {
			f, err := pipe.Peer.Recv()
			if err != nil {
				return
			}
			got <- f
		}
		// Drain until the client closes so sendLoop's final Close is observed.
		for {
			if _, err := pipe.Peer.Recv(); err != nil {
				return
			}
		}
	}()

	captureStdout(t, func() {
		sendLoop(pipe.Client, "alice", scannerOf("/who zoe", "hello", "//who"), dead)
	})

	name, args := commandFrom(t, <-got)
	if name != "who" || !reflect.DeepEqual(args, []string{"zoe"}) {
		t.Errorf("Command was %q %#v, want \"who\" [zoe]", name, args)
	}

	from, text := chatFrom(t, <-got)
	if from != "alice" || text != "hello" {
		t.Errorf("Chat was %q %q, want \"alice\" \"hello\"", from, text)
	}

	from, text = chatFrom(t, <-got)
	if from != "alice" || text != "/who" {
		t.Errorf("Escaped chat was %q %q, want \"alice\" \"/who\"", from, text)
	}
}
