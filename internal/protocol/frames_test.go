package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

// Every builder must stamp the right kind and encode its own payload shape.
func TestBuildersSetKindAndPayload(t *testing.T) {
	cases := []struct {
		name        string
		build       func() (Frame, error)
		wantKind    Kind
		wantPayload string
	}{
		{
			name:        "chat",
			build:       func() (Frame, error) { return NewChatFrame("alice", "hello") },
			wantKind:    KindChat,
			wantPayload: `{"from":"alice","text":"hello"}`,
		},
		{
			name:        "system",
			build:       func() (Frame, error) { return NewSystemFrame("bob joined") },
			wantKind:    KindSystem,
			wantPayload: `{"text":"bob joined"}`,
		},
		{
			name:        "error",
			build:       func() (Frame, error) { return NewErrorFrame("name taken") },
			wantKind:    KindError,
			wantPayload: `{"message":"name taken"}`,
		},
		{
			name:        "handshake",
			build:       func() (Frame, error) { return newHandshakeFrame("alice") },
			wantKind:    KindHandshake,
			wantPayload: `{"name":"alice"}`,
		},
		{
			name:        "handshake ack",
			build:       func() (Frame, error) { return newHandshakeAckFrame("alice_2") },
			wantKind:    KindHandshakeAck,
			wantPayload: `{"name":"alice_2"}`,
		},
		{
			name:        "command with args",
			build:       func() (Frame, error) { return NewCommandFrame("msg", []string{"bob", "hi"}) },
			wantKind:    KindCommand,
			wantPayload: `{"name":"msg","args":["bob","hi"]}`,
		},
		{
			name:        "command without args omits the args field",
			build:       func() (Frame, error) { return NewCommandFrame("who", nil) },
			wantKind:    KindCommand,
			wantPayload: `{"name":"who"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := tc.build()
			if err != nil {
				t.Fatalf("Building the frame returned an unexpected error: %v", err)
			}
			if f.Kind != tc.wantKind {
				t.Errorf("Frame kind = %q, wanted %q", f.Kind, tc.wantKind)
			}
			if got := string(f.Payload); got != tc.wantPayload {
				t.Errorf("Payload = %s, wanted %s", got, tc.wantPayload)
			}
		})
	}
}

// An empty args slice is treated the same as no args at all: omitempty drops it,
// so the far side sees a nil Args either way.
func TestCommandFrameEmptyArgsRoundTripToNil(t *testing.T) {
	f, err := NewCommandFrame("who", []string{})
	if err != nil {
		t.Fatalf("NewCommandFrame returned an unexpected error: %v", err)
	}

	var cmd Command
	payloadInto(t, f, &cmd)
	if cmd.Args != nil {
		t.Errorf("Args = %#v, wanted nil after a round trip", cmd.Args)
	}
}

// The wire format is one JSON object with a kind and an optional payload.
func TestFrameWireFormat(t *testing.T) {
	f, err := NewSystemFrame("hello")
	if err != nil {
		t.Fatalf("NewSystemFrame returned an unexpected error: %v", err)
	}

	encoded, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("Could not encode the frame: %v", err)
	}
	if want := `{"kind":"system","payload":{"text":"hello"}}`; string(encoded) != want {
		t.Errorf("Encoded frame = %s, wanted %s", encoded, want)
	}
}

// A frame with no payload leaves the field out entirely rather than sending null.
func TestFrameOmitsAnEmptyPayload(t *testing.T) {
	encoded, err := json.Marshal(Frame{Kind: KindHandshake})
	if err != nil {
		t.Fatalf("Could not encode the frame: %v", err)
	}
	if strings.Contains(string(encoded), "payload") {
		t.Errorf("Encoded frame = %s, wanted the payload field omitted", encoded)
	}
}

// A payload that cannot be encoded is reported rather than sent as a broken frame.
func TestNewFrameReportsAnUnencodablePayload(t *testing.T) {
	f, err := newFrame(KindSystem, make(chan int))
	if err == nil {
		t.Fatalf("newFrame returned %+v and no error, wanted an encoding error", f)
	}
	if f.Kind != "" || f.Payload != nil {
		t.Errorf("newFrame returned %+v alongside an error, wanted the zero frame", f)
	}
}
