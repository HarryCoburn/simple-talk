package client

import (
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

const ClosedPipeNotification = "Quitting client...\nYou have been disconnected.\n"

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

	t.Run("closes dead on disconnect", func(t *testing.T) {

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
