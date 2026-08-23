package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// Tests for the sending half of Conn: what goes onto the wire, and when.

// Each Send helper must arrive at the far end as the matching frame with its
// fields intact.
func TestSendHelpersRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		send  func(*Conn) error
		kind  Kind
		check func(*testing.T, Frame)
	}{
		{
			name: "chat",
			send: func(c *Conn) error { return c.SendChat("alice", "hello") },
			kind: KindChat,
			check: func(t *testing.T, f Frame) {
				var chat Chat
				payloadInto(t, f, &chat)
				if chat.From != "alice" || chat.Text != "hello" {
					t.Errorf("Received %+v, wanted From alice and Text hello", chat)
				}
			},
		},
		{
			name: "system",
			send: func(c *Conn) error { return c.SendSystem("bob joined") },
			kind: KindSystem,
			check: func(t *testing.T, f Frame) {
				var sys System
				payloadInto(t, f, &sys)
				if sys.Text != "bob joined" {
					t.Errorf("Received the text %q, wanted %q", sys.Text, "bob joined")
				}
			},
		},
		{
			name: "error",
			send: func(c *Conn) error { return c.SendError("name taken") },
			kind: KindError,
			check: func(t *testing.T, f Frame) {
				var e Error
				payloadInto(t, f, &e)
				if e.Message != "name taken" {
					t.Errorf("Received the message %q, wanted %q", e.Message, "name taken")
				}
			},
		},
		{
			name: "handshake",
			send: func(c *Conn) error { return c.SendHandshake("alice", "0.0.1") },
			kind: KindHandshake,
			check: func(t *testing.T, f Frame) {
				var hs Handshake
				payloadInto(t, f, &hs)
				if hs.Name != "alice" {
					t.Errorf("Received the name %q, wanted %q", hs.Name, "alice")
				}
				if hs.Version != "0.0.1" {
					t.Errorf("Received the version %q, wanted %q", hs.Version, "0.0.1")
				}
			},
		},
		{
			name: "handshake ack",
			send: func(c *Conn) error { return c.SendHandshakeAck("alice_2") },
			kind: KindHandshakeAck,
			check: func(t *testing.T, f Frame) {
				var ack HandshakeAck
				payloadInto(t, f, &ack)
				if ack.Name != "alice_2" {
					t.Errorf("Received the name %q, wanted %q", ack.Name, "alice_2")
				}
			},
		},
		{
			name: "command",
			send: func(c *Conn) error { return c.SendCommand("msg", []string{"bob", "hi"}) },
			kind: KindCommand,
			check: func(t *testing.T, f Frame) {
				var cmd Command
				payloadInto(t, f, &cmd)
				if cmd.Name != "msg" {
					t.Errorf("Received the command %q, wanted %q", cmd.Name, "msg")
				}
				if len(cmd.Args) != 2 || cmd.Args[0] != "bob" || cmd.Args[1] != "hi" {
					t.Errorf("Received the args %q, wanted [bob hi]", cmd.Args)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipe := newTestPipe(t)
			go tc.send(pipe.A)
			tc.check(t, mustRecv(t, pipe.B, tc.kind))
		})
	}
}

// SendFrame is guarded by a mutex so that each frame reaches the wire whole.
// The connection here splits every write into small pieces and yields in
// between, the way a real socket can under a partial write: without the lock,
// two senders interleave their fragments and the bytes on the wire stop being
// one parseable frame per line. The far side is drained into a buffer rather
// than parsed live, so a corrupt stream fails the assertions instead of
// deadlocking the senders. Run this under -race.
func TestSendFrameIsSafeForConcurrentUse(t *testing.T) {
	const senders, perSender = 8, 25
	const text = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

	near, far := net.Pipe()
	conn := NewConn(chunkedWrites{Conn: near, chunk: 16})

	wire := make(chan []byte, 1)
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, far)
		wire <- buf.Bytes()
	}()

	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perSender; j++ {
				if err := conn.SendChat(fmt.Sprintf("sender-%d", id), text); err != nil {
					t.Errorf("SendChat returned an unexpected error: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	conn.Close()

	var written []byte
	select {
	case written = <-wire:
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out collecting the written bytes")
	}

	lines := strings.Split(strings.TrimSuffix(string(written), "\n"), "\n")
	if len(lines) != senders*perSender {
		t.Fatalf("Wrote %d lines for %d frames: the frames interleaved", len(lines), senders*perSender)
	}
	for i, line := range lines {
		var f Frame
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			t.Fatalf("Line %d is not a whole frame (%.60q): %v", i, line, err)
		}
		var chat Chat
		if err := json.Unmarshal(f.Payload, &chat); err != nil {
			t.Fatalf("Line %d has a garbled payload: %v", i, err)
		}
		if !strings.HasPrefix(chat.From, "sender-") || chat.Text != text {
			t.Fatalf("Line %d arrived garbled: From = %q, Text = %q", i, chat.From, chat.Text)
		}
	}
}

// A closed connection fails writes instead of silently dropping them.
func TestSendFailsOnAClosedConnection(t *testing.T) {
	conn := closedPipeConn(t)

	if err := conn.SendChat("alice", "hello"); err == nil {
		t.Error("SendChat returned no error on a closed connection")
	}
}

// Close shuts down the connection the Conn was built on.
func TestCloseClosesTheUnderlyingConnection(t *testing.T) {
	near, far := net.Pipe()
	defer far.Close()
	discard(t, far)

	conn := NewConn(near)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close returned an unexpected error: %v", err)
	}
	if _, err := near.Write([]byte("x")); err == nil {
		t.Error("The underlying connection still accepts writes after Close")
	}
}

// Frames written by the Send helpers are exactly one newline-terminated JSON
// object each, which is what any other implementation of this protocol relies on.
func TestSendFrameWritesOneLinePerFrame(t *testing.T) {
	near, far := net.Pipe()
	defer near.Close()
	defer far.Close()

	collected := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 0, 1024)
		chunk := make([]byte, 256)
		for {
			n, err := far.Read(chunk)
			buf = append(buf, chunk[:n]...)
			if err != nil || bytes.Count(buf, []byte("\n")) == 2 {
				collected <- buf
				return
			}
		}
	}()

	conn := NewConn(near)
	if err := conn.SendSystem("one"); err != nil {
		t.Fatalf("SendSystem returned an unexpected error: %v", err)
	}
	if err := conn.SendSystem("two"); err != nil {
		t.Fatalf("SendSystem returned an unexpected error: %v", err)
	}

	var wire []byte
	select {
	case wire = <-collected:
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out reading the wire bytes")
	}

	lines := strings.Split(strings.TrimSuffix(string(wire), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("Wrote %d lines for 2 frames: %q", len(lines), wire)
	}
	for i, line := range lines {
		var f Frame
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			t.Errorf("Line %d is not a standalone JSON frame (%q): %v", i, line, err)
		}
	}
}
