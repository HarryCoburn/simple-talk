package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

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
			send: func(c *Conn) error { return c.SendHandshake("alice") },
			kind: KindHandshake,
			check: func(t *testing.T, f Frame) {
				var hs Handshake
				payloadInto(t, f, &hs)
				if hs.Name != "alice" {
					t.Errorf("Received the name %q, wanted %q", hs.Name, "alice")
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

// Frames are newline delimited, so several sent back to back are read back one
// at a time and in order.
func TestRecvReadsFramesInOrder(t *testing.T) {
	pipe := newTestPipe(t)

	go func() {
		pipe.A.SendChat("alice", "first")
		pipe.A.SendChat("alice", "second")
		pipe.A.SendChat("alice", "third")
	}()

	for _, want := range []string{"first", "second", "third"} {
		var chat Chat
		payloadInto(t, mustRecv(t, pipe.B, KindChat), &chat)
		if chat.Text != want {
			t.Errorf("Received %q, wanted %q", chat.Text, want)
		}
	}
}

// Recv copies the payload out of the read buffer. The two frames below are
// written as one stream and together overflow the buffer, so reading the second
// makes bufio slide its buffer over where the first frame's bytes were sitting.
// Without the copy in Recv, the caller's first payload is corrupted by that slide.
func TestRecvPayloadSurvivesTheNextRead(t *testing.T) {
	first, err := NewChatFrame("alice", strings.Repeat("a", MaxFrameSize/2))
	if err != nil {
		t.Fatalf("Could not build the first frame: %v", err)
	}
	second, err := NewChatFrame("bob", strings.Repeat("b", MaxFrameSize/2))
	if err != nil {
		t.Fatalf("Could not build the second frame: %v", err)
	}

	var stream bytes.Buffer
	enc := json.NewEncoder(&stream)
	if err := enc.Encode(first); err != nil {
		t.Fatalf("Could not encode the first frame: %v", err)
	}
	if err := enc.Encode(second); err != nil {
		t.Fatalf("Could not encode the second frame: %v", err)
	}
	conn := rawConn(t, stream.Bytes())

	got, err := recvOne(t, conn)
	if err != nil {
		t.Fatalf("The first Recv returned an unexpected error: %v", err)
	}
	held := string(got.Payload)

	if _, err := recvOne(t, conn); err != nil {
		t.Fatalf("The second Recv returned an unexpected error: %v", err)
	}

	if string(got.Payload) != held {
		t.Errorf("The first payload was overwritten by the next read.\n got: %.80s\nwant: %.80s", got.Payload, held)
	}
	var chat Chat
	payloadInto(t, got, &chat)
	if chat.From != "alice" {
		t.Errorf("The first frame now reads as being from %q, wanted alice", chat.From)
	}
}

// Malformed input is rejected as a bad frame rather than surfacing as a
// half-decoded one.
func TestRecvRejectsMalformedFrames(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{name: "empty line", input: "\n"},
		{name: "blank line with a carriage return", input: "\r\n"},
		{name: "not json", input: "this is not json\n"},
		{name: "truncated json", input: `{"kind":"chat"` + "\n"},
		{name: "json that is not an object", input: "[1,2,3]\n"},
		{name: "missing kind", input: `{"payload":{"text":"hi"}}` + "\n"},
		{name: "empty kind", input: `{"kind":"","payload":{"text":"hi"}}` + "\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := recvOne(t, rawConn(t, []byte(tc.input)))
			if !errors.Is(err, ErrBadFrame) {
				t.Fatalf("Recv(%q) returned frame %+v and error %v, wanted ErrBadFrame", tc.input, f, err)
			}
		})
	}
}

// A trailing carriage return is part of the delimiter, not the JSON.
func TestRecvAcceptsCarriageReturnLineEndings(t *testing.T) {
	conn := rawConn(t, []byte(`{"kind":"system","payload":{"text":"hi"}}`+"\r\n"))

	var sys System
	payloadInto(t, mustRecv(t, conn, KindSystem), &sys)
	if sys.Text != "hi" {
		t.Errorf("Received the text %q, wanted %q", sys.Text, "hi")
	}
}

// A frame with no payload at all is still a valid frame; the kind carries the meaning.
func TestRecvAcceptsAFrameWithoutAPayload(t *testing.T) {
	conn := rawConn(t, []byte(`{"kind":"handshake"}`+"\n"))

	f := mustRecv(t, conn, KindHandshake)
	if f.Payload != nil {
		t.Errorf("Payload = %s, wanted nil", f.Payload)
	}
}

// One bad frame does not poison the stream: the reader stays in sync and the
// next well-formed frame is read normally.
func TestRecvStaysInSyncAfterABadFrame(t *testing.T) {
	conn := rawConn(t, []byte("not json\n"+`{"kind":"chat","payload":{"from":"bob","text":"still here"}}`+"\n"))

	if _, err := recvOne(t, conn); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("Wanted ErrBadFrame for the first line, got %v", err)
	}

	var chat Chat
	payloadInto(t, mustRecv(t, conn, KindChat), &chat)
	if chat.Text != "still here" {
		t.Errorf("Received %q, wanted %q", chat.Text, "still here")
	}
}

// A frame that fills the buffer but still ends within the discard budget is
// rejected as too large, and the reader recovers at the following frame.
func TestRecvRejectsAnOversizedFrame(t *testing.T) {
	oversized := strings.Repeat("x", MaxFrameSize+2000)
	conn := rawConn(t, []byte(oversized+"\n"+`{"kind":"system","payload":{"text":"after"}}`+"\n"))

	if _, err := recvOne(t, conn); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Wanted ErrFrameTooLarge for an oversized frame, got %v", err)
	}

	var sys System
	payloadInto(t, mustRecv(t, conn, KindSystem), &sys)
	if sys.Text != "after" {
		t.Errorf("Received %q, wanted %q: the reader did not recover", sys.Text, "after")
	}
}

// Past the discard budget the reader gives up on finding the delimiter: the
// stream is desynchronized and the connection is no longer usable.
func TestRecvGivesUpOnAnUnboundedFrame(t *testing.T) {
	flood := strings.Repeat("x", maxDiscardSize+MaxFrameSize)
	conn := rawConn(t, []byte(flood+"\n"))

	if _, err := recvOne(t, conn); !errors.Is(err, ErrUnrecoverable) {
		t.Fatalf("Wanted ErrUnrecoverable for a frame past the discard budget, got %v", err)
	}
}

// A frame right up against the size limit is still legal and must round trip.
func TestRecvAcceptsAFrameAtTheSizeLimit(t *testing.T) {
	pipe := newTestPipe(t)

	// Leave room for the JSON envelope and the newline.
	text := strings.Repeat("x", MaxFrameSize-128)
	go pipe.A.SendChat("alice", text)

	var chat Chat
	payloadInto(t, mustRecv(t, pipe.B, KindChat), &chat)
	if chat.Text != text {
		t.Errorf("The round tripped text was %d bytes, wanted %d", len(chat.Text), len(text))
	}
}

// Text that needs escaping (quotes, newlines, non-ASCII) survives the round trip
// without breaking the newline framing.
func TestRecvHandlesTextNeedingEscapes(t *testing.T) {
	cases := map[string]string{
		"embedded newline":   "line one\nline two",
		"embedded quotes":    `she said "hello"`,
		"backslashes":        `C:\path\to\file`,
		"non-ascii":          "café 🙂 日本語",
		"carriage return":    "one\r\ntwo",
		"json looking text":  `{"kind":"error","payload":{"message":"spoofed"}}`,
		"empty text is fine": "",
	}

	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			pipe := newTestPipe(t)
			go pipe.A.SendChat("alice", text)

			var chat Chat
			payloadInto(t, mustRecv(t, pipe.B, KindChat), &chat)
			if chat.Text != text {
				t.Errorf("Received %q, wanted %q", chat.Text, text)
			}
		})
	}
}

// Text that only looks like a frame is delivered as text, not obeyed as a frame.
func TestRecvDoesNotLetChatTextSpoofAFrame(t *testing.T) {
	pipe := newTestPipe(t)
	go pipe.A.SendChat("alice", `{"kind":"error","payload":{"message":"spoofed"}}`)

	mustRecv(t, pipe.B, KindChat) // the spoof arrives as one chat frame, not an error frame

	// Nothing else was written, so a second read must time out rather than
	// produce the frame the text was imitating.
	if err := pipe.B.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("Could not set the read deadline: %v", err)
	}
	if f, err := pipe.B.Recv(); err == nil {
		t.Errorf("A second frame %+v arrived, wanted only the one chat frame", f)
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

// A peer hanging up shows up as a read error, which is what the read loops use
// to know they are done.
func TestRecvReportsAClosedConnection(t *testing.T) {
	pipe := newTestPipe(t)
	pipe.A.Close()

	// No deadline here: net.Pipe rejects deadline calls once the peer is gone,
	// and a closed pipe fails the read immediately anyway.
	if f, err := pipe.B.Recv(); err == nil {
		t.Errorf("Recv returned %+v and no error after the peer hung up, wanted an error", f)
	}
}

// A half-written frame is a read error, not a frame: the delimiter never arrived.
func TestRecvReportsATruncatedStream(t *testing.T) {
	conn := rawConnEOF(t, []byte(`{"kind":"chat","payload":{"from":"a"`)) // no newline, then EOF

	// No deadline: the closed write side ends the read on its own.
	if f, err := conn.Recv(); err == nil {
		t.Errorf("Recv returned %+v and no error for a truncated stream, wanted an error", f)
	}
}

// The read deadline is what lets the server bound a handshake.
func TestSetReadDeadlineTimesOutARead(t *testing.T) {
	pipe := newTestPipe(t)

	if err := pipe.B.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline returned an unexpected error: %v", err)
	}

	_, err := pipe.B.Recv()
	if err == nil {
		t.Fatal("Recv returned no error, wanted a timeout")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("Recv returned %v, wanted a deadline error", err)
	}
}

// Clearing the deadline lets a later frame through.
func TestClearingTheReadDeadlineResumesReads(t *testing.T) {
	pipe := newTestPipe(t)

	if err := pipe.B.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline returned an unexpected error: %v", err)
	}
	if _, err := pipe.B.Recv(); err == nil {
		t.Fatal("Wanted the first read to time out")
	}
	if err := pipe.B.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("Clearing the read deadline returned an unexpected error: %v", err)
	}

	go pipe.A.SendSystem("after the deadline")

	var sys System
	payloadInto(t, mustRecv(t, pipe.B, KindSystem), &sys)
	if sys.Text != "after the deadline" {
		t.Errorf("Received %q, wanted %q", sys.Text, "after the deadline")
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
