package protocol

import (
	"encoding/json"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

// testPipe wires two Conns together over an in-memory connection so a test can
// send from one end and receive on the other without touching a real socket.
type testPipe struct {
	A *Conn
	B *Conn
}

func newTestPipe(t *testing.T) testPipe {
	t.Helper()
	sideA, sideB := net.Pipe()
	a, b := NewConn(sideA), NewConn(sideB)
	t.Cleanup(func() {
		a.Close()
		b.Close()
	})
	return testPipe{A: a, B: b}
}

// rawConn returns a Conn whose reader is fed the exact bytes given, so tests can
// hand Recv input that the Send helpers would never produce. The write runs in a
// goroutine because net.Pipe is unbuffered. The write side stays open until the
// test ends: net.Pipe refuses deadline calls once a side is closed.
func rawConn(t *testing.T, data []byte) *Conn {
	t.Helper()
	return rawConnWith(t, data, false)
}

// rawConnEOF is rawConn for tests that need the stream to end after the given
// bytes, such as a frame that is cut off before its delimiter.
func rawConnEOF(t *testing.T, data []byte) *Conn {
	t.Helper()
	return rawConnWith(t, data, true)
}

func rawConnWith(t *testing.T, data []byte, closeAfterWrite bool) *Conn {
	t.Helper()
	readSide, writeSide := net.Pipe()
	conn := NewConn(readSide)
	t.Cleanup(func() {
		conn.Close()
		writeSide.Close()
	})

	go func() {
		writeSide.Write(data)
		if closeAfterWrite {
			writeSide.Close()
		}
	}()
	return conn
}

// recvOne reads a single frame with a deadline so a hung read fails the test
// instead of hanging the suite.
func recvOne(t *testing.T, c *Conn) (Frame, error) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set the read deadline: %v", err)
	}
	defer c.SetReadDeadline(time.Time{})
	return c.Recv()
}

// mustRecv reads a frame of the wanted kind, failing the test on any error.
func mustRecv(t *testing.T, c *Conn, want Kind) Frame {
	t.Helper()
	f, err := recvOne(t, c)
	if err != nil {
		t.Fatalf("Recv returned an unexpected error: %v", err)
	}
	if f.Kind != want {
		t.Fatalf("Received a %q frame, wanted %q", f.Kind, want)
	}
	return f
}

// payloadInto unpacks a frame's payload into v, failing the test if it cannot.
func payloadInto(t *testing.T, f Frame, v any) {
	t.Helper()
	if err := json.Unmarshal(f.Payload, v); err != nil {
		t.Fatalf("Could not unpack the %q payload %s: %v", f.Kind, f.Payload, err)
	}
}

// closedPipeConn returns a Conn whose peer has already hung up, so writes fail
// and reads report the close.
func closedPipeConn(t *testing.T) *Conn {
	t.Helper()
	near, far := net.Pipe()
	conn := NewConn(near)
	t.Cleanup(func() { conn.Close() })
	if err := far.Close(); err != nil {
		t.Fatalf("Could not close the far side of the pipe: %v", err)
	}
	return conn
}

// discard keeps a pipe drained so a writer under test never blocks.
func discard(t *testing.T, c net.Conn) {
	t.Helper()
	go io.Copy(io.Discard, c)
}

// chunkedWrites is a net.Conn that breaks each Write into small pieces and
// yields between them, mimicking a socket that accepts a partial write. It
// widens the window in which two unsynchronized senders could interleave.
type chunkedWrites struct {
	net.Conn
	chunk int
}

func (c chunkedWrites) Write(b []byte) (int, error) {
	written := 0
	for written < len(b) {
		end := min(written+c.chunk, len(b))
		n, err := c.Conn.Write(b[written:end])
		written += n
		if err != nil {
			return written, err
		}
		runtime.Gosched()
	}
	return written, nil
}
