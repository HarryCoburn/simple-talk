package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// Helpers shared by the client tests.

// testPipe is a client Conn wired to a stand-in server (Peer) over an
// in-memory connection, so the tests never touch a real socket.
type testPipe struct {
	Client *protocol.Conn // the connection the code under test uses
	Peer   *protocol.Conn // the fake server on the other end
}

// newTestPipe builds a connected client/server pair and closes both when the
// test finishes. net.Pipe is synchronous: every Send blocks until the other
// side reads, so tests must keep the peer moving (usually in a goroutine).
func newTestPipe(t *testing.T) testPipe {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	client := protocol.NewConn(clientSide)
	peer := protocol.NewConn(serverSide)
	t.Cleanup(func() {
		client.Close()
		peer.Close()
	})
	return testPipe{Client: client, Peer: peer}
}

// scannerOf turns test input into the *bufio.Scanner the client reads from,
// standing in for os.Stdin.
func scannerOf(lines ...string) *bufio.Scanner {
	if len(lines) == 0 {
		return bufio.NewScanner(strings.NewReader(""))
	}
	return bufio.NewScanner(strings.NewReader(strings.Join(lines, "\n") + "\n"))
}

// captureStdout runs fn with os.Stdout redirected and returns everything it
// printed. The client writes user-facing output with fmt, so this is how the
// tests assert on what the user would have seen.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Could not create the stdout pipe: %v", err)
	}
	original := os.Stdout
	os.Stdout = w

	// Drain in the background so a chatty fn can't fill the pipe and deadlock.
	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		io.Copy(&sb, r)
		done <- sb.String()
	}()

	defer func() {
		os.Stdout = original
		w.Close()
	}()
	fn()
	os.Stdout = original
	w.Close()
	return <-done
}

// handshakeName reads one frame from the peer and returns the name it carries.
func handshakeName(t *testing.T, f protocol.Frame) string {
	t.Helper()
	if f.Kind != protocol.KindHandshake {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindHandshake, f.Kind)
	}
	var hs protocol.Handshake
	if err := json.Unmarshal(f.Payload, &hs); err != nil {
		t.Fatalf("Could not unpack the handshake payload: %v", err)
	}
	return hs.Name
}

// chatFrom unpacks a frame expected to be a chat, returning sender and text.
func chatFrom(t *testing.T, f protocol.Frame) (string, string) {
	t.Helper()
	if f.Kind != protocol.KindChat {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindChat, f.Kind)
	}
	var chat protocol.Chat
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		t.Fatalf("Could not unpack the chat payload: %v", err)
	}
	return chat.From, chat.Text
}

// waitClosed fails the test if c is not closed within a short grace period.
// Keeps a broken goroutine from hanging the whole suite.
func waitClosed(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for %s to close", what)
	}
}
