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

// testPipe is a Client protocol.Conn directly wired to Peer protocol.Conn over an in-memory
// connection to avoid using real sockets.
type testPipe struct {
	Client *protocol.Conn // The test connection
	Peer   *protocol.Conn // The mocked server
}

// newTestPipe does what it says on the tin. Makes a testPipe struct, wires it up, and closes it cleanly.
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

// scannerOf stands in for os.Stdin in testing.
// If the separator ever changes, this will need an update.
func scannerOf(lines ...string) *bufio.Scanner {
	if len(lines) == 0 {
		return bufio.NewScanner(strings.NewReader(""))
	}
	return bufio.NewScanner(strings.NewReader(strings.Join(lines, "\n") + "\n"))
}

// captureStdout reroutes os.Stdout temporarily to an os.Pipe we can
// control and check.
// TODO: Replace this with something better. Redirecting a process global
// like this is dangerous.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Could not created the stdoutpipe: %v", err)
	}
	original := os.Stdout
	os.Stdout = w

	// Drain to prevent deadlocks
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

// handshakeName reads a KindHandshake frame from the peer and returns the name it carries
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

// chatFrom unpacks a KindChat frame, returning From and Text fields.
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

// commandFrom unpacks a KindCommand frame, returning its Name and Args.
func commandFrom(t *testing.T, f protocol.Frame) (string, []string) {
	t.Helper()
	if f.Kind != protocol.KindCommand {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindCommand, f.Kind)
	}
	var cmd protocol.Command
	if err := json.Unmarshal(f.Payload, &cmd); err != nil {
		t.Fatalf("Could not unpack the command payload: %v", err)
	}
	return cmd.Name, cmd.Args
}

// waitClosed fails tests if c is not closed within a short grace period.
// Keeps a broken goroutine from hanging the whole suite.
func waitClosed(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for %s to close", what)
	}
}
