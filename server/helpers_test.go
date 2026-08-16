package main

import (
	"encoding/json"
	"net"
	"sync"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// Contains helper functions for testing.
type testHelper struct {
	Client  *Client        // The server-side client under test
	Peer    *protocol.Conn // The far side of the pipe, speaking the real protocol
	InConn  net.Conn       // Raw server side, for tests that close it directly
	OutConn net.Conn       // Raw peer side, for tests that close it directly
}

// Builds a chat frame, failing the test if the payload will not marshal.
func mustChatFrame(t *testing.T, from string, text string) protocol.Frame {
	t.Helper()
	f, err := protocol.NewChatFrame(from, text)
	if err != nil {
		t.Fatalf("Could not build a chat frame: %v", err)
	}
	return f
}

// Unpacks a frame that is expected to be a chat, returning its text.
func chatText(t *testing.T, f protocol.Frame) string {
	t.Helper()
	if f.Kind != protocol.KindChat {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindChat, f.Kind)
	}
	var chat protocol.Chat
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		t.Fatalf("Could not unpack the chat payload: %v", err)
	}
	return chat.Text
}

func newTestHub(t *testing.T) (*Hub, func()) {
	chatHub := newHub()

	// Start the chathub goroutine
	go chatHub.run()
	var once sync.Once
	stop := func() {
		once.Do(func() { close(chatHub.Done) })
		<-chatHub.Finished
	}
	t.Cleanup(stop)
	return chatHub, stop
}

func setUpTest(t *testing.T) testHelper {
	in, out := net.Pipe()
	conn := protocol.NewConn(in)
	chatClient := newClient(conn, "test")
	peer := protocol.NewConn(out)
	t.Cleanup(func() {
		out.Close()
		in.Close()
		conn.Close()
	})
	return testHelper{
		Client:  chatClient,
		Peer:    peer,
		InConn:  in,
		OutConn: out}
}
