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
	Client  *Client        // server-side client
	Peer    *protocol.Conn // Far side of the pipe
	InConn  net.Conn
	OutConn net.Conn
}

// Builds a chat frame
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

// Unpacks a frame that is expected to be a system, returning its text.
func systemText(t *testing.T, f protocol.Frame) string {
	t.Helper()
	if f.Kind != protocol.KindSystem {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindSystem, f.Kind)
	}
	var chat protocol.System
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		t.Fatalf("Could not unpack the chat payload: %v", err)
	}
	return chat.Text
}

// Registers a client, failing the test if the hub refuses it.
func mustRegister(t *testing.T, hub *Hub, c *Client) {
	t.Helper()
	if err := hub.register(c); err != nil {
		t.Fatalf("Could not register %q: %v", c.Name, err)
	}
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
