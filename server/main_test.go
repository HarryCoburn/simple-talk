package main

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"
)

func newTestHub(t *testing.T) *Hub {
	hub := newHub()

	// Start the chathub goroutine
	go hub.run()
	t.Cleanup(func() { close(hub.Done) })
	return hub
}

func TestRegisterClient(t *testing.T) {
	newHub := newTestHub(t)

	// Testing
	if got := newHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

	// Make a pipe connection
	in, _ := net.Pipe()

	// Register a client
	newHub.Register <- newClient(in)

	if got := newHub.clientCount(); got != 1 {
		t.Errorf("Expected length 1, got %d", got)
	}

}

func TestDeregisterClient(t *testing.T) {
	newHub := newTestHub(t)

	// Make a pipe connection
	in, _ := net.Pipe()

	// Make a client
	newClient := newClient(in)

	// Register, then deregister the client
	newHub.Register <- newClient
	newHub.Unregister <- newClient

	// Testing
	if got := newHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0 after de-registration, got %d", got)
	}

	// Try double deregistration
	newHub.Unregister <- newClient

	if got := newHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0 after double de-registration, got %d", got)
	}

}

// Test if broadcasting works to connected clients
func TestBroadcast(t *testing.T) {
	newHub := newTestHub(t)
	client1 := &Client{Out: make(chan Message, 1)}
	client2 := &Client{Out: make(chan Message, 1)}
	payload := []byte("Test")
	msg := Message{
		From: client1,
		Data: payload,
	}
	newHub.Register <- client1
	newHub.Register <- client2
	newHub.Broadcast <- msg
	var read1 []byte
	var read2 []byte
	select {
	case m := <-client1.Out:
		read1 = m.Data
	case <-time.After(time.Millisecond * 100):
		t.Fatal("Timed out getting result for read1")
	}
	select {
	case m := <-client2.Out:
		read2 = m.Data
	case <-time.After(time.Millisecond * 100):
		t.Fatal("Timed out getting result for read2")
	}
	if !bytes.Equal(read1, fmt.Appendf(nil, "<> %s", payload)) {
		t.Errorf("client1 got %q, want %q", read1, fmt.Appendf(nil, "<> %s", payload))
	}
	if !bytes.Equal(read2, fmt.Appendf(nil, "<> %s", payload)) {
		t.Errorf("client1 got %q, want %q", read2, fmt.Appendf(nil, "<> %s", payload))
	}
}

func TestDroppedClientPath(t *testing.T) {
	newHub := newTestHub(t)
	client := &Client{Out: make(chan Message, 1)} // Make a channel with a tiny buffer
	newHub.Register <- client
	payload1 := []byte("Test")
	payload2 := []byte("Received")
	newHub.Broadcast <- Message{From: client, Data: payload1} // Fill buffer
	newHub.Broadcast <- Message{From: client, Data: payload2} // Overload Client.Out, which should force server to drop the client.

	if got := newHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

}
