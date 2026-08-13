package main

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func newTestHub() *Hub {
	hub := newHub()

	// Start the chathub goroutine
	go hub.run()
	return hub
}

func TestRegisterClient(t *testing.T) {
	newHub := newTestHub()

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
	newHub := newTestHub()

	// Make a pipe connection
	in, _ := net.Pipe()

	// Make a client
	newClient := newClient(in)

	// Register, then deregister the client
	newHub.Register <- newClient
	newHub.Unregister <- newClient

	// Testing
	if got := newHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

}

// Test if broadcasting works to connected clients
func TestBroadcast(t *testing.T) {
	newHub := newTestHub()
	client1 := &Client{Out: make(chan []byte, 1)}
	client2 := &Client{Out: make(chan []byte, 1)}
	payload := []byte("Test")
	newHub.Register <- client1
	newHub.Register <- client2
	newHub.Broadcast <- payload
	var read1 []byte
	var read2 []byte
	select {
	case read1 = <-client1.Out:
	case <-time.After(time.Millisecond * 100):
		t.Fatal("Timed out getting result for read1")
	}
	select {
	case read2 = <-client2.Out:
	case <-time.After(time.Millisecond * 100):
		t.Fatal("Timed out getting result for read2")
	}
	if !bytes.Equal(read1, payload) {
		t.Errorf("client1 got %q, want %q", read1, payload)
	}
	if !bytes.Equal(read2, payload) {
		t.Errorf("client2 got %q, want %q", read2, payload)
	}
}
