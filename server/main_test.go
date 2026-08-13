package main

import (
	"net"
	"testing"
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
