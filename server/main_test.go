package main

import (
	"bufio"
	"net"
	"testing"
)

func TestRegisterClient(t *testing.T) {
	newHub := newTestHub()

	// Testing
	if got := newHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

	// Make a pipe connection
	in, _ := net.Pipe()

	// Register a client
	newHub.Register <- &Client{
		Connection: in,
		Reader:     bufio.NewReader(in),
	}

	if got := newHub.clientCount(); got != 1 {
		t.Errorf("Expected length 1, got %d", got)
	}

}

func newTestHub() Hub {
	newHub := newHub()

	// Start the chathub goroutine
	go newHub.run()
	return newHub
}

// func TestDeregisterClient(t *testing.T) {
// 	// Make channels
// 	rCh := make(chan *Client)
// 	uCh := make(chan *Client)
// 	bCh := make(chan []byte)
// 	qCh := make(chan ClientNumReq)

// 	// Start the chathub goroutine
// 	go runChatHub(rCh, uCh, bCh, qCh)

// 	// Make a pipe connection
// 	in, _ := net.Pipe()

// 	// Make a client
// 	newClient := Client{
// 		Connection: in,
// 		Reader:     bufio.NewReader(in),
// 	}

// 	// Register, then deregister the client
// 	rCh <- &newClient
// 	uCh <- &newClient

// 	// Testing
// 	if got := clientCount(t, qCh); got != 0 {
// 		t.Errorf("Expected length 0, got %d", got)
// 	}

// }
