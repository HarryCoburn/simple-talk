package main

import (
	"testing"
	"time"
)

func TestRegisterClient(t *testing.T) {
	// Make channels
	rCh := make(chan *Client)
	uCh := make(chan *Client)
	bCh := make(chan []byte)
	qCh := make(chan ClientNumReq)

	// Start the chathub goroutine
	go runChatHub(rCh, uCh, bCh, qCh)

	// Testing
	if got := clientCount(t, qCh); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

	// Okay to mock a nil client because we are not touching what is inside
	// for this test. However, we will mock a connection momentarily.
	rCh <- &Client{}

	if got := clientCount(t, qCh); got != 1 {
		t.Errorf("Expected length 1, got %d", got)
	}

}

// Helper for creating the channel connections into a goroutine
func clientCount(t *testing.T, qCh chan<- ClientNumReq) int {
	t.Helper()                     // Helps with test dumps
	ch := make(chan int, 1)        // It's a single query, so make its size 1
	qCh <- ClientNumReq{reply: ch} // Wrap the reply channel into the request so the goroutine can return without exiting.
	select {
	case numClients := <-ch: // ch replied
		return numClients
	case <-time.After(time.Millisecond * 500): // ch waited too long
		t.Fatalf("Timed out trying to get the number of clients.")
	}
	return -1 // Marker of a bad result.
}
