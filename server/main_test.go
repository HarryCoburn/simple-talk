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
	// for this test.
	rCh <- &Client{}

	if got := clientCount(t, qCh); got != 1 {
		t.Errorf("Expected length 1, got %d", got)
	}

	// uCh <- &Client{}
	// if got := clientCount(t, qCh); got != 0 {
	// 	t.Errorf("Expected length 0, got %d", got)
	// }

}

func clientCount(t *testing.T, qCh chan<- ClientNumReq) int {
	t.Helper()
	ch := make(chan int, 1)
	qCh <- ClientNumReq{reply: ch}
	select {
	case numClients := <-ch:
		return numClients
	case <-time.After(time.Millisecond * 500):
		t.Fatalf("Timed out trying to get the number of clients.")
	}
	return -1
}
