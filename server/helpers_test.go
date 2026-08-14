package main

import (
	"bufio"
	"net"
	"sync"
	"testing"
)

// Contains helper functions for testing.
type testHelper struct {
	Client  *Client
	Reader  *bufio.Reader
	InConn  net.Conn
	OutConn net.Conn
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
	chatClient := newClient(in)
	reader := bufio.NewReader(out)
	t.Cleanup(func() {
		out.Close()
		in.Close()
	})
	return testHelper{
		Client:  chatClient,
		Reader:  reader,
		InConn:  in,
		OutConn: out}
}
