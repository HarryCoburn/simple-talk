package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestRegisterClient(t *testing.T) {
	chatHub := newTestHub(t)

	// Testing
	if got := chatHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

	// Register a client
	chatHub.Register <- &Client{Out: make(chan Message, 1)}

	if got := chatHub.clientCount(); got != 1 {
		t.Errorf("Expected length 1, got %d", got)
	}

}

func TestDeregisterClient(t *testing.T) {
	chatHub := newTestHub(t)

	// Make a client
	chatClient := &Client{Out: make(chan Message, 1)}

	// Register, then deregister the client
	chatHub.Register <- chatClient
	chatHub.Unregister <- chatClient

	// Testing
	if got := chatHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0 after de-registration, got %d", got)
	}

	// Try double deregistration
	chatHub.Unregister <- chatClient

	if got := chatHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0 after double de-registration, got %d", got)
	}

}

// Test if broadcasting works to connected clients
func TestBroadcast(t *testing.T) {
	chatHub := newTestHub(t)
	client1 := &Client{Name: "Alice", Out: make(chan Message, 1)}
	client2 := &Client{Name: "Bob", Out: make(chan Message, 1)}
	payload := []byte("Test")
	msg := Message{
		From: client1,
		Data: payload,
	}
	chatHub.Register <- client1
	chatHub.Register <- client2
	chatHub.Broadcast <- msg
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
	if !bytes.Equal(read1, []byte("<Alice> Test")) {
		t.Errorf("client1 got %q, want %q", read1, []byte("<Alice> Test"))
	}
	if !bytes.Equal(read2, []byte("<Alice> Test")) {
		t.Errorf("client2 got %q, want %q", read2, []byte("<Bob> Test"))
	}
}

func TestDroppedClientPath(t *testing.T) {
	chatHub := newTestHub(t)
	client := &Client{Out: make(chan Message, 1)} // Make a channel with a tiny buffer
	chatHub.Register <- client
	payload1 := []byte("Test")
	payload2 := []byte("Received")
	chatHub.Broadcast <- Message{From: client, Data: payload1} // Fill buffer
	chatHub.Broadcast <- Message{From: client, Data: payload2} // Overload Client.Out, which should force server to drop the client.

	if got := chatHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

}

func TestWriteLoopWriteAndClose(t *testing.T) {
	in, out := net.Pipe()
	t.Cleanup(func() { out.Close() })
	chatClient := newClient(in)
	go chatClient.writeLoop()
	reader := bufio.NewReader(out)
	msg := Message{From: chatClient, Data: []byte("Hello\n")}
	chatClient.Out <- msg
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString failure.")
	}
	if line != "Hello\n" {
		t.Errorf("Client sent Hello, Output pipe got %v", line)
	}
	close(chatClient.Out)
	line, err = reader.ReadString('\n')
	if errors.Is(err, io.EOF) {
		t.Errorf("Did not receive io.EOF after closing Out: %v", err)
	}
}

// Helpers

func newTestHub(t *testing.T) *Hub {
	chatHub := newHub()

	// Start the chathub goroutine
	go chatHub.run()
	t.Cleanup(func() { close(chatHub.Done) })
	return chatHub
}
