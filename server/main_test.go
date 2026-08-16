package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestRegisterClient(t *testing.T) {
	chatHub, _ := newTestHub(t)

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
	chatHub, _ := newTestHub(t)

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
	chatHub, _ := newTestHub(t)
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
	chatHub, _ := newTestHub(t)
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
		t.Fatalf("ReadString failure: %v", err)
	}
	if line != "Hello\n" {
		t.Errorf("Client sent Hello, Output pipe got %v", line)
	}
	close(chatClient.Out)
	_, err = reader.ReadString('\n')
	if !errors.Is(err, io.EOF) {
		t.Errorf("Did not receive io.EOF after closing Out: %T %v", err, err)
	}
}

func TestReadLoop(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.readLoop(chatHub)
	fmt.Fprintln(testHelp.OutConn, "Hello")
	var result Message
	select {
	case result = <-chatHub.Broadcast:
	case <-time.After(time.Millisecond * 100):
		t.Errorf("Timed out trying to TestReadLoop")
	}
	if string(result.Data) != "Hello\n" {
		t.Errorf("TestReadLoop did not receive correct response: %q", string(result.Data))
	}
}

func TestReadLoopUnregistersOnCleanDisconnect(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.readLoop(chatHub)
	testHelp.OutConn.Close()
	var got *Client
	select {
	case got = <-chatHub.Unregister:
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out trying to TestReadLoopUnregister")
	}
	if testHelp.Client != got {
		t.Errorf("Did not unregister the correct client. Sent %v, got %v", testHelp.Client, got)
	}
}

func TestReadLoopUnregisteresOnBrokenConnection(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.readLoop(chatHub)
	testHelp.InConn.Close()
	var got *Client
	select {
	case got = <-chatHub.Unregister:
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out trying to TestReadLoopClosingInChan")
	}
	if testHelp.Client != got {
		t.Errorf("Did not unregister the correct client. Sent %v, got %v", testHelp.Client, got)
	}
}

func TestDoneGuardsBroadcastSend(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	exited := make(chan struct{})
	go func() { testHelp.Client.readLoop(chatHub); close(exited) }()
	fmt.Fprintln(testHelp.OutConn, "Hello")
	close(chatHub.Done)
	select {
	case <-exited:
		return
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Sending to Done channel did not close Client")
	}
}

func TestDoneGuardsUnregister(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	exited := make(chan struct{})
	go func() { testHelp.Client.readLoop(chatHub); close(exited) }()
	close(chatHub.Done)
	testHelp.OutConn.Close()
	select {
	case <-exited:
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Sending to Done channel did not close Client")
	}
}

func TestEndtoEnd(t *testing.T) {
	alice := setUpTest(t)
	alice.Client.Name = "Alice"
	bob := setUpTest(t)
	bob.Client.Name = "Bob"
	chatHub, _ := newTestHub(t) // newTestHub() starts hub.run()

	chatHub.Register <- alice.Client
	go alice.Client.writeLoop()
	go alice.Client.readLoop(chatHub)

	chatHub.Register <- bob.Client
	go bob.Client.writeLoop()
	go bob.Client.readLoop(chatHub)

	fmt.Fprintln(alice.OutConn, "Hello")
	result, err := bob.Reader.ReadString('\n')
	if result != "<Alice> Hello\n" {
		t.Errorf("Did not receive end to end message. Received: %q", err)
	}
	// Echo check
	result, err = alice.Reader.ReadString('\n')
	if result != "<Alice> Hello\n" {
		t.Errorf("Did not receive end to end message on echo. Received: %q", err)
	}

}

func TestTeardownCascade(t *testing.T) {
	alice := setUpTest(t)
	alice.Client.Name = "Alice"
	bob := setUpTest(t)
	bob.Client.Name = "Bob"
	chatHub, stop := newTestHub(t) // newTestHub() starts hub.run()

	var wg sync.WaitGroup
	for _, c := range []*Client{alice.Client, bob.Client} {
		chatHub.Register <- c
		wg.Add(2)
		go func() { defer wg.Done(); c.writeLoop() }()
		go func() { defer wg.Done(); c.readLoop(chatHub) }()
	}

	allDone := make(chan struct{})
	go func() { wg.Wait(); close(allDone) }()

	stop()
	select {
	case <-allDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Goroutines still running after Done was closed.")
	}

	if _, err := alice.OutConn.Write([]byte("x\n")); err == nil {
		t.Error("Alice's connection was not closed.")
	}
	if _, err := bob.OutConn.Write([]byte("x\n")); err == nil {
		t.Error("Bob's connection was not closed.")
	}
}

func TestAcceptLoop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Coult not open the listener: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	chatHub, _ := newTestHub(t)
	stopped := make(chan struct{})
	go acceptLoop(chatHub, ln, stopped)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Could not dial the listener: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	// Bound every read, so a stalled server fails this test in two seconds
	// instead of hanging the binary until the ten second panic.
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a read deadline: %v", err)
	}
	reader := bufio.NewReader(conn)

	prompt, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Did not receive the username prompt: %v", err)
	}
	if prompt != userNamePrompt {
		t.Fatalf("Wanted prompt %q, got %q", userNamePrompt, prompt)
	}

	// Pipelined deliberately. "Hello\n" can fall into the same bufio.Reader
	// that setUserName reads from. readLoop has to inherit that reader rather than
	// construct its own.
	fmt.Fprintln(conn, "Harry")
	fmt.Fprintln(conn, "Hello")

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Errorf("Did not receive the broadcast: %v", err)
	}

	if line != "<Harry> Hello\n" {
		t.Errorf("Wanted %q, got %q", "<Harry> Hello\n", line)
	}

	if got := chatHub.clientCount(); got != 1 {
		t.Errorf("Wanted 1 registered client. got %d", got)
	}
}

func TestAcceptLoopExitsWhenListenerCloses(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Something went wrong opening the listener: %v", err)
	}
	chatHub, _ := newTestHub(t)
	stopped := make(chan struct{})
	go acceptLoop(chatHub, ln, stopped)

	// conn, err := net.Dial("tcp", ln.Addr().String())
	// if err != nil {
	// 	t.Fatalf("Something went wrong dialing the listener: %v", err)
	// }
	// t.Cleanup(func() { conn.Close() })
	ln.Close()

	select {
	case <-stopped:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("acceptLoop did not exit after the listener closed.")
	}
}

func TestHandleConnClosesWhenHubIsDone(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })

	chatHub := newHub()
	close(chatHub.Done)

	go handleConn(chatHub, serverSide)
	if err := clientSide.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a deadline: %v", err)
	}

	reader := bufio.NewReader(clientSide)

	prompt, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Did not receive the username prompt: %v", err)
	}
	if prompt != userNamePrompt {
		t.Fatalf("Wanted prompt %q, got %q", userNamePrompt, prompt)
	}

	fmt.Fprintln(clientSide, "Harry")
	// Register has no receiver and Done is closed, so the select is deterministic:
	// handleConn takes the Done branch and closes the connection.
	if _, err := reader.ReadString('\n'); !errors.Is(err, io.EOF) {
		t.Errorf("Wanted io.EOF after a post-shutdown handshake, got %v", err)
	}
}
