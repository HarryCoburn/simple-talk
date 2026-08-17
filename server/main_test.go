package main

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

func TestRegisterClient(t *testing.T) {
	chatHub, _ := newTestHub(t)

	// Testing
	if got := chatHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

	// Register a client
	chatHub.Register <- &Client{Out: make(chan protocol.Frame, 1)}

	if got := chatHub.clientCount(); got != 1 {
		t.Errorf("Expected length 1, got %d", got)
	}

}

func TestDeregisterClient(t *testing.T) {
	chatHub, _ := newTestHub(t)

	// Make a client
	chatClient := &Client{Out: make(chan protocol.Frame, 1)}

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
	client1 := &Client{Name: "Alice", Out: make(chan protocol.Frame, 1)}
	client2 := &Client{Name: "Bob", Out: make(chan protocol.Frame, 1)}

	f := mustChatFrame(t, "Alice", "Test")
	chatHub.Register <- client1
	chatHub.Register <- client2
	chatHub.Broadcast <- f
	var f1 protocol.Frame
	var f2 protocol.Frame
	select {
	case m := <-client1.Out:
		f1 = m
	case <-time.After(time.Millisecond * 100):
		t.Fatal("Timed out getting result for read1")
	}
	select {
	case m := <-client2.Out:
		f2 = m
	case <-time.After(time.Millisecond * 100):
		t.Fatal("Timed out getting result for read2")
	}
	if got := chatText(t, f1); got != "Test" {
		t.Errorf("client1 got %q, want %q", got, "Test")
	}
	if got := chatText(t, f2); got != "Test" {
		t.Errorf("client1 got %q, want %q", got, "Test")
	}
}

func TestDroppedClientPath(t *testing.T) {
	chatHub, _ := newTestHub(t)
	client := &Client{Out: make(chan protocol.Frame, 1)} // Make a channel with a tiny buffer
	chatHub.Register <- client
	frame1 := mustChatFrame(t, "Alice", "Test")
	frame2 := mustChatFrame(t, "Alice", "Received")
	chatHub.Broadcast <- frame1 // Fill buffer
	chatHub.Broadcast <- frame2 // Overload Client.Out, which should force server to drop the client.

	if got := chatHub.clientCount(); got != 0 {
		t.Errorf("Expected length 0, got %d", got)
	}

}

func TestWriteLoopWriteAndClose(t *testing.T) {
	in, out := net.Pipe()
	t.Cleanup(func() { out.Close() })
	fullc := protocol.NewConn(in)
	chatClient := newClient(fullc, "Harry")
	go chatClient.processClientOutQueue()
	peer := protocol.NewConn(out)

	chatClient.Out <- mustChatFrame(t, "Harry", "Hello")
	frame, err := peer.Recv()

	if err != nil {
		t.Fatalf("Recv failure: %v", err)
	}
	if got := chatText(t, frame); got != "Hello" {
		t.Errorf("Client sent Hello, Output pipe got %q", got)
	}

	// Closing Out stops the write loop but leaves the socket alone; the hub owns
	// the connection and closes it as part of closeClient.
	close(chatClient.Out)
	fullc.Close()
	if _, err = peer.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("Did not receive io.EOF after closing the connection: %T %v", err, err)
	}
}

func TestReadLoop(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.readClientInput(chatHub)
	if err := testHelp.Peer.SendChat("test", "Hello"); err != nil {
		t.Fatalf("Could not send the chat: %v", err)
	}
	var result protocol.Frame
	select {
	case result = <-chatHub.Broadcast:
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out trying to TestReadLoop")
	}
	if got := chatText(t, result); got != "<test> Hello" {
		t.Errorf("TestReadLoop did not receive correct response: %q", got)
	}

}

func TestReadLoopUnregistersOnCleanDisconnect(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.readClientInput(chatHub)
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
	go testHelp.Client.readClientInput(chatHub)
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
	go func() { testHelp.Client.readClientInput(chatHub); close(exited) }()
	// Nothing is draining chatHub.Broadcast, so readClientInput parks in the
	// send select until Done releases it.
	if err := testHelp.Peer.SendChat("test", "Hello"); err != nil {
		t.Fatalf("Could not send the chat: %v", err)
	}
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
	go func() { testHelp.Client.readClientInput(chatHub); close(exited) }()
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
	go alice.Client.processClientOutQueue()
	go alice.Client.readClientInput(chatHub)

	chatHub.Register <- bob.Client
	go bob.Client.processClientOutQueue()
	go bob.Client.readClientInput(chatHub)

	// Bound the reads, so a regression fails this test instead of hanging the
	// whole binary until the package timeout panics.
	deadline := time.Now().Add(2 * time.Second)
	alice.OutConn.SetDeadline(deadline)
	bob.OutConn.SetDeadline(deadline)

	if err := alice.Peer.SendChat("Alice", "Hello"); err != nil {
		t.Fatalf("Alice could not send: %v", err)
	}

	frame, err := bob.Peer.Recv()
	if err != nil {
		t.Fatalf("Bob did not receive the message: %v", err)
	}
	if got := chatText(t, frame); got != "<Alice> Hello" {
		t.Errorf("Did not receive end to end message. Received: %q", got)
	}
	// Echo check
	frame, err = alice.Peer.Recv()
	if err != nil {
		t.Fatalf("Alice did not receive the echo: %v", err)
	}
	if got := chatText(t, frame); got != "<Alice> Hello" {
		t.Errorf("Did not receive end to end message on echo. Received: %q", got)
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
		go func() { defer wg.Done(); c.processClientOutQueue() }()
		go func() { defer wg.Done(); c.readClientInput(chatHub) }()
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
	// instead of hanging the binary until the package timeout panics.
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a read deadline: %v", err)
	}
	peer := protocol.NewConn(conn)
	if err := peer.SendHandshake("Harry"); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}
	ackFrame, err := peer.Recv()

	if err != nil {
		t.Fatalf("Did not receive the handshake ack: %v", err)
	}
	if ackFrame.Kind != protocol.KindHandshakeAck {
		t.Fatalf("Wanted a %q frame, got %q", protocol.KindHandshakeAck, ackFrame.Kind)
	}
	var ack protocol.HandshakeAck
	if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
		t.Fatalf("Could not unpack the ack payload: %v", err)
	}

	if ack.Name != "Harry" {
		t.Errorf("Wanted the name %q back, got %q", "Harry", ack.Name)
	}

	// Chat sent on the same connection the handshake used. verifyName and
	// readClientInput share one protocol.Conn, so a chat that arrived in the
	// same read as the handshake is already sitting in that bufio.Reader and is
	// not lost.
	if err := peer.SendChat("Harry", "Hello"); err != nil {
		t.Fatalf("Could not send the chat: %v", err)
	}

	frame, err := peer.Recv()
	if err != nil {
		t.Fatalf("Did not receive the broadcast: %v", err)
	}

	if got := systemText(t, frame); got != "Harry has connected." {
		t.Errorf("Wanted %q, got %q", "Harry has connected.", got)
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

func TestHandleNewConnClosesWhenHubIsDone(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })

	chatHub := newHub()
	close(chatHub.Done)

	go handleNewConn(chatHub, serverSide)
	if err := clientSide.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a deadline: %v", err)
	}

	peer := protocol.NewConn(clientSide)
	if err := peer.SendHandshake("Harry"); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}

	// nameTaken and Register both observe a closed Done, so the selects are
	// deterministic: handleNewConn bails out and closes the connection.
	if _, err := peer.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("Wanted io.EOF after a post-shutdown handshake, got %v", err)
	}
}
