package server

import (
	"encoding/json"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

func TestEndtoEnd(t *testing.T) {
	alice := setUpTest(t)
	alice.Session.Name = "Alice"
	bob := setUpTest(t)
	bob.Session.Name = "Bob"
	chatHub, _ := newTestHub(t) // newTestHub() starts hub.run()

	mustRegister(t, chatHub, alice.Session)
	go alice.Session.writeLoop(chatHub)
	go alice.Session.readInput(chatHub)

	mustRegister(t, chatHub, bob.Session)
	go bob.Session.writeLoop(chatHub)
	go bob.Session.readInput(chatHub)

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
	if err := peer.SendHandshake("Harry", serverVersion); err != nil {
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
	// readInput share one protocol.Conn, so a chat that arrived in the
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

	wantRoster(t, chatHub, "Harry")
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

// A shutdown signal tears down the whole server: serve returns, the listener
// stops accepting, and connected clients are closed rather than left hanging.
func TestServeShutsDownOnSignal(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open the listener: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	addr := ln.Addr().String()

	signals := make(chan os.Signal, 1)
	returned := make(chan struct{})
	go func() { defer close(returned); serve(ln, signals) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Could not dial the listener: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a read deadline: %v", err)
	}
	peer := protocol.NewConn(conn)
	if err := peer.SendHandshake("Harry", serverVersion); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}
	if _, err := peer.Recv(); err != nil {
		t.Fatalf("Did not receive the handshake ack: %v", err)
	}

	signals <- syscall.SIGTERM

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return after the shutdown signal.")
	}

	// The hub finished its teardown before serve returned, so Harry's socket
	// is closed and the port is free again. Frames already queued for Harry
	// (his own connect announcement) still read out first.
	closed := false
	for range 8 {
		if _, err := peer.Recv(); err != nil {
			closed = true
			break
		}
	}
	if !closed {
		t.Error("Harry's connection was still open after shutdown.")
	}
	if _, err := net.Dial("tcp", addr); err == nil {
		t.Error("The listener still accepted a connection after shutdown.")
	}
}

// If the accept loop stops on its own, serve tears the hub down instead of
// blocking forever on a signal that is never coming.
func TestServeShutsDownWhenTheListenerDies(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open the listener: %v", err)
	}

	returned := make(chan struct{})
	go func() { defer close(returned); serve(ln, make(chan os.Signal)) }()

	ln.Close()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return after the listener closed.")
	}
}
