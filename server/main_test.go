package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/signal"
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
	go acceptLoop(context.Background(), chatHub, ln, stopped)

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
	if err := peer.SendHandshake("Harry", protocol.ProtocolVersion); err != nil {
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
	go acceptLoop(context.Background(), chatHub, ln, stopped)

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

// A cancelled context tears down the whole server: serve returns, the listener
// stops accepting, and connected clients are closed rather than left hanging.
//
// This no longer covers the signal itself. Turning SIGTERM into a cancelled
// context is signal.NotifyContext's job inside Run, which nothing tests yet.
func TestServeShutsDownWhenTheContextIsCancelled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open the listener: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	returned := make(chan struct{})
	go func() { defer close(returned); serve(ctx, ln) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Could not dial the listener: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a read deadline: %v", err)
	}
	peer := protocol.NewConn(conn)
	if err := peer.SendHandshake("Harry", protocol.ProtocolVersion); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}
	if _, err := peer.Recv(); err != nil {
		t.Fatalf("Did not receive the handshake ack: %v", err)
	}

	cancel()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return after the context was cancelled.")
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
// blocking forever on a cancellation that is never coming.
func TestServeShutsDownWhenTheListenerDies(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open the listener: %v", err)
	}

	returned := make(chan struct{})
	go func() { defer close(returned); serve(context.Background(), ln) }()

	ln.Close()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return after the listener closed.")
	}
}

// run owns its listener, so an address already in use is the one failure it can
// hit before there is anything to shut down. It has to come back as an error
// the caller can act on: log.Fatal here would take the process down on the
// library's behalf, and returning nil would let cmd/server exit 0 on a server
// that never started.
//
// The address is a parameter, so this no longer has to gamble on a fixed port
// being free and skip when it is not.
func TestRunReturnsAnErrorWhenTheAddressIsInUse(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open the blocking listener: %v", err)
	}
	t.Cleanup(func() { blocker.Close() })

	err = run(context.Background(), blocker.Addr().String(), signal.NotifyContext)
	if err == nil {
		t.Fatal("run returned nil against an address already taken, wanted an error")
	}
	// The cause travels with it: the caller can tell a port clash from anything
	// else that stops a listener opening.
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Errorf("run returned %v, wanted it to carry a *net.OpError", err)
	}
}

// A connection accepted after teardown began has nowhere to go: the hub is
// closing sessions, not registering them. The accept loop hands it back rather
// than starting a handshake that cannot finish.
func TestAcceptLoopRefusesAConnectionOnceCancelled(t *testing.T) {
	chatHub, _ := newTestHub(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open the listener: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	// Cancelled before the loop starts, so the first accepted connection takes
	// the guard. The listener stays open: closing it is the other exit path,
	// and this test is about the ctx one.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stopped := make(chan struct{})
	go acceptLoop(ctx, chatHub, ln, stopped)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Could not dial the listener: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("acceptLoop did not exit after its context was cancelled")
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a read deadline: %v", err)
	}
	if _, err := protocol.NewConn(conn).Recv(); err == nil {
		t.Error("The accepted connection stayed open after cancellation, wanted it closed")
	}
}

// Which signals the server listens for is part of its contract. A fake notify
// records the request, so this holds without the test binary receiving one.
func TestRunAsksForInterruptAndSIGTERM(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open the listener: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	var got []os.Signal
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	notify := func(parent context.Context, sigs ...os.Signal) (context.Context, context.CancelFunc) {
		got = append(got, sigs...)
		return ctx, cancel
	}

	returned := make(chan error, 1)
	go func() { returned <- runOn(context.Background(), ln, notify) }()

	// notify runs before serve, so a listener that accepts proves it was called.
	waitUntilServing(t, ln.Addr().String())
	cancel()

	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("runOn returned %v, wanted nil after a clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runOn did not return after its notify context was cancelled")
	}

	// Safe to read: receiving from returned happens after the write.
	want := []os.Signal{os.Interrupt, syscall.SIGTERM}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("The server asked to be notified of %v, wanted %v", got, want)
	}
}

// waitUntilServing blocks until the address accepts a connection, so a test
// does not race the goroutine that is still starting the server.
func waitUntilServing(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Nothing was accepting on %s", addr)
}
