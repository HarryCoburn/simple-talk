package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
	"github.com/HarryCoburn/simple-talk/internal/validate"
)

func TestRegisterClient(t *testing.T) {
	chatHub, _ := newTestHub(t)

	// Testing
	wantRoster(t, chatHub)

	// Register a client
	mustRegister(t, chatHub, &Client{Name: "Alice", Out: make(chan protocol.Frame, 1)})

	wantRoster(t, chatHub, "Alice")
}

func TestDeregisterClient(t *testing.T) {
	chatHub, _ := newTestHub(t)

	// Make a client
	chatClient := &Client{Name: "Alice", Out: make(chan protocol.Frame, 1)}

	// Register, then deregister the client
	mustRegister(t, chatHub, chatClient)
	chatHub.Unregister(chatClient)

	// Testing
	wantRoster(t, chatHub)

	// Try double deregistration
	chatHub.Unregister(chatClient)
	wantRoster(t, chatHub)
}

// The name check and the insert are one hub operation, so a name can only be
// claimed once no matter how the registrations interleave.
func TestRegisterRejectsADuplicateName(t *testing.T) {
	chatHub, _ := newTestHub(t)
	first := &Client{Name: "Harry", Out: make(chan protocol.Frame, 1)}
	mustRegister(t, chatHub, first)

	err := chatHub.Register(&Client{Name: "Harry", Out: make(chan protocol.Frame, 1)})
	if !errors.Is(err, ErrNameTaken) {
		t.Fatalf("Wanted ErrNameTaken for a second %q, got %v", "Harry", err)
	}
	wantRoster(t, chatHub, "Harry")

	// The name frees up once its holder leaves.
	chatHub.Unregister(first)
	mustRegister(t, chatHub, &Client{Name: "Harry", Out: make(chan protocol.Frame, 1)})
}

// Racing registrations must not both win. Without an atomic check-and-insert
// both goroutines can see a free name.
func TestRegisterIsAtomicUnderRacingNames(t *testing.T) {
	chatHub, _ := newTestHub(t)

	const racers = 8
	var wg sync.WaitGroup
	results := make(chan error, racers)
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- chatHub.Register(&Client{Name: "Harry", Out: make(chan protocol.Frame, 1)})
		}()
	}
	wg.Wait()
	close(results)

	won := 0
	for err := range results {
		switch {
		case err == nil:
			won++
		case errors.Is(err, ErrNameTaken):
		default:
			t.Errorf("Unexpected registration error: %v", err)
		}
	}
	if won != 1 {
		t.Errorf("Wanted exactly 1 client to claim the name, %d did", won)
	}
	wantRoster(t, chatHub, "Harry")
}

// Test if broadcasting works to connected clients
func TestBroadcast(t *testing.T) {
	chatHub, _ := newTestHub(t)
	client1 := &Client{Name: "Alice", Out: make(chan protocol.Frame, 1)}
	client2 := &Client{Name: "Bob", Out: make(chan protocol.Frame, 1)}

	f := mustChatFrame(t, "Alice", "Test")
	mustRegister(t, chatHub, client1)
	mustRegister(t, chatHub, client2)
	chatHub.Broadcast(f)
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
	client := &Client{Name: "Alice", Out: make(chan protocol.Frame, 1)} // Make a channel with a tiny buffer
	mustRegister(t, chatHub, client)
	frame1 := mustChatFrame(t, "Alice", "Test")
	frame2 := mustChatFrame(t, "Alice", "Received")
	chatHub.Broadcast(frame1) // Fill buffer
	chatHub.Broadcast(frame2) // Overload Client.Out, which should force server to drop the client.

	wantRoster(t, chatHub)

}

func TestWriteLoopWriteAndClose(t *testing.T) {
	in, out := net.Pipe()
	t.Cleanup(func() { out.Close() })
	fullc := protocol.NewConn(in)
	chatClient := newClient(fullc, "Harry")
	chatHub := newHub()
	go chatClient.processClientOutQueue(chatHub)
	peer := protocol.NewConn(out)

	chatClient.Out <- mustChatFrame(t, "Harry", "Hello")
	frame, err := peer.Recv()

	if err != nil {
		t.Fatalf("Recv failure: %v", err)
	}
	if got := chatText(t, frame); got != "Hello" {
		t.Errorf("Client sent Hello, Output pipe got %q", got)
	}

	// Clsoing out stops the write loop but leaves socket alone.
	close(chatClient.Out)
	fullc.Close()
	if _, err = peer.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("Did not receive io.EOF after closing the connection: %T %v", err, err)
	}
}

func TestWriteLoopUnregistersOnWriteError(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.processClientOutQueue(chatHub)

	// Break the socket so the next write fails.
	testHelp.OutConn.Close()
	testHelp.Client.Out <- mustChatFrame(t, "Harry", "Hello")

	var got *Client
	select {
	case got = <-chatHub.unregister:
	case <-time.After(time.Millisecond * 500):
		t.Fatalf("A failed write did not unregister the client")
	}
	if testHelp.Client != got {
		t.Errorf("Did not unregister the correct client. Sent %v, got %v", testHelp.Client, got)
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
	case result = <-chatHub.broadcast:
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out trying to TestReadLoop")
	}
	if got := chatText(t, result); got != "<test> Hello" {
		t.Errorf("TestReadLoop did not receive correct response: %q", got)
	}

}

func TestReadLoopSurvivesOversizedFrame(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.readClientInput(chatHub)

	// A frame past MaxFrameSize is recoverable: protocol.Conn resynchronizes on
	// the trailing newline, so the session must stay alive.
	oversized := append([]byte(`{"kind":"chat","payload":{"from":"test","text":"`), bytes.Repeat([]byte("x"), protocol.MaxFrameSize)...)
	oversized = append(oversized, []byte("\"}}\n")...)
	go func() { testHelp.OutConn.Write(oversized) }()

	f, err := testHelp.Peer.Recv()
	if err != nil {
		t.Fatalf("Could not read the error frame: %v", err)
	}
	if f.Kind != protocol.KindError {
		t.Fatalf("Wanted a %q frame after an oversized frame, got %q", protocol.KindError, f.Kind)
	}

	// The connection is still usable.
	if err := testHelp.Peer.SendChat("test", "Hello"); err != nil {
		t.Fatalf("Could not send the chat: %v", err)
	}
	select {
	case result := <-chatHub.broadcast:
		if got := chatText(t, result); got != "<test> Hello" {
			t.Errorf("Did not receive correct response after an oversized frame: %q", got)
		}
	case <-chatHub.unregister:
		t.Fatalf("Client was disconnected by a recoverable oversized frame")
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out waiting for a chat after an oversized frame")
	}
}

func TestReadLoopIgnoresUnsupportedFrameKinds(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.readClientInput(chatHub)

	// A kind this server does not handle must not tear down the connection.
	if err := testHelp.Peer.SendHandshake("test"); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}

	if err := testHelp.Peer.SendChat("test", "Hello"); err != nil {
		t.Fatalf("Could not send the chat: %v", err)
	}
	select {
	case result := <-chatHub.broadcast:
		if got := chatText(t, result); got != "<test> Hello" {
			t.Errorf("Did not receive correct response after an unsupported kind: %q", got)
		}
	case <-chatHub.unregister:
		t.Fatalf("Client was disconnected by an unsupported frame kind")
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out waiting for a chat after an unsupported kind")
	}
}

func TestReadLoopUnregistersOnCleanDisconnect(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub()
	go testHelp.Client.readClientInput(chatHub)
	testHelp.OutConn.Close()
	var got *Client
	select {
	case got = <-chatHub.unregister:
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
	case got = <-chatHub.unregister:
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
	chatHub.Stop()
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
	chatHub.Stop()
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

	mustRegister(t, chatHub, alice.Client)
	go alice.Client.processClientOutQueue(chatHub)
	go alice.Client.readClientInput(chatHub)

	mustRegister(t, chatHub, bob.Client)
	go bob.Client.processClientOutQueue(chatHub)
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
		mustRegister(t, chatHub, c)
		wg.Add(2)
		go func() { defer wg.Done(); c.processClientOutQueue(chatHub) }()
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

func TestHandleNewConnClosesWhenHubIsDone(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })

	chatHub := newHub()
	chatHub.Stop()

	go handleNewConn(chatHub, serverSide)
	if err := clientSide.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a deadline: %v", err)
	}

	peer := protocol.NewConn(clientSide)
	if err := peer.SendHandshake("Harry"); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}

	// register observes the closed Done, so the selects are deterministic:
	// handleNewConn bails out and closes the connection. A shutting-down server
	// has nobody to explain itself to, so this is a silent hangup, not an error
	// frame.
	if _, err := peer.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("Wanted io.EOF after a post-shutdown handshake, got %v", err)
	}
}

// A refused name is the client's problem to fix, so the server says why before
// hanging up instead of dropping the connection silently.
func TestHandleNewConnReportsATakenName(t *testing.T) {
	chatHub, _ := newTestHub(t)
	mustRegister(t, chatHub, &Client{Name: "Harry", Out: make(chan protocol.Frame, 1)})

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })
	go handleNewConn(chatHub, serverSide)

	if err := clientSide.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("Could not set a deadline: %v", err)
	}
	peer := protocol.NewConn(clientSide)
	if err := peer.SendHandshake("Harry"); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}

	f, err := peer.Recv()
	if err != nil {
		t.Fatalf("Did not receive the rejection: %v", err)
	}
	if f.Kind != protocol.KindError {
		t.Fatalf("Wanted a %q frame for a taken name, got %q", protocol.KindError, f.Kind)
	}
	if _, err := peer.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("Wanted the server to hang up after rejecting, got %v", err)
	}
	wantRoster(t, chatHub, "Harry")
}

// The connect announcement used to be an unguarded send, so a hub that stopped
// between registering and announcing parked this goroutine forever.
func TestHandleNewConnExitsWhenHubStopsMidHandshake(t *testing.T) {
	chatHub, stop := newTestHub(t)

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { serverSide.Close(); clientSide.Close() })

	exited := make(chan struct{})
	go func() { handleNewConn(chatHub, serverSide); close(exited) }()

	peer := protocol.NewConn(clientSide)
	if err := peer.SendHandshake("Harry"); err != nil {
		t.Fatalf("Could not send the handshake: %v", err)
	}
	// Take the ack, then stop the hub. Nothing drains Broadcast after that, so
	// the announcement has only the Done arm left to take.
	if _, err := peer.Recv(); err != nil {
		t.Fatalf("Did not receive the handshake ack: %v", err)
	}
	stop()

	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("handleNewConn did not exit after the hub stopped")
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
	if err := peer.SendHandshake("Harry"); err != nil {
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

// TestDecoratedFrameFitsAFrame guards the budget behind validate.MaxMessageRunes.
// The server decorates a chat frame before fanning it out, and decoration only
// grows it: the sender's name, the format, and JSON escaping all add bytes. If a
// legal message could decorate into an oversized frame, every recipient would
// discard it as ErrFrameTooLarge. "<" is the worst case per rune, since the JSON
// encoder escapes it to <.
func TestDecoratedFrameFitsAFrame(t *testing.T) {
	name := strings.Repeat("<", validate.MaxNameRunes)
	text := strings.Repeat("<", validate.MaxMessageRunes)

	f, err := protocol.NewChatFrame(name, text)
	if err != nil {
		t.Fatalf("Could not build the worst-case chat frame: %v", err)
	}
	decorated, err := decorateChat(name, f)
	if err != nil {
		t.Fatalf("Could not decorate the worst-case chat frame: %v", err)
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(decorated); err != nil {
		t.Fatalf("Could not encode the decorated frame: %v", err)
	}
	if buf.Len() > protocol.MaxFrameSize {
		t.Errorf("The worst-case decorated frame is %d bytes, over the %d limit. Lower validate.MaxMessageRunes.",
			buf.Len(), protocol.MaxFrameSize)
	}
	t.Logf("worst case decorated frame: %d of %d bytes", buf.Len(), protocol.MaxFrameSize)
}

// The server is the authority on names: a hand-written client that skips the
// prompt's rules is rejected at the handshake.
func TestVerifyNameRejectsBadNames(t *testing.T) {
	cases := []struct {
		desc string
		name string
	}{
		{"blank", ""},
		{"only spaces", "   "},
		{"a newline, which would forge chat lines", "Alice\n<Bob> hi"},
		{"an ANSI escape, which writes to other terminals", "Ali\x1b[31mce"},
		{"a zero width rune, which hides a duplicate", "Ali​ce"},
		{"far too long", strings.Repeat("a", validate.MaxNameRunes+1)},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			in, out := net.Pipe()
			t.Cleanup(func() { in.Close(); out.Close() })
			server := protocol.NewConn(in)
			peer := protocol.NewConn(out)

			go func() { peer.SendHandshake(tc.name) }()

			got, err := verifyName(server)
			if err == nil {
				t.Fatalf("verifyName accepted %q as %q, wanted a rejection", tc.name, got)
			}
		})
	}
}

// A name that only differs by case is the same person, so the second one is
// turned away rather than quietly shadowing the first.
func TestRegisterRejectsANameTakenInAnotherCase(t *testing.T) {
	chatHub, _ := newTestHub(t)
	joinRoom(t, chatHub, "Harry")

	second := &Client{Name: "harry", Out: make(chan protocol.Frame, 8)}
	if err := chatHub.Register(second); !errors.Is(err, ErrNameTaken) {
		t.Fatalf("Registering %q against %q returned %v, wanted %v", "harry", "Harry", err, ErrNameTaken)
	}
}

// A message carrying an escape sequence never reaches the room: the sender is
// told, and nobody else sees a frame.
func TestReadClientInputRejectsADisallowedMessage(t *testing.T) {
	testHelp := setUpTest(t)
	testHelp.Client.Name = "Alice"
	chatHub, _ := newTestHub(t)
	mustRegister(t, chatHub, testHelp.Client)
	go testHelp.Client.readClientInput(chatHub)

	if err := testHelp.Peer.SendChat("Alice", "clear\x1b[2J"); err != nil {
		t.Fatalf("Could not send the chat: %v", err)
	}

	f, err := testHelp.Peer.Recv()
	if err != nil {
		t.Fatalf("Expected an error frame back, got: %v", err)
	}
	if f.Kind != protocol.KindError {
		t.Fatalf("Wanted a %q frame back, got %q", protocol.KindError, f.Kind)
	}
	select {
	case got := <-testHelp.Client.Out:
		t.Errorf("The room received %v, wanted nothing", got)
	default:
	}
}
