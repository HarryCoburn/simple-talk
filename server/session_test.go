package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
	"github.com/HarryCoburn/simple-talk/internal/validate"
)

// Tests for a single client's read and write goroutines.

func TestWriteLoopWriteAndClose(t *testing.T) {
	in, out := net.Pipe()
	t.Cleanup(func() { out.Close() })
	fullc := protocol.NewConn(in)
	chatClient := newSession(fullc, "Harry")
	chatHub := newHub(context.Background(), protocol.ProtocolVersion)
	go chatClient.writeLoop(chatHub)
	peer := protocol.NewConn(out)

	chatClient.Out <- mustChatFrame(t, "Harry", "Hello")
	frame, err := peer.Recv()

	if err != nil {
		t.Fatalf("Recv failure: %v", err)
	}
	if got := chatText(t, frame); got != "Hello" {
		t.Errorf("Session sent Hello, Output pipe got %q", got)
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
	chatHub := newHub(context.Background(), protocol.ProtocolVersion)
	go testHelp.Session.writeLoop(chatHub)

	// Break the socket so the next write fails.
	testHelp.OutConn.Close()
	testHelp.Session.Out <- mustChatFrame(t, "Harry", "Hello")

	var got *Session
	select {
	case got = <-chatHub.unregister:
	case <-time.After(time.Millisecond * 500):
		t.Fatalf("A failed write did not unregister the client")
	}
	if testHelp.Session != got {
		t.Errorf("Did not unregister the correct client. Sent %v, got %v", testHelp.Session, got)
	}
}

func TestReadLoop(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub(context.Background(), protocol.ProtocolVersion)
	go testHelp.Session.readInput(chatHub)
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
	chatHub := newHub(context.Background(), protocol.ProtocolVersion)
	go testHelp.Session.readInput(chatHub)

	// A frame past MaxFrameSize is recoverable: protocol.Conn resynchronizes on
	// the trailing newline, so the session must stay alive.
	oversized := append([]byte(`{"kind":"chat","payload":{"from":"test","text":"`), bytes.Repeat([]byte("x"), protocol.MaxFrameSize)...)
	oversized = append(oversized, []byte("\"}}\n")...)
	inBackground(t, "the oversized frame", func() error {
		_, err := testHelp.OutConn.Write(oversized)
		return err
	})

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
		t.Fatalf("Session was disconnected by a recoverable oversized frame")
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out waiting for a chat after an oversized frame")
	}
}

func TestReadLoopIgnoresUnsupportedFrameKinds(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub(context.Background(), protocol.ProtocolVersion)
	go testHelp.Session.readInput(chatHub)

	// A kind this server does not handle must not tear down the connection.
	if err := testHelp.Peer.SendHandshake("test", protocol.ProtocolVersion); err != nil {
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
		t.Fatalf("Session was disconnected by an unsupported frame kind")
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out waiting for a chat after an unsupported kind")
	}
}

func TestReadLoopUnregistersOnCleanDisconnect(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub(context.Background(), protocol.ProtocolVersion)
	go testHelp.Session.readInput(chatHub)
	testHelp.OutConn.Close()
	var got *Session
	select {
	case got = <-chatHub.unregister:
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out trying to TestReadLoopUnregister")
	}
	if testHelp.Session != got {
		t.Errorf("Did not unregister the correct client. Sent %v, got %v", testHelp.Session, got)
	}
}

func TestReadLoopUnregisteresOnBrokenConnection(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub(context.Background(), protocol.ProtocolVersion)
	go testHelp.Session.readInput(chatHub)
	testHelp.InConn.Close()
	var got *Session
	select {
	case got = <-chatHub.unregister:
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Timed out trying to TestReadLoopClosingInChan")
	}
	if testHelp.Session != got {
		t.Errorf("Did not unregister the correct client. Sent %v, got %v", testHelp.Session, got)
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

// A message carrying an escape sequence never reaches the room: the sender is
// told, and nobody else sees a frame.
func TestReadClientInputRejectsADisallowedMessage(t *testing.T) {
	testHelp := setUpTest(t)
	testHelp.Session.Name = "Alice"
	chatHub, _ := newTestHub(t)
	mustRegister(t, chatHub, testHelp.Session)
	go testHelp.Session.readInput(chatHub)

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
	case got := <-testHelp.Session.Out:
		t.Errorf("The room received %v, wanted nothing", got)
	default:
	}
}

// A panic in a command handler is one client's problem. Without a recover on the
// read goroutine it unwinds past main and every other session dies with it.
func TestReadLoopSurvivesAPanickingHandler(t *testing.T) {
	logs := captureLog(t)

	// Register a handler that panics. commandNames caches the roster on its
	// first call, so prime it before adding this one: otherwise the cache can
	// capture "boom" and /help reports it for the rest of the run.
	_ = commandNames()
	commands["boom"] = func(cmdEnv) error { panic("handler exploded") }
	t.Cleanup(func() { delete(commands, "boom") })

	chatHub, _ := newTestHub(t)
	victim := setUpTest(t)
	victim.Session.Name = "Alice"
	mustRegister(t, chatHub, victim.Session)
	bystander := joinRoom(t, chatHub, "Bob")

	go victim.Session.readInput(chatHub)
	if err := victim.Peer.SendCommand("boom", nil); err != nil {
		t.Fatalf("Could not send the command: %v", err)
	}

	// The panicking client is dropped, which the room hears about. Waiting on
	// that announcement also means the hub has finished the unregister.
	if got, want := systemText(t, nextReply(t, bystander)), "Alice has disconnected."; got != want {
		t.Fatalf("The room was told %q, wanted %q", got, want)
	}
	wantRoster(t, chatHub, "Bob")

	// Bob is untouched: the room still delivers.
	if err := chatHub.broadcastFrame(mustChatFrame(t, "Bob", "still here")); err != nil {
		t.Fatalf("Could not broadcast after the panic: %v", err)
	}
	if got := chatText(t, nextReply(t, bystander)); got != "still here" {
		t.Errorf("Bob received %q after the panic, wanted %q", got, "still here")
	}

	if !strings.Contains(logs.String(), "panic in the read loop for Alice") {
		t.Errorf("The panic was not logged. Log was: %q", logs.String())
	}
}

// The write goroutine needs its own recover: one deferred on the read goroutine
// cannot see a panic raised here.
func TestWriteLoopSurvivesAPanic(t *testing.T) {
	logs := captureLog(t)
	chatHub, _ := newTestHub(t)

	// No Conn, so the first write panics on a nil dereference.
	broken := &Session{Name: "Alice", Out: make(chan protocol.Frame, 1)}
	mustRegister(t, chatHub, broken)
	bystander := joinRoom(t, chatHub, "Bob")

	go broken.writeLoop(chatHub)
	broken.Out <- mustChatFrame(t, "Alice", "boom")

	if got, want := systemText(t, nextReply(t, bystander)), "Alice has disconnected."; got != want {
		t.Fatalf("The room was told %q, wanted %q", got, want)
	}
	wantRoster(t, chatHub, "Bob")

	if !strings.Contains(logs.String(), "panic in the write loop for Alice") {
		t.Errorf("The panic was not logged. Log was: %q", logs.String())
	}
}
