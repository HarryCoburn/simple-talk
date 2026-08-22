package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// registerNamed makes a Conn-less client and puts it in the hub. Command
// handling never touches the socket, only the Out queue, so a nil Conn is safe
// here and hub.closeClient guards against it.
func registerNamed(t *testing.T, hub *Hub, name string) *Client {
	t.Helper()
	c := newClient(nil, name)
	mustRegister(t, hub, c)
	return c
}

func TestWhoListsEveryoneToTheCallerOnly(t *testing.T) {
	hub, _ := newTestHub(t)
	caller := registerNamed(t, hub, "zoe")
	other := registerNamed(t, hub, "adam")

	if err := dispatchCommand(caller, hub, mustCommandFrame(t, "who")); err != nil {
		t.Fatalf("dispatchCommand: %v", err)
	}

	got := systemText(t, nextFrame(t, caller))
	want := "Connected (2): adam, zoe" // Sorted, so this is stable.
	if got != want {
		t.Errorf("who replied %q, want %q", got, want)
	}
	wantNoFrame(t, other)
}

func TestHelpListsRegisteredCommands(t *testing.T) {
	hub, _ := newTestHub(t)
	caller := registerNamed(t, hub, "zoe")

	if err := dispatchCommand(caller, hub, mustCommandFrame(t, "help")); err != nil {
		t.Fatalf("dispatchCommand: %v", err)
	}

	got := systemText(t, nextFrame(t, caller))
	for name := range commands {
		if !strings.Contains(got, name) {
			t.Errorf("help reply %q does not mention %q", got, name)
		}
	}
}

func TestCommandNamesAreCaseAndSpaceInsensitive(t *testing.T) {
	hub, _ := newTestHub(t)
	caller := registerNamed(t, hub, "zoe")

	if err := dispatchCommand(caller, hub, mustCommandFrame(t, "  WhO  ")); err != nil {
		t.Fatalf("dispatchCommand: %v", err)
	}

	if got := systemText(t, nextFrame(t, caller)); !strings.HasPrefix(got, "Connected") {
		t.Errorf("Expected a who reply, got %q", got)
	}
}

func TestUnknownCommandErrorsToTheCallerOnly(t *testing.T) {
	hub, _ := newTestHub(t)
	caller := registerNamed(t, hub, "zoe")
	other := registerNamed(t, hub, "adam")

	if err := dispatchCommand(caller, hub, mustCommandFrame(t, "nope")); err != nil {
		t.Fatalf("dispatchCommand: %v", err)
	}

	if got := errorText(t, nextFrame(t, caller)); !strings.Contains(got, "nope") {
		t.Errorf("Error %q does not name the bad command", got)
	}
	wantNoFrame(t, other)
}

func TestEmptyCommandNameErrors(t *testing.T) {
	hub, _ := newTestHub(t)
	caller := registerNamed(t, hub, "zoe")

	if err := dispatchCommand(caller, hub, mustCommandFrame(t, "   ")); err != nil {
		t.Fatalf("dispatchCommand: %v", err)
	}

	if got := errorText(t, nextFrame(t, caller)); got != "no command given" {
		t.Errorf("Got error %q, want %q", got, "no command given")
	}
}

func TestDispatchCommandRejectsMalformedPayload(t *testing.T) {
	hub, _ := newTestHub(t)
	caller := registerNamed(t, hub, "zoe")

	bad := protocol.Frame{Kind: protocol.KindCommand, Payload: json.RawMessage(`{"name":`)}
	if err := dispatchCommand(caller, hub, bad); err == nil {
		t.Fatal("Expected an error from a malformed command payload")
	}
	wantNoFrame(t, caller)
}

// A command run against a hub that is already shutting down must return rather
// than block on the closed hub.
func TestCommandOnStoppedHubDoesNotBlock(t *testing.T) {
	hub, stop := newTestHub(t)
	caller := registerNamed(t, hub, "zoe")
	stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := dispatchCommand(caller, hub, mustCommandFrame(t, "who")); err != nil {
			t.Errorf("dispatchCommand: %v", err)
		}
	}()
	waitClosedOrFail(t, done)
}
