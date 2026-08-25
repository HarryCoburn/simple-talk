package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// fakeHub stands in for the hub so a handler can be run against a roster the
// live hub will not produce. Nothing here talks to a goroutine: the point of the
// narrow interface is that a handler only needs these three answers.
type fakeHub struct {
	names    []string
	namesErr error
	sent     []protocol.Frame
}

func (f *fakeHub) sendTo(_ *Client, frame protocol.Frame) error {
	f.sent = append(f.sent, frame)
	return nil
}

func (f *fakeHub) broadcastFrame(protocol.Frame) error {
	return nil
}

func (f *fakeHub) clientNames() ([]string, error) { return f.names, f.namesErr }

func (f *fakeHub) Broadcast(frame protocol.Frame) error {
	f.sent = append(f.sent, frame)
	return nil
}

// runCommand dispatches a command frame as if the client had sent it.
func runCommand(t *testing.T, hub commandHub, c *Client, name string, args ...string) error {
	t.Helper()
	f, err := protocol.NewCommandFrame(name, args)
	if err != nil {
		t.Fatalf("Could not build the %q command frame: %v", name, err)
	}
	return dispatchCommand(c, hub, f)
}

// who runs /who as the given client and returns the text of the reply.
func who(t *testing.T, hub *Hub, c *Client) string {
	t.Helper()
	if err := runCommand(t, hub, c, "who"); err != nil {
		t.Fatalf("/who returned an unexpected error: %v", err)
	}
	return systemText(t, nextReply(t, c))
}

// An empty roster is a real answer, not an error: the caller can be dropped
// between issuing the command and the hub reading the list. A live hub always has
// the caller on it, so this path needs a stand in hub to reach.
func TestCmdWhoReportsAnEmptyRoster(t *testing.T) {
	hub := &fakeHub{}
	caller := &Client{Name: "Alice", Out: make(chan protocol.Frame, 1)}

	if err := runCommand(t, hub, caller, "who"); err != nil {
		t.Fatalf("/who returned an unexpected error: %v", err)
	}
	if len(hub.sent) != 1 {
		t.Fatalf("/who sent %d frames, wanted 1", len(hub.sent))
	}
	if got, want := systemText(t, hub.sent[0]), "Nobody is connected."; got != want {
		t.Errorf("/who said %q, wanted %q", got, want)
	}
}

// A roster the hub could not produce is reported, not answered with a misleading
// empty room.
func TestCmdWhoSurfacesARosterError(t *testing.T) {
	hub := &fakeHub{namesErr: ErrHubClosed}
	caller := &Client{Name: "Alice", Out: make(chan protocol.Frame, 1)}

	if err := runCommand(t, hub, caller, "who"); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("/who returned %v, wanted %v", err, ErrHubClosed)
	}
	if len(hub.sent) != 0 {
		t.Errorf("/who sent %v, wanted nothing said to the caller", hub.sent)
	}
}

// /who is how a user sees who is connected, and it reports the roster in sorted
// order with a count.
func TestCmdWhoListsEveryoneConnected(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")
	joinRoom(t, chatHub, "Carol")
	joinRoom(t, chatHub, "Bob")

	if got, want := who(t, chatHub, alice), "Connected (3): Alice, Bob, Carol"; got != want {
		t.Errorf("/who said %q, wanted %q", got, want)
	}
}

// The count /who reports follows the hub: it is the same state registration and
// unregistration change.
func TestCmdWhoFollowsTheHubRoster(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")

	if got, want := who(t, chatHub, alice), "Connected (1): Alice"; got != want {
		t.Errorf("/who said %q, wanted %q", got, want)
	}

	bob := joinRoom(t, chatHub, "Bob")
	if got, want := who(t, chatHub, alice), "Connected (2): Alice, Bob"; got != want {
		t.Errorf("/who said %q after Bob joined, wanted %q", got, want)
	}

	chatHub.unregisterClient(bob)
	// Bob leaving is announced to the room, so that frame is ahead of the reply
	// in Alice's queue.
	if got, want := systemText(t, nextReply(t, alice)), "Bob has disconnected."; got != want {
		t.Errorf("The room was told %q, wanted %q", got, want)
	}
	if got, want := who(t, chatHub, alice), "Connected (1): Alice"; got != want {
		t.Errorf("/who said %q after Bob left, wanted %q", got, want)
	}
}

// A dropped client is gone from the roster too: the hub state /who reads is the
// same one the delivery path prunes.
func TestCmdWhoDropsAStalledClient(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")
	stalled := &Client{Name: "Bob", Out: make(chan protocol.Frame, 1)}
	mustRegister(t, chatHub, stalled)

	// Two broadcasts overrun Bob's one-frame buffer, so the hub drops him.
	chatHub.broadcastFrame(mustChatFrame(t, "Alice", "one"))
	chatHub.broadcastFrame(mustChatFrame(t, "Alice", "two"))

	// Alice's own buffer now holds those broadcasts; clear them so her /who
	// reply is the next frame out.
	<-alice.Out
	<-alice.Out

	if got, want := who(t, chatHub, alice), "Connected (1): Alice"; got != want {
		t.Errorf("/who said %q, wanted the dropped client gone: %q", got, want)
	}
}

// A reply is for the caller alone. Nobody else in the room sees it.
func TestCommandRepliesGoOnlyToTheCaller(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")
	bob := joinRoom(t, chatHub, "Bob")

	who(t, chatHub, alice)
	wantNoReply(t, bob)
}

// /help lists the commands the server actually has, sorted.
func TestCmdHelpListsTheCommandTable(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")

	if err := runCommand(t, chatHub, alice, "help"); err != nil {
		t.Fatalf("/help returned an unexpected error: %v", err)
	}

	got := systemText(t, nextReply(t, alice))
	prefix := "Commands: "
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("/help said %q, wanted it to start with %q", got, prefix)
	}
	listed := strings.Split(strings.TrimPrefix(got, prefix), ", ")
	if len(listed) != len(commands) {
		t.Errorf("/help listed %v, wanted all %d commands", listed, len(commands))
	}
	for _, name := range listed {
		if _, ok := commands[name]; !ok {
			t.Errorf("/help listed %q, which is not a command", name)
		}
	}
	for i := 1; i < len(listed); i++ {
		if listed[i-1] > listed[i] {
			t.Errorf("/help listed %v out of order", listed)
			break
		}
	}
}

// Command names are matched after trimming and lowercasing, so the user's
// spacing and capitalization do not decide whether a command is found.
func TestDispatchNormalizesTheCommandName(t *testing.T) {
	for _, name := range []string{"who", "WHO", "Who", "  who  ", "\twho\n"} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			chatHub, _ := newTestHub(t)
			alice := joinRoom(t, chatHub, "Alice")

			if err := runCommand(t, chatHub, alice, name); err != nil {
				t.Fatalf("Dispatching %q returned an unexpected error: %v", name, err)
			}
			if got, want := systemText(t, nextReply(t, alice)), "Connected (1): Alice"; got != want {
				t.Errorf("%q replied %q, wanted %q", name, got, want)
			}
		})
	}
}

// An unknown command is the user's mistake, not a server error: they are told,
// and dispatch reports no error.
func TestDispatchReportsAnUnknownCommand(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")

	if err := runCommand(t, chatHub, alice, "shrug", "hard"); err != nil {
		t.Fatalf("An unknown command returned an unexpected error: %v", err)
	}

	got := errorText(t, nextReply(t, alice))
	if !strings.Contains(got, `"shrug"`) {
		t.Errorf("The reply %q does not name the command the user typed", got)
	}
	if !strings.Contains(got, "help") {
		t.Errorf("The reply %q does not point the user at help", got)
	}
}

// An empty command name is reported to the caller rather than looked up.
func TestDispatchReportsAnEmptyCommandName(t *testing.T) {
	for _, name := range []string{"", "   "} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			chatHub, _ := newTestHub(t)
			alice := joinRoom(t, chatHub, "Alice")

			if err := runCommand(t, chatHub, alice, name); err != nil {
				t.Fatalf("An empty command returned an unexpected error: %v", err)
			}
			if got, want := errorText(t, nextReply(t, alice)), "no command given"; got != want {
				t.Errorf("The reply was %q, wanted %q", got, want)
			}
		})
	}
}

// A command frame whose payload is not a command is a protocol fault, so it
// comes back as an error to the caller of dispatch rather than a reply.
func TestDispatchRejectsAMalformedPayload(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")

	f := protocol.Frame{Kind: protocol.KindCommand, Payload: []byte(`"who"`)}
	if err := dispatchCommand(alice, chatHub, f); err == nil {
		t.Fatal("Dispatching a malformed payload returned no error")
	}
	wantNoReply(t, alice)
}

// Everything after the command word reaches the handler untouched.
func TestDispatchPassesArgumentsToTheHandler(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")

	got := make(chan cmdCtx, 1)
	commands["echo"] = func(ctx cmdCtx) error {
		got <- ctx
		return nil
	}
	t.Cleanup(func() { delete(commands, "echo") })

	if err := runCommand(t, chatHub, alice, "echo", "Bob", "Hello There"); err != nil {
		t.Fatalf("Dispatch returned an unexpected error: %v", err)
	}

	ctx := <-got
	if ctx.client != alice {
		t.Errorf("The handler ran for %v, wanted the calling client", ctx.client)
	}
	if ctx.hub != chatHub {
		t.Error("The handler was given a different hub than the caller's")
	}
	want := []string{"Bob", "Hello There"}
	if len(ctx.args) != len(want) || ctx.args[0] != want[0] || ctx.args[1] != want[1] {
		t.Errorf("The handler got the args %q, wanted %q", ctx.args, want)
	}
}

// An error a handler returns is passed back to the caller of dispatch, which is
// what readClientInput logs.
func TestDispatchReturnsAHandlerError(t *testing.T) {
	chatHub, _ := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")

	commands["boom"] = func(ctx cmdCtx) error { return fmt.Errorf("handler failed") }
	t.Cleanup(func() { delete(commands, "boom") })

	if err := runCommand(t, chatHub, alice, "boom"); err == nil {
		t.Fatal("Dispatch swallowed the handler's error")
	}
}

// A command run against a stopped hub has nowhere to send its reply. It must
// return rather than park on a hub that will never answer.
func TestCommandsDoNotBlockOnAStoppedHub(t *testing.T) {
	chatHub, stop := newTestHub(t)
	alice := joinRoom(t, chatHub, "Alice")
	stop()

	done := make(chan error, 1)
	go func() { done <- runCommand(t, chatHub, alice, "who") }()

	select {
	case err := <-done:
		// Report a stopped hub
		if !errors.Is(err, ErrHubClosed) {
			t.Errorf("/who on a stopped hub returned %v, wanted no error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("/who parked on a stopped hub")
	}
}
