package server

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// Tests for the hub: who is in the room, and how frames reach them.

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
	chatHub.unregisterClient(chatClient)

	// Testing
	wantRoster(t, chatHub)

	// Try double deregistration
	chatHub.unregisterClient(chatClient)
	wantRoster(t, chatHub)
}

// The name check and the insert are one hub operation, so a name can only be
// claimed once no matter how the registrations interleave.
func TestRegisterRejectsADuplicateName(t *testing.T) {
	chatHub, _ := newTestHub(t)
	first := &Client{Name: "Harry", Out: make(chan protocol.Frame, 1)}
	mustRegister(t, chatHub, first)

	err := chatHub.registerClient(&Client{Name: "Harry", Out: make(chan protocol.Frame, 1)})
	if !errors.Is(err, ErrNameTaken) {
		t.Fatalf("Wanted ErrNameTaken for a second %q, got %v", "Harry", err)
	}
	wantRoster(t, chatHub, "Harry")

	// The name frees up once its holder leaves.
	chatHub.unregisterClient(first)
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
			results <- chatHub.registerClient(&Client{Name: "Harry", Out: make(chan protocol.Frame, 1)})
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
	chatHub.broadcastFrame(f)
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
	chatHub.broadcastFrame(frame1) // Fill buffer
	chatHub.broadcastFrame(frame2) // Overload Client.Out, which should force server to drop the client.

	wantRoster(t, chatHub)

}

func TestDoneGuardsBroadcastSend(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub(serverVersion)
	exited := make(chan struct{})
	go func() { testHelp.Client.readClientInput(chatHub); close(exited) }()
	// Nothing is draining chatHub.Broadcast, so readClientInput parks in the
	// send select until Done releases it.
	if err := testHelp.Peer.SendChat("test", "Hello"); err != nil {
		t.Fatalf("Could not send the chat: %v", err)
	}
	chatHub.stop()
	select {
	case <-exited:
		return
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Sending to Done channel did not close Client")
	}
}

func TestDoneGuardsUnregister(t *testing.T) {
	testHelp := setUpTest(t)
	chatHub := newHub(serverVersion)
	exited := make(chan struct{})
	go func() { testHelp.Client.readClientInput(chatHub); close(exited) }()
	chatHub.stop()
	testHelp.OutConn.Close()
	select {
	case <-exited:
	case <-time.After(time.Millisecond * 100):
		t.Fatalf("Sending to Done channel did not close Client")
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

// A name that only differs by case is the same person, so the second one is
// turned away rather than quietly shadowing the first.
func TestRegisterRejectsANameTakenInAnotherCase(t *testing.T) {
	chatHub, _ := newTestHub(t)
	joinRoom(t, chatHub, "Harry")

	second := &Client{Name: "harry", Out: make(chan protocol.Frame, 8)}
	if err := chatHub.registerClient(second); !errors.Is(err, ErrNameTaken) {
		t.Fatalf("Registering %q against %q returned %v, wanted %v", "harry", "Harry", err, ErrNameTaken)
	}
}

// Both of a client's goroutines can call leave, so the hub can see the same
// client twice. If the user reconnects under that name in between, the stale
// leave must not tear the new client down: the folded name is the map key, but
// it is not a unique handle for a client across a reconnect.
func TestAStaleUnregisterLeavesAReconnectedClientAlone(t *testing.T) {
	chatHub, _ := newTestHub(t)

	first := joinRoom(t, chatHub, "Alice")
	chatHub.unregisterClient(first)

	// Register blocks on the hub's reply, so by the time it returns the
	// unregister ahead of it has already been handled.
	second := joinRoom(t, chatHub, "Alice")

	// The second leave for the client that is already gone.
	chatHub.unregisterClient(first)

	names, err := chatHub.clientNames()
	if err != nil {
		t.Fatalf("The hub did not survive a stale unregister: %v", err)
	}
	if !slices.Equal(names, []string{"Alice"}) {
		t.Fatalf("The roster is %v, wanted the reconnected Alice still on it", names)
	}

	// Still a live member of the room, not a half closed one.
	if err := chatHub.sendTo(second, mustChatFrame(t, "bob", "hello")); err != nil {
		t.Fatalf("Could not send to the reconnected client: %v", err)
	}
	if got := chatText(t, nextReply(t, second)); got != "hello" {
		t.Errorf("The reconnected client received %q, wanted %q", got, "hello")
	}
}

// A client that has left is not delivered to just because someone else now
// holds its name.
func TestABroadcastSkipsAClientReplacedByAReconnect(t *testing.T) {
	chatHub, _ := newTestHub(t)

	first := joinRoom(t, chatHub, "Alice")
	chatHub.unregisterClient(first)
	second := joinRoom(t, chatHub, "Alice")

	if err := chatHub.broadcastFrame(mustChatFrame(t, "bob", "hello")); err != nil {
		t.Fatalf("Could not broadcast: %v", err)
	}
	if got := chatText(t, nextReply(t, second)); got != "hello" {
		t.Errorf("The reconnected client received %q, wanted %q", got, "hello")
	}
	if _, stillOpen := <-first.Out; stillOpen {
		t.Error("The departed client was delivered to, wanted its queue closed and empty")
	}
}

// Names are stored folded so they can be matched case insensitively, but the
// roster shows them the way the user typed them.
func TestTheRosterShowsNamesAsTyped(t *testing.T) {
	chatHub, _ := newTestHub(t)
	joinRoom(t, chatHub, "Alice")
	joinRoom(t, chatHub, "BOB")

	names, err := chatHub.clientNames()
	if err != nil {
		t.Fatalf("clientNames returned an unexpected error: %v", err)
	}
	if !slices.Equal(names, []string{"Alice", "BOB"}) {
		t.Errorf("The roster is %v, wanted the names as typed", names)
	}
}
