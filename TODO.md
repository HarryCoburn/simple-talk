# Next time
- ~~Get disconnect message to announce on unclean disconnects to all other parties.~~
- ~~Use log instead of fmt where needed.~~
- ~~Experiment with slash commands or other MUD style commands from the client side.~~
- ~~Update all unit tests and thoroughly understand the error paths and why they're in there.~~
- TLS


# Later
- BubbleTea conversion
- Sending more than text?
- MUD-type rooms?


# Plan

1. Are we following Go best practices?

Error strings are capitalised and punctuated — against Go Code Review Comments, since these get wrapped into longer sentences. Watch for these.

Watch for %v where %w is meant


Exported identifiers in package main are noise. Client.Conn/.Name/.Out and every Hub channel are exported where export means nothing. This matters because it hides the real question of what the Hub's public API should be (see §3).

sort.Strings (server/commands.go:87) alongside slices.Sort (server/hub.go:122). Pick one; slices.Sort is current.

init() for the command table (server/commands.go:28). The comment explains the real initialisation cycle, so this is defensible — but it's avoidable by having cmdHelp take names from a separate slice, and init() is worth avoiding on principle.

Missing: no CI, no golangci-lint config, no Makefile. For a project this size a single workflow running go vet ./... && go test -race ./... would have caught the server build break before it reached a branch.

2. Code smells and defects
Actual bugs

server/client.go:81 — wrong format verb. log.Printf("command from %s failed, %f", c.Name, err) uses %f on an error. Every command failure logs %!f(*errors.errorString=&{...}) instead of the message. go vet does not catch it because log.Printf arg checking doesn't flag %f against an interface type here. Also note the comma instead of a colon.

server/main.go:115 — formats an error that is always nil. In verifyName, err is checked and returned at line 108. By line 115 it is necessarily nil, so the wrong-frame-kind path always logs "Wrong frame type sent in handshake! : <nil>". The useful information — what kind actually arrived — is not in the message.

Graceful shutdown is dead code in production. Nothing ever closes hub.Done. main() (server/main.go:32-35) starts a goroutine that blocks on <-hub.Done forever; killServer only closes when acceptLoop returns, which only happens when the listener closes, which only happens on Done. There is no signal handling. So Hub.Done, Hub.Finished, the teardown loop, and closeClient's shutdown path are exercised only by the test suite. In production the server can only be hard-killed: clients get a TCP reset, no disconnect announcements, no flush. The machinery is already written and correct — it just has no trigger.

server/main.go:63 — fmt.Println(err) on the handshake failure path, the last survivor of the "use log instead of fmt" TODO item.

Smells

The Hub's public API is its raw channels. Some callers go through methods (hub.register, c.leave), others send directly (hub.Broadcast <- f at server/main.go:95, chatHub.Unregister <- c throughout the tests). Every direct sender has to remember the select-on-Done dance or risk parking forever — that exact bug is what TestHandleNewConnExitsWhenHubStopsMidHandshake was written for. One inconsistent caller reintroduces it.

Hub.Query is not a query channel. sendTo (server/hub.go:148) pushes a closure that calls deliverTo, which can call closeClient — a mutation. The name says read-only; the type (chan func(*Hub)) permits anything. Either rename it or split reads from writes.

query[T]'s ok result is discarded at both call sites (clientNames, sendTo). A stopped hub silently yields the zero value, so "nobody is connected" and "hub is gone" are indistinguishable. This is why cmdWho's "Nobody is connected." branch is unreachable — as noted when testing it.

The server trusts client-side validation. cleanUserName lives only in client/main.go:180. verifyName (server/main.go:104) unmarshals hs.Name and returns it with no trimming, no length cap, no charset check, no empty check. A hand-written client can register as "", as 8KB of text, or as a name with control characters that corrupt other users' terminals. Uniqueness is also exact-match and case-sensitive, so Harry and harry coexist.

Display formatting is baked into the wire payload. decorateChat renders "<alice> hi" into Chat.Text and the client prints that string verbatim. Flagged, not fixed, per your call — the cost is recorded in §4.

Duplication: announceConnection / announceDisconnection are the same function with a different verb. RegisterReq is declared in main.go:14 but belongs with the hub.

Typos in user- and operator-visible strings: "unsuable" in server/client.go:56 and internal/protocol/conn.go:23 (ErrUnrecoverable's message reaches clients via the error frame).

docs/server.md has drifted. It documents a NameTaken channel that does not exist, describes Query as "only returns the length of the client list" (it's now generic), claims Broadcast decorates chat frames (decoration happens in readClientInput), and its ## client.go section is empty.

MaxFrameSize is off by one in practice. bufio.NewReaderSize(c, MaxFrameSize) must hold the delimiter too, so the largest accepted line is 8191 bytes, not the documented 8192.

3. Is the architecture shape sensible?

Yes. A single hub goroutine owning all shared state, reached only by channels, with two goroutines per client (one read, one write), is the standard Go chat design — the same shape as the gorilla/websocket chat example. It gives you freedom from mutexes over the client set, and the check-and-insert atomicity that TestRegisterIsAtomicUnderRacingNames verifies. Don't change it at this scale.

The three-layer split is also right, and internal/protocol is the strongest part of the codebase: genuinely reusable, defensive about hostile input, with the desync recovery logic that most hobby implementations skip.

Two shape-level reservations:

The hub owns transport concerns it shouldn't. closeClient closes c.Out and c.Conn. The hub therefore knows about sockets, which is what makes it untestable without net.Pipe and what will make a second transport (WebSocket, TLS) invasive.
No context.Context anywhere. The Done/Finished pair is a correct hand-rolled equivalent, but context composes with timeouts, cancellation, and every library you'll add later. For a learning project, hand-rolling it once was worth doing; converting is a natural next exercise.
4. Where it breaks down as features are added

Rooms / MUD (your stated direction) is the big one. clientList is one flat set and deliver means "everyone". Rooms require a Room type owning its own membership, with the hub demoted to a room registry. Doing this after commands, private messages, and history are built means rewriting all of them. This is the change to think about before the others.

Private messages (/msg bob hi) need a name→client lookup. Today nameInUse (server/hub.go:127) linearly scans a map[*Client]struct{}. The fix — keying by name — is small now and touches everything later.

Command handlers are given the whole hub, and there is an undocumented invariant. cmdCtx.hub is the real thing, so any handler can call query. If a handler is ever invoked from inside the hub goroutine, that self-deadlocks. It works today only because handlers run on the client's read goroutine — nothing in the code says so. A narrow interface (reply, broadcast, roster) instead of *Hub would make the hazard unrepresentable. Note also that /who costs two hub round-trips (clientNames, then reply).

Authentication has no seam. protocol.Handshake carries only Name. Adding a token or password is a breaking wire change.

No protocol version field. Frame has Kind and Payload only. Adding a version once other people run your client is painful; adding it now is nearly free. This is the cheapest high-value change on the list.

Display strings on the wire (deferred). Because the server ships "<alice> hi" as the text, the client cannot restyle it, colour the nick separately, add timestamps, or let a user mute someone. Every one of those is a BubbleTea feature, and each will want structured From/Text instead. The protocol already carries Chat.From — it's populated and then ignored by the client (client/main.go:58). Whenever you take this on, it is a breaking change, so it pairs naturally with adding the version field.

One panic kills the server. No recover() anywhere and no per-connection recovery. A malformed input path that panics in one client's goroutine takes down every other user's session.

5. Where it breaks down at 2,000 concurrent users

Ordered by which wall you hit first. (Hypothetical target — recorded so the cliffs are known, not as work to do now.)

Every frame is JSON-marshalled once per recipient. deliverTo queues the Frame struct, and each client's processClientOutQueue calls SendFrame → enc.Encode(f). One message to 2,000 users is 2,000 marshals of identical bytes. Marshalling once into []byte and writing that is the single highest-leverage change, and it's contained.
Fan-out is O(N) inside the one goroutine that also does everything else. deliver iterates the whole client map per message while holding the hub's only thread. Registration, unregistration, /who, and every other message wait behind it. At 2,000 users × even modest traffic this is the serialisation point.
The drop-on-full policy disconnects healthy users under burst. deliverTo's default: closes any client whose 256-frame buffer is momentarily full. That's a transient condition being treated as death, with no notice sent to the user. Under load this sheds exactly the users on slower connections.
No write deadline, anywhere. A client that stops reading (laptop lid closed) blocks SendFrame indefinitely, because Conn.SendFrame holds the mutex across the network write. The hub only notices after 256 frames queue up. SetWriteDeadline does not appear in the codebase.
No read deadline after the handshake. verifyName clears the deadline (server/main.go:111) and nothing sets another. Idle connections hold two goroutines and a socket forever. There is no ping/keepalive.
/who is an O(N log N) sort executed inside the hub goroutine. At 2,000 users, a client spamming /who stalls all message routing — a trivial DoS from an ordinary account.
nameInUse is an O(N) scan per registration, so a reconnect storm is O(N²).
No limits on anything else: no max connections, no accept rate limit, no per-client message rate limit. log.Printf on every bad frame and disconnect becomes a serialised firehose (the standard logger takes a mutex).
Memory: 2,000 clients × a 256-frame buffer is a worst case in the hundreds of MB, and it is reached precisely when things are already going wrong.
Prioritised backlog

Tier 1 — clear defects, no design decisions (≈1 sitting)

server/client.go:81 %f → %v
server/main.go:115 report the actual frame kind instead of a nil error
Wire up signal handling so hub.Done is actually closed; the teardown path already works
server/main.go:63 fmt.Println → log.Printf
Error strings: lowercase, unpunctuated; %v → %w at the four wrapping sites
Fix "unsuable" ×2

Tier 2 — cheap now, expensive later

Add a version field to Frame (or to Handshake) before other people run the client
Validate names server-side; don't trust cleanUserName. Cap length, reject empty and control characters, decide on case-insensitive uniqueness
Key clientList by name
Update docs/server.md; it currently describes a hub that no longer exists

Tier 3 — worth doing before the next big feature

Unexport the hub's channels; make methods the only API
Give command handlers a narrow interface instead of *Hub, and document the "handlers never run on the hub goroutine" invariant
recover() per client goroutine
Move to cmd/ + internal/, so the chat logic is importable

Tier 4 — only if scale becomes real

Marshal once per broadcast, not once per recipient
Replace drop-on-full with a bounded-with-warning policy
Read/write deadlines and a keepalive
Room abstraction, if the MUD direction is taken (decide before Tier 3 restructuring)
A caveat on the tests I wrote

TestSendLoopStopsOnceDeadIsClosed (client/main_test.go) passes, but it does not prove what client/main.go:17-19 claims. That comment says the send loop "stops once dead is closed, so a server-side disconnect doesn't leave it blocking on input." In fact sendLoop checks dead only after scan.Scan() returns, so with a real terminal a disconnected user still sits blocked until they press enter. The test passes because scannerOf has input buffered and ready. The comment overstates the behaviour; the fix is a real one (read stdin on its own goroutine, or accept the limitation and correct the comment) and it belongs in Tier 2.

Verification

Nothing to verify — this review changes no code. The current state is gofmt-clean, go vet-clean, and go test -race -count=2 ./... passes for all three packages.
