# Server for SimpleTalk

The server directory holds the server for the program. Activated by ./cmd/server/main.go, which
parses flags, calls Run() and reports the error it returns. Nothing in this package exits the
process on its caller's behalf.

    simpletalk-server -port 2069

-port defaults to server.DefaultPort. The command turns it into a listen address with an empty
host, so the server accepts on every interface -- which is what reaching it from another machine
needs. Bind to one interface by calling Run with an explicit host instead.

The shape is one hub goroutine owning all shared state, reached only by channels, with two
goroutines per connected session. That buys freedom from mutexes over the session map and makes
the check-and-claim of a username atomic. Shutdown is a context.Context: cancelling the one Run
builds reaches the hub, the accept loop, and any handshake still in flight.

## server.go

Run() is the entry point, and is split into three so it can be tested.

- Run(addr) calls run() with that address and the real signal.NotifyContext.
- run(ctx, addr, notify) opens the TCP listener and returns an error if it cannot. Taking the
  address as a parameter is what lets a test pick a port it knows is busy.
- runOn(ctx, ln, notify) wires os.Interrupt and syscall.SIGTERM to a context, then calls serve().
  Taking notify as a parameter is what lets a test assert which signals were asked for without
  the test binary receiving one.

serve(ctx, ln) builds the hub from that context and starts three goroutines: the hub itself, a
watcher that closes the listener when the hub is done, and acceptLoop(). It then waits for either
the context to be cancelled or the accept loop to stop on its own, and tears down in order:
hub.stop(), wait for the accept loop, then hub.wait().

acceptLoop(ctx, hub, ln, stopped) takes connections until the listener closes. A connection
accepted after teardown has begun is closed rather than handed on — the hub is closing sessions,
not registering them. Every other connection gets a buildNewSession() goroutine.

buildNewSession(ctx, hub, conn) sets up the server side of one session.

- Creates a protocol.Conn around the incoming connection.
- Arms a context.AfterFunc that closes the socket if the context is cancelled. A handshake parks
  in Recv, and closing the socket is the only thing that unblocks it: the read deadline is thirty
  seconds out and Recv has no context of its own. The AfterFunc is disarmed at registration,
  because from there the socket belongs to hub.closeSession.
- Calls acceptHandshake() and, on failure, tells the client why before hanging up. A version mismatch
  and a name the rules reject both get an error frame; a bare EOF would leave the client with
  nothing to act on. The client's own version is never echoed back, and nameRejection() matches
  the reason against a fixed list of validate errors so a hostile name cannot reach a terminal.
- Registers the session with the hub, which is where a duplicate name is caught.
- Sends the handshake ack, announces the arrival through announcePresence(), and starts the
  session's two goroutines.

acceptHandshake() reads the one handshake frame under protocol.HandshakeTimeout, clears the deadline,
checks the protocol version before it looks at the name, then runs the name through
internal/validate. The client validates too, for a faster prompt, but this is where the rule is
enforced: a hand-written client is under no obligation to have checked.

announcePresence() builds the system frame for an arrival or a departure. The two differ only in
a verb, so they are one helper taking eventConnected or eventDisconnected.

## hub.go

The hub holds the connected sessions and routes frames between them. hub.run() is a single
goroutine selecting over four channels — register, unregister, broadcast and tasks — plus its own
context. Because only that goroutine touches the session map, the fold-check-claim of a username
happens in one loop iteration and two sessions racing for one name cannot both find it free.

These channels are never touched directly. The methods around them are unexported: nothing
outside package server holds a *Hub.

- registerSession(*Session) claims a name, returning ErrNameTaken if it is held.
- unregisterSession(*Session) removes a session and announces the departure.
- broadcastFrame(protocol.Frame) sends a frame to everyone. deliver() is the internal equivalent,
  for code already running inside the hub goroutine.
- sendTo(*Session, protocol.Frame) replies to one session, and sessionNames() reads the roster.
  Both are used by commands.
- stop() begins teardown, wait() blocks until it is complete, and doneChan() reports shutdown to
  the rest of the package.

Every one of these blocks until the hub goroutine services it, so none may be called from inside
it. A closure passed to tasks that called back into the hub would wait on the loop that is
running it.

### Shutdown

The hub derives its own context from the one serve() was given, so cancelling that context and
calling stop() are the same thing. stop() is the cancel func, which is idempotent — calling it
twice is safe and needs no guard.

Two signals, not one, and the difference matters:

- ctx.Done() means teardown has been *requested*. Every method above selects on it, so a caller
  is released rather than parked forever against a hub that will never answer.
- finished means run() has *drained and returned*, with every session closed. wait() blocks on
  it. Context has no equivalent, which is why the channel survives the conversion — serve()
  depends on the distinction to guarantee sessions are closed before it returns.

Callers see ErrHubClosed, which wraps the context's own error. errors.Is finds both it and
context.Canceled: ErrHubClosed is this package's vocabulary, context.Canceled is the cause.

### Running work inside the hub

submit() is the generic core: it sends a closure on tasks and waits for the result, reporting
false if the hub shut down first. exec() is submit for work with no result to return, which is
what earns it a name of its own.

closeSession() is hub-goroutine-only and is the single owner of teardown for one session: it
closes the session's Out channel, closes the socket, and deletes the map entry. registered()
guards it by identity rather than by name, because both of a session's goroutines can unregister
it, and a reconnect under the same name in between would otherwise let a stale unregister tear
down the new session.

deliverTo() is deliberately non-blocking. If a session's 256-frame buffer is full the session is
dropped rather than the hub goroutine blocking on it — one slow reader must not stall routing for
everyone.

## session.go

The Session struct is one connected user: their protocol.Conn, the name they chose, the queue of
frames waiting to go out to them, and the folded key the hub registered them under.

Each session runs two goroutines, started by buildNewSession():

- readLoop() reads frames off the socket and acts on them — chat and pose frames are checked and
  decorated, command frames go to dispatchCommand(), unsupported kinds are ignored rather than
  fatal. An oversized or malformed frame is recoverable: protocol.Conn resynchronises on the
  newline, so the session survives.
- writeLoop() ranges over the session's Out channel and writes each frame to the socket.

Both defer guard(), which turns a panic on one session's goroutine into that one session leaving
rather than the whole server unwinding. recover only sees panics from its own goroutine, so each
of the two needs its own.

Either goroutine hands the session back by calling hub.unregisterSession() directly, and calling
it twice is safe: the hub guards on identity.

checkChat() runs the message through internal/validate; decorateChat() renders it, using the
message format for ordinary chat and the pose format for a line starting with ":". Decoration
happens here, on the session's own goroutine, not in the hub.

### The Out channel

Out is written to only by the hub goroutine, through deliverTo(), and closed only by
closeSession(). That is what makes closing it safe while sends are in flight: both happen on the
same goroutine, and sendTo() reaches deliverTo() through exec() rather than directly. A send
added from anywhere else is a send-on-closed-channel panic in production, not a test failure.

## commands.go

Commands arrive as their own frame kind — the client strips the leading "/" before building it —
and are dispatched by dispatchCommand() from the session's read goroutine. An unknown or empty
command name is reported to the caller and is not an error.

Handlers receive a cmdEnv: the calling session, a hub, the arguments, and the sorted roster of
command names. They get commandHub rather than the *Hub itself — an interface of exactly three
methods, sendTo, sessionNames and broadcastFrame — so a handler cannot reach submit, exec or the
session map.

The receivers and parameters of cmdEnv are named cc, not ctx. Commands is where a real
context.Context will want to be threaded, and two things reading alike in one function is worth
avoiding.

Every cmdEnv method is a round trip through the hub goroutine, so handlers must run on the
caller's own goroutine and never inside hub.run(). Being called from readLoop() is what makes
that true today.

The command table is a map of name to handler. Currently:

- who lists everyone connected, via sessionNames().
- help lists the command names, from the memoised roster.

## Related

- internal/protocol is the wire format shared with the client: frames, kinds, and the Conn that
  reads and writes them.
- internal/validate holds the rules for user-supplied names and messages. The server enforces
  them; it never trusts that the client ran them first.
