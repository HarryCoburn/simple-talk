# SimpleTalk — backlog

## Status

Reviewed against `a9cbde0`. Since the last review the tree gained `internal/validate` (PRECIS-based
name and message rules, enforced server-side), the `cmd/` + `internal/` layout, unexported hub
channels behind methods, and graceful signal shutdown. Baseline is `gofmt -l .` clean, `go vet ./...`
clean, and `go test ./...` passing for all five packages.

Landed since the last review, and no longer listed below: server-side name validation, case-
insensitive uniqueness (`validate.NameKey`), the `%f` verb bug, reporting the actual frame kind on a
bad handshake, signal handling, `fmt.Println` → `log`, hub channel encapsulation, the `cmd/` split.

Only one milestone is scheduled: TLS. Everything past it is unscheduled and lives under **Later**.

---

## Next up — TLS

Two anchors:

- `server/main.go:25-26` — the raw `net.Listen("tcp", ":2069")` and its existing TODO.
- `client/main.go:21` — the dial target is hardcoded to `localhost:2069`. It has to become a
  connection string for TLS to be testable at all, so do that as part of this.

Worth reading `server/hub.go:201` first: `closeClient` closes `c.Conn` itself, so the hub knows
about sockets. That is the thing that makes a transport swap invasive. See the architecture note.

---

## P0 — defects, one sitting

- [X] `unsuable` → `unusable`, twice: `server/client.go:54` and `internal/protocol/conn.go:23`. The
      second one is in `ErrUnrecoverable`'s message, which reaches clients in an error frame.
- [X] `server/commands.go:23` — the comment says the table is "Populated in `init()`". It is a
      literal now; the comment is describing code that no longer exists.
- [X] `commandNames` (`server/commands.go:31`) is a hand-maintained copy of the `commands` map keys.
      Derive it from the map and sort it, so `/help` cannot drift from what dispatch accepts.
- [X] Error-string style: lowercase, unpunctuated, since these get wrapped into longer sentences.
      `server/main.go:138` ("wrong frame type sent in handshake! ...") and `client/handshake.go:46`
      ("username failure.").
- [X] `%v` → `%w` at the remaining wrap sites: `server/main.go:143`, `client/handshake.go:55`.
- [X] The `sendLoop` comment at `client/main.go:17-19` overstates the behaviour. It claims the send
      loop "stops once dead is closed, so a server-side disconnect doesn't leave it blocking on
      input" — but `sendLoop` (`client/main.go:81`) checks `dead` only *after* `scan.Scan()` returns,
      so a real terminal user still sits blocked until they press enter.
      `TestSendLoopStopsOnceDeadIsClosed` (`client/main_test.go:346`) passes only because
      `scannerOf` has input pre-buffered. Two honest fixes: read stdin on its own goroutine, or
      accept the limitation and correct the comment. Either is fine; the current comment is not.


## P1 — cheap now, expensive later

- [X] **Version field** on `Frame` or `Handshake` (`internal/protocol/protocol.go:18`). Nearly free
      today; painful once anyone else runs the client. Cheapest high-value item on this list.
- [X] **Key `clientList` by folded name.** `nameInUse` (`server/hub.go:184`) linearly scans the map
      *and* runs `validate.NameKey` per client per registration — so a reconnect storm is O(N²) with
      a PRECIS pass inside the loop. Keying by the folded name kills the scan and is exactly the
      lookup private messages (`/msg bob hi`) will need.
- [ ] **Rewrite `docs/server.md`.** It has drifted badly: it documents a `NameTaken` channel that
      does not exist, describes `Query` as "only returns the length of the client list" (it is now
      the generic `tasks` channel with `submit`/`query`/`exec`), credits `Broadcast` with decorating
      chat frames (decoration happens in `readClientInput`), leaves `## client.go` empty, and never
      mentions `commands.go` or `internal/validate`.

## P2 — before the next big feature

- [ ] Give command handlers a narrow interface (reply / broadcast / roster) instead of `*Hub`.
      `cmdCtx.hub` (`server/commands.go:14`) is the real hub, so any handler can call `query` — which
      self-deadlocks if a handler is ever invoked from inside the hub goroutine. It works today only
      because handlers run on the client's read goroutine, and nothing in the code says so. A narrow
      interface makes the hazard unrepresentable; until then, write the invariant down.
- [ ] `recover()` per client goroutine. One panic anywhere in a client's read path currently takes
      down every other user's session.
- [ ] Move `RegisterReq` (`server/main.go:18`) into `hub.go` and unexport it — it is hub internals.
- [ ] Fold `announceConnection` / `announceDisconnection` (`server/main.go:155,164`) into one helper;
      they differ only in a verb.
- [ ] Document the `MaxFrameSize` off-by-one. `bufio.NewReaderSize(c, MaxFrameSize)`
      (`internal/protocol/conn.go:15,37`) must hold the delimiter too, so the largest accepted line
      is 8191 bytes, not the documented 8192.
- [ ] Note `/who` costs two hub round-trips (`clientNames`, then `reply`).

## Later — unscheduled

- **BubbleTea conversion.**
- **Rooms / MUD.** `clientList` is one flat set and `deliver` means "everyone". Rooms need a `Room`
  type owning its own membership with the hub demoted to a room registry. If this direction is ever
  taken, decide it *before* the P2 restructuring — doing it after commands, private messages, and
  history exist means rewriting all of them.
- **Structured chat payloads.** `decorateChat` (`server/client.go:116`) renders `"<alice> hi"` into
  `Chat.Text` and the client prints that verbatim, so the client cannot restyle, colour the nick,
  add timestamps, or mute a sender — every one of which is a BubbleTea feature. `protocol.Chat.From`
  is already populated and then ignored by the client (`client/main.go:58`). This is a breaking wire
  change, so it pairs naturally with the version field.
- **Sending more than text.**
- **`context.Context`.** The `done`/`finished` pair is a correct hand-rolled equivalent; converting
  is a natural exercise, and context composes with timeouts and every library added later.
- **CI / golangci-lint / Makefile.** Held until BubbleTea is done and completed builds are being
  pushed to the VPS.
- **Authentication.** `protocol.Handshake` carries only `Name`; adding a token or password is a
  breaking wire change, so it also pairs with the version field.

---

## Appendix — scale cliffs

Hypothetical 2,000-user target. Recorded so the cliffs are known, not as scheduled work. Ordered by
which wall you hit first.

1. Every frame is JSON-marshalled once *per recipient* — `deliverTo` queues the `Frame` struct and
   each client's `processClientOutQueue` calls `SendFrame` → `enc.Encode`. Marshalling once into
   `[]byte` is the highest-leverage change and is well contained.
2. Fan-out is O(N) inside the single hub goroutine (`deliver`, `server/hub.go:235`), so registration,
   `/who`, and everything else queues behind it.
3. Drop-on-full disconnects healthy users: `deliverTo`'s `default:` (`server/hub.go:224`) closes any
   client whose 256-frame buffer is momentarily full, with no notice — shedding exactly the users on
   slower connections.
4. No write deadline anywhere. `Conn.SendFrame` holds the mutex across the network write, so a
   client that stops reading blocks it indefinitely.
5. No read deadline after the handshake — `verifyName` clears it (`server/main.go:134`) and nothing
   sets another. No ping/keepalive, so idle connections hold two goroutines and a socket forever.
6. `/who` sorts inside the hub goroutine, so spamming it stalls all routing — a trivial DoS.
7. No max connections, no accept rate limit, no per-client message rate limit. `log.Printf` on every
   bad frame becomes a serialised firehose (the standard logger takes a mutex).
8. Memory: 2,000 clients × a 256-frame buffer is a worst case in the hundreds of MB, reached
   precisely when things are already going wrong.

## Architecture note

The shape is right: a single hub goroutine owning all shared state reached only by channels, with two
goroutines per client. It buys freedom from mutexes over the client set and the check-and-insert
atomicity `TestRegisterIsAtomicUnderRacingNames` verifies. Don't change it at this scale. The
three-layer split is right too, and `internal/protocol` is the strongest part of the codebase —
genuinely reusable, defensive about hostile input, with the desync recovery most hobby
implementations skip.

Two reservations survive: `closeClient` (`server/hub.go:201`) owns transport concerns, which is what
makes the hub untestable without `net.Pipe` and what will make TLS or a WebSocket transport invasive;
and there is no `context.Context` anywhere.
