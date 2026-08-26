# SimpleTalk Client

The client software lives inside the client folder. Not to be confused with server/session.go,
which is the server's representation of a connected user.

Activated by ./cmd/client/main.go, which parses flags, calls Run() and reports the error it
returns. Nothing in this package exits the process on its caller's behalf.

    simpletalk-client -addr 192.168.1.5:2069

Both client and server use the internal/protocol package as their shared language, including
protocol.ProtocolVersion. That constant is the protocol's, not either end's: both send and check
the same value, so the two cannot drift apart.

The server address is -addr, in host:port form, defaulting to client.DefaultAddr
(localhost:2069). An IPv6 literal needs brackets: -addr "[::1]:2069".

## client.go

Run(addr) dials that address, wraps the socket in a protocol.Conn, and hands it to
negotiateName() for the handshake. If that succeeds it starts receiveLoop() on its own goroutine and runs sendLoop()
on the main one, with a dead channel to unblock the sender when the receiver stops.

receiveLoop listens for protocol frames on the client's connection and prints the ones it
understands — chat and system messages, and error frames from the server. A frame it cannot use is
skipped rather than fatal. When the connection drops it closes dead, which is what releases
sendLoop.

sendLoop watches stdin, decides whether each line is a command or a chat, and sends it on.

- Commands are marked by a leading "/". (/who -> the who command, sent as a command frame)
- Ordinary lines are sent as chat. (Hello -> \<clientname\> Hello)
- Lines starting with ":" are poses. (:jumps -> Clientname jumps)
- A line starting with "//" is an escaped chat line, not a command: the first slash is stripped
  and the rest is sent as chat. That is how a user says "/who" out loud.

parseInput() makes that command-or-chat decision and splits the arguments; unescapeInput() strips
the leading slash from an escaped line. Decoration is not done here — the server renders the
"\<name\> text" and pose forms, and the client prints what it is given.

## handshake.go

negotiateName() picks a username the server will accept. It prompts, validates, sends, and
re-prompts in a loop until a name is acked or the input closes, returning the name the server
acknowledged — which is the one to use from then on, since the server is the authority on it.

The name is checked against internal/validate before it is sent. That is for a faster prompt
only: the server enforces the same rules itself and does not trust that the client ran them.

recvHandshake() reads the server's reply and turns it into a name or an error. A HandshakeAck
carries the accepted name. An error frame carries a reason the user can act on — a version
mismatch, a name already taken, or a name the rules reject — and is printed rather than swallowed,
so a rejected client is never left staring at a bare EOF. Any other frame kind is an error.

## Related

- internal/protocol is the wire format shared with the server: frames, kinds, and the Conn that
  reads and writes them.
- internal/validate holds the rules for names and messages. The client calls them so a user learns
  about a bad name at the prompt instead of after a round trip.
- docs/server.md describes the other end.
