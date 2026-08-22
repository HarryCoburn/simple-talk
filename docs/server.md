# Server for SimpleChat

The server directory holds the server for the program.

## main.go

main() starts the server by

- Opening a TCP listener
- Creating a hub
- Creating a channel lock to hold three goroutines: starting the hub, a listener closer, and acceptLoop

acceptLoop() listens for connections. When a connection comes in, it passes the hub and the connection to handleNewConn()

handleNewConn() sets up the client on the server side.

- Creates a new protocol.Conn and ties the incoming connection into it
- Verifies the name sent using verifyName() to prevent name conflicts
- Creates a Client with the verified name
- Starts a select lock on hub.Register to add the client, and hub.Done for teardown.
- Successful hub.Register sends a handshakeAck and a server announcement through announceConnection().
- Starts two goroutines on the Client, which takes over from the server to handle future frames.

## hub.go

This holds the connected client list and routes frames. The hub has many channels for communication. hub.run() listens to these channels and sends the message to the right locations.

The current channels are:

Register - receives a client, adds the client to the the hub's clientList

Unregister - calls hub.closeClient() to remove the sent client and close it down properly

Done - Tells the hub to start shutdown procedures

Finished - Final send from the hub telling everything else the hub is closed.

Query - A general read-only query channel. It carries a `func(*Hub)` that hub.run() executes on its own goroutine, so the query can read clientList directly without locking. Callers use the generic `query()` helper, which handles the reply channel and the Done guard; `clientCount()` and `clientNames()` are thin wrappers over it. A query must not retain anything it is handed — `clientNames()` copies the names out and sorts them. `sendTo()` uses the same channel to queue a frame for a single client, because the hub owns each client's Out channel and its close.

NameTaken - Checks if a name is taken and returns a bool. Used in name verification.

Broadcast - Sends a message to everyone on the server. If it's a Chat, it decorates it with the sender name.

## commands.go

Slash commands are parsed on the client, which strips the leading `/` and sends a `command` frame carrying a bare name and its arguments. The server never inspects chat text for commands, so a chat line that happens to start with `/` stays chat.

readClientInput() routes `KindCommand` frames to dispatchCommand(), which lowercases the name, looks it up in the `commands` table, and runs the handler. An unknown or empty name is reported to the caller as an error frame and is not treated as a server-side failure.

Handlers take a cmdCtx (the calling client, the hub, and the arguments) and reply through `ctx.reply()` or `ctx.replyError()`, which reach the caller alone. A command that everyone should see would push to hub.Broadcast instead.

To add a command: write a `cmdHandler` and add it to the `commands` map in init(). The map is filled in init() rather than as a literal because cmdHelp reads it, which would otherwise be an initialization cycle.

## client.go
