# Server for SimpleChat

The server directory holds the server for the program.

## main.go

Run() starts the server by

- Opening a TCP listener for the server and setting up appropriate signals for shutdown.
- Running serve() to create a hub and run it, then start an acceptLoop goroutine
- acceptLoop() listens for connection signals on the connection, then passes them to handleNewConn().

handleNewConn() sets up the session on the server side.

- Creates a new protocol.Conn and ties the incoming connection into it
- Verifies the name sent using verifyName() to prevent name conflicts
- Creates a Session with the verified name
- Starts a select lock on hub.registerSession to add the session, and hub.doneChan for teardown.
- Successful hub.registerSession sends a handshakeAck and a server announcement through announceConnection().
- Starts two goroutines on the Session, which takes over from the server to handle future frames.


## hub.go

This holds the connected sessions and routes frames between sessions and the server based on a Kind entry in the frame. hub.run() listens to these channels and also monitors for shutdowns.

These channels are not accessed directly. Instead, there are several methods on the hub to access the channels. None are exported: nothing outside package server holds a *Hub.

- registerSession(*Session) registers a session.
- unregisterSession(*Session) unregisters a session, closing it down properly.
- broadcastFrame(protocol.Frame) sends a frame to all users.
- stop() begins teardown, wait() blocks until it is done, and doneChan() reports the shutdown to the rest of the package.

There are also hub methods used by commands: sendTo() replies to one session, and sessionNames() reads the roster.

## session.go
