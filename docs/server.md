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
- Creates a Session with the verified name
- Starts a select lock on hub.registerSession to add the session, and hub.doneChan for teardown.
- Successful hub.registerSession sends a handshakeAck and a server announcement through announceConnection().
- Starts two goroutines on the Session, which takes over from the server to handle future frames.

## hub.go

This holds the connected sessions and routes frames. The hub has many channels for communication. hub.run() listens to these channels and sends the message to the right locations.

The current channels are:

registerSession - receives a session, adds it to the hub's sessions map

unregisterSession - calls hub.closeSession() to remove the sent session and close it down properly

doneChan - Tells the hub to start shutdown procedures

Finished - Final send from the hub telling everything else the hub is closed.

Query - Intended to be a general query to the server, but may split it out into individual channel commands. Currently only returns the length of the session list.

NameTaken - Checks if a name is taken and returns a bool. Used in name verification.

broadcastFrame - Sends a message to everyone on the server. If it's a Chat, it decorates it with the sender name.

## session.go
