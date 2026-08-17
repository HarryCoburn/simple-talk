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

Query - Intended to be a general query to the server, but may split it out into individual channel commands. Currently only returns the length of the client list.

NameTaken - Checks if a name is taken and returns a bool. Used in name verification.

Broadcast - Sends a message to everyone on the server. If it's a Chat, it decorates it with the sender name.

## client.go
