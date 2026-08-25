# Server for SimpleChat

The server directory holds the server for the program.

## main.go

Run() starts the server by

- Opening a TCP listener for the server and setting up appropriate signals for shutdown.
- Running serve() to create a hub and run it, then start an acceptLoop goroutine
- acceptLoop() listens for connection signals on the connection, then passes them to handleNewConn().

handleNewConn() sets up the client data on the server side.

- Creates a new protocol.Conn and ties the incoming connection into it
- Verifies the name sent using verifyName() to prevent name conflicts
- Creates a Client with the verified name
- Starts a select lock on hub.Register to add the client, and hub.Done for teardown.
- Successful hub.Register sends a handshakeAck and a server announcement through announceConnection().
- Starts two goroutines on the Client, which takes over from the server to handle future frames.


## hub.go

This holds the connected client list and routes frames between clients and the server based on a Kind entry in the frame. hub.run() listens to these channels and also monitors for shutdowns.

These channels are not accessed directly. Instead, there are several exposed methods on the hub to access the channels.

- Register(*Client) registers a client.
- Unregister(*Client) unregisters a client
- Broadcast(protocol.Frame) sends a frame to all users

There are also hub methods used by commands.




## client.go
