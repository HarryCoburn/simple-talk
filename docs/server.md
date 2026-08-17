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
- Verifies the name sent using verifyName to prevent name conflicts
- Creates a Client with the verified name
- Starts a select lock on hub.Register to add the client, and hub.Done for teardown.
- Successful hub.Register sends a handshakeAck and an announcement to everyone about the connection.
- Starts two goroutines on the Client, which takes over from the server from here.
