# Simpletalk Client

The client software lives inside the client folder. Not to be confused with server/client.go, which is the server representation of a client connection.

Activated by ./cmd/client/main.go

clientVersion in client/main.go and serverVersion in server/main.go must match to use.

Both client and server use the internal/protocol package as their shared language.

Currently, the connection string is hardcoded to localhost:2069

client/main.go exposes Run() to start. That method makes a protocol.Conn, then reaches out to a server to perform a handshake. If it succeeds, it activates a sendLoop and receiveLoop goroutine and opens a dead channel to close these if a problem arises or the user closes the program.

receiveLoop listens for protocol frames on the client's connection and processes them.

sendLoop watches stdin, processes whether the input is a command or a chat, and sends it on. 

- Commands are marked by starting "/". (/who -> Activates the who command) 
- Undecorated chat strings are sent as chats. (Hello -> <clientname> Hello)
- Chat strings that start with ":" are poses. (:jumps -> Clientname jumps)

client/handshake.go performs handshake functions. Currently, it just asks for a username and checks with the client to ensure it's safe and not already taken.
