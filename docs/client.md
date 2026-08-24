# Simpletalk Client

The client software lives inside the client folder. Not to be confused with server/client.go, which is the server representation of a client connection.

Activated by ./cmd/client/main.go

clientVersion in client/main.go and serverVersion in server/main.go must match to use.

Currently, the connection string is hardcoded to localhost:2069

client/main.go exposes Run() to start. That method reaches out to a server to perform a handshake. If it succeeds, it activates a sendLoop and receiveLoop goroutine and opens a dead channel to close these if a problem arises or the user closes the program.

client/handshake.go performs handshake functions. Currently, it just asks for a username and checks with the client to ensure it's safe and not already taken.
