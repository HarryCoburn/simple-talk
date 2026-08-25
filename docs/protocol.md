# Protocol library

Protocol is the frame library for SimpleText and its associated connection.

## protocol/protocol.go
Holds the definitions for the kinds of possible frames sent through the connection. Each Frame has a Kind, and a Payload. Each Kind has an associated struct that goes into the payload. All frames are intended to be encoded with JSON and separated by newlines.

The frame kinds are:

- Handshake and HandshakeAck: For handshake communication and acknowledgment.
- Chat: When sending, a line of text from the client that may be parsed into other frames. When receiving, a line of chat text or posing.
- Error: An error from the server reporting back to the user.
- System: A message from the server reporting to one or more users (e.g. a connection/disconnection notification)
- Command: A command to the server.


## protocol/frames.go
Holds constructor methods for the various frame types. New frame kinds should have a new builder method created here.

## protocol/conn.go
Holds the definition and methods of Conn, the protocol connector.

A Conn holds a connection, a reader, a mutex, and a json encoder.
- The connection holds the TCP connection from client/main.go
- The reader accepts incoming data and is read through Conn.Recv()
- The encoder takes a frame, encodes it, and sends it through Conn.SendFrame
- The mutex locks and unlocks the encoding process.

Use NewConn() to create a new Conn.

There are constants in this file to control the MaxFrameSize, how many times to retry before discarding a frame, and the timeout delay for a handshake response.


Sending is either sending a frame directly to Conn.SendFrame, or calling one of the Send subtypes with the appropriate payloads. The Send subtypes call the correct frame builders and sends a complete encoded frame into Conn.SendFrame.

Receiving is done by calling Conn.Recv. It will deliver one frame per call with the aid of readLine(), which splits the frames on newline delimiters and handles error processing.

Conn.SetReadDeadline sets the timeout for reads. This is adjusted through the constant HandshakeTimeout

Conn.Close closes the connection in Conn.conn when it is time to shut down or clean up.
