# Protocol library

Protocol is the frame library for SimpleText and its associated connection.

- protocol.go holds the definitions for the kinds of possible frames sent through the connection. Each Frame has a Kind, and a Payload. Each Kind has an associated struct that goes into the payload. All frames are intended to be encoded with JSON.
- frames.go contains constructor methods for the various frames. New frame kinds should have a new builder method created.
- conn.go holds the connector and its associated functions, along with some constants for special errors, frame sizes, and timeouts.

## Kinds

| Kind | Payload | Direction | Notes |
| --- | --- | --- | --- |
| `handshake` | Handshake{Name} | client to server | Opens the connection. |
| `handshake_ack` | HandshakeAck{Name} | server to client | Carries the cleaned name. |
| `chat` | Chat{From, Text} | both | The server takes the sender from the registered connection and ignores the From the client sends. |
| `command` | Command{Name, Args} | client to server | A slash command. Name is the bare command word, lowercased and without the leading slash. |
| `system` | System{Text} | server to client | Announcements and command output. |
| `error` | Error{Message} | server to client | Rejected name, bad frame, unknown command. |

Commands have no dedicated reply kind: results come back as `system`, failures as `error`. Give commands their own result kind only if a client needs to lay the result out structurally.

A client should skip kinds it does not recognize rather than failing, so a newer server can add kinds without breaking older clients.

## protocol.Conn

A Conn holds a connection, a reader, a mutex, and a json encoder.
- The connection holds the TCP connection
- The reader accepts incoming data and is read through Conn.Recv()
- The encoder takes a frame, encodes it, and sends it through Conn.SendFrame
- The mutex locks and unlocks the encoding process.

Sending is either sending a frame directly to Conn.SendFrame, or calling one of the Send subtypes with the appropriate payloads. The Send subtypes call the correct frame builders and sends a complete encoded frame into Conn.SendFrame.

Receiving is done by calling Conn.Recv. It will deliver one frame per call.

Conn.SetReadDeadline sets the timeout for reads. This is adjusted through the constant HandshakeTimeout

Conn.Close closes the connection.
