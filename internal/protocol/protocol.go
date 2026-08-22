// Protocol for SimpleTalk

package protocol

import "encoding/json"

type Kind string

const (
	KindHandshake    Kind = "handshake"
	KindHandshakeAck Kind = "handshake_ack"
	KindChat         Kind = "chat"
	KindError        Kind = "error"
	KindSystem       Kind = "system"
)

type Frame struct {
	Kind    Kind            `json:"kind"`              // The kind of frame the protocol is sending
	Payload json.RawMessage `json:"payload,omitempty"` // The contents of the frame kind, of a type listed below.
}

type Handshake struct {
	Name string `json:"name"`
}

type HandshakeAck struct {
	Name string `json:"name"` // name returned by Handshake after cleanup from the server
}

type Chat struct {
	From string `json:"from"`
	Text string `json:"text"`
}

type Error struct {
	Message string `json:"message"`
}

type System struct {
	Text string `json:"text"`
}
