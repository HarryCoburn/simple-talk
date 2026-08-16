// Protocol for SimpleTalk

package internal

import "encoding/json"

type Kind string

const (
	KindHandshake    Kind = "handshake"
	KindHandshakeAck Kind = "handshake_ack"
	KindChat         Kind = "chat"
	KindError        Kind = "error"
)

type Frame struct {
	Kind    Kind            `json:"kind"`
	Payload json.RawMessage `json:"payload,omitempty"`
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
