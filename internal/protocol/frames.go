package protocol

import "encoding/json"

// Encoders

// newFrame encodes a subframe payload and returns a full Frame ready for sending.
func newFrame(kind Kind, payload any) (Frame, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return Frame{}, err
	}

	return Frame{
		Kind:    kind,
		Payload: encoded,
	}, nil
}

// Builders

// Build a chat frame. Allows server to change a frame's sender.
func NewChatFrame(sender string, msg string) (Frame, error) {
	return newFrame(KindChat, Chat{
		From: sender,
		Text: msg,
	})
}

// Build a system frame.
func NewSystemFrame(text string) (Frame, error) {
	return newFrame(KindSystem, System{Text: text})
}

// Build an error frame
func NewErrorFrame(message string) (Frame, error) {
	return newFrame(KindError, Error{Message: message})
}

// Build a handshake frame
func NewHandshakeFrame(name string) (Frame, error) {
	return newFrame(KindHandshake, Handshake{Name: name})
}

// Build a handshake ack frame
func NewHandshakeAckFrame(name string) (Frame, error) {
	return newFrame(KindHandshakeAck, HandshakeAck{Name: name})
}
