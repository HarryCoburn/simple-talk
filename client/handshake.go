package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
	"github.com/HarryCoburn/simple-talk/internal/validate"
)

// sendHandshake sends handshake information to the server. Currently, it only verifies
// that the user name given is legal and doesn't clash with another name on the server.
// Receipt of a HandshakeAck frame proves the server verified the name and tells the client
// that it is safe to start sendLoop and receiveLoop.
func sendHandshake(conn *protocol.Conn, inputScanner *bufio.Scanner) (string, error) {
	fmt.Print(userNamePrompt)
	for { // To handle reasking if there's a problem. Break if successful.
		// Get a name and clean it properly
		if inputScanner.Scan() {
			input := inputScanner.Text()
			// Make sure username meets our rules
			cleaned, err := cleanUserName(input)
			if err != nil {
				fmt.Println("Error in input. Try again")
				fmt.Print(userNamePrompt)
				continue
			}
			// Try to send it to the server for further validation
			if err := conn.SendHandshake(cleaned); err != nil {
				return "", err
			}
			// Check the response
			resp, err := recvHandshake(conn)
			if err != nil {
				return "", err
			}
			return resp, nil
		}
		// Input scanner is closed normally
		if inputScanner.Err() == nil {
			return "", nil
		}
		// Catch for any other kind of error
		return "", errors.New("username failure.")
	}
}

// recvHandshake checks for a HandshakeAck frame return from the server and processes it.
func recvHandshake(conn *protocol.Conn) (string, error) {
	// Check the response
	resp, err := conn.Recv()
	if err != nil {
		return "", fmt.Errorf("receive error while setting username: %v", err)
	}
	// Check the kind of response for an error
	if resp.Kind == protocol.KindError {
		var serverErr protocol.Error
		if err := json.Unmarshal(resp.Payload, &serverErr); err != nil {
			return "", fmt.Errorf("server rejected the name: %w", err)
		}
		return "", errors.New(serverErr.Message)
	}
	if resp.Kind != protocol.KindHandshakeAck {
		return "", errors.New("didn't receive handshake ack or error from server")

	}
	// We look good, unpack the handshakeack frame and unpack it for client.
	var ack protocol.HandshakeAck
	if err := json.Unmarshal(resp.Payload, &ack); err != nil {
		return "", fmt.Errorf("could not read the handshake ack: %w", err)
	}
	// Valid.
	return ack.Name, nil
}

// cleanUserName takes a string from the username prompt and sanitizes the input before
// sending it to the server for further verification.
func cleanUserName(name string) (string, error) {
	// TODO, consider more validation rules. Pass for now with assumptions for MVP.
	return validate.Name(name)
}
