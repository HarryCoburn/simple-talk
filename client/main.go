package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

const userNamePrompt = "Please state your username: "

// main connects to the chat server, sends a handshake through setUserName, then runs a receive loop
// and send loop. The send loop reads stdin and stops once dead is closed, so a server-side disconnect doesn't leave it
// blocking on input.
func main() {
	bareConn, err := net.Dial("tcp", "localhost:2069") // TODO, change to ask for a connection string.
	if err != nil {
		log.Fatal("Client could not dial to the server.")
	}
	conn := protocol.NewConn(bareConn)
	defer conn.Close() // This will also close bareConn

	stdin := bufio.NewScanner(os.Stdin)

	// Handshake
	name, err := setUserName(conn, stdin)
	if err != nil {
		log.Printf("Problem with setting username: %v", err)
		return
	}

	dead := make(chan struct{})
	go receiveLoop(conn, dead)
	sendLoop(conn, name, stdin, dead)

}

// receiveLoop listens to a protocol.Conn for frames. If they are a KindChat or a KindSystem,
// it displays the message. TODO: intercept additional frame types.
func receiveLoop(conn *protocol.Conn, dead chan struct{}) {
	defer func() { fmt.Println("You have been disconnected."); close(dead) }()
	for {
		f, err := conn.Recv()
		if err != nil {
			fmt.Printf("\nDisconnected: %v\n", err)
			return
		}
		switch f.Kind {
		case protocol.KindChat:
			var msg protocol.Chat
			if err := json.Unmarshal(f.Payload, &msg); err != nil {
				continue
			}
			fmt.Println(msg.Text)
		case protocol.KindSystem:
			var msg protocol.System
			if err := json.Unmarshal(f.Payload, &msg); err != nil {
				continue
			}
			fmt.Println(msg.Text)
		case protocol.KindError:
			// Post-handshake errors are things like an unknown command, and are
			// meant for this user alone.
			var msg protocol.Error
			if err := json.Unmarshal(f.Payload, &msg); err != nil {
				continue
			}
			fmt.Printf("Error: %s\n", msg.Message)
		default:
			log.Print("Client received frame kind it can't process yet.")
		}
	}

}

// sendLoop sends information from stdin to a protocol.Conn for processing. This will be replaced
// once we begin BubbleTea usage.
func sendLoop(conn *protocol.Conn, name string, scan *bufio.Scanner, dead chan struct{}) {
	for scan.Scan() {
		select {
		case <-dead:
			return
		default:
		}
		line := scan.Text()
		var err error
		if cmd, args, ok := parseInput(line); ok {
			err = conn.SendCommand(cmd, args)
		} else {
			err = conn.SendChat(name, unescapeInput(line))
		}
		if err != nil {
			fmt.Printf("Send failed: %v\n", err)
			return
		}
	}
	conn.Close()
	<-dead
}

// parseInput decides whether a typed line is a slash command. It reports the
// bare command name (lowercased, no slash) and its whitespace-separated
// arguments. A line starting with "//" is an escaped literal slash, not a
// command, and a bare "/" is not a command either.
//
// It is deliberately a pure function so the BubbleTea client can call it from
// Update and intercept client-local commands before any frame is built.
func parseInput(line string) (name string, args []string, ok bool) {
	if !strings.HasPrefix(line, "/") || strings.HasPrefix(line, "//") {
		return "", nil, false
	}

	fields := strings.Fields(line[1:])
	if len(fields) == 0 {
		return "", nil, false // A lone "/" or "/   ".
	}
	return strings.ToLower(fields[0]), fields[1:], true
}

// unescapeInput turns a leading "//" back into the literal "/" the user meant.
func unescapeInput(line string) string {
	if strings.HasPrefix(line, "//") {
		return line[1:]
	}
	return line
}

// setUserName doubles as a handshake function for the server. Currently, it only verifies
// that the user name given is legal and doesn't clash with another name on the server.
// Receipt of a HandshakeAck frame proves the server verified the name and tells the client
// that it is safe to start sendLoop and receiveLoop.
func setUserName(conn *protocol.Conn, inputScanner *bufio.Scanner) (string, error) {
	fmt.Print(userNamePrompt)
	for { // To handle reasking if there's a problem. Break if successful.
		// Get a name and clean it properly
		if inputScanner.Scan() {
			input := inputScanner.Text()
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
			resp, err := conn.Recv()
			if err != nil {
				fmt.Printf("Receive error while setting username: %v", err)
				return "", err
			}
			if resp.Kind == protocol.KindError {
				var serverErr protocol.Error
				if err := json.Unmarshal(resp.Payload, &serverErr); err != nil {
					return "", fmt.Errorf("server rejected the name: %v", err)
				}
				return "", errors.New(serverErr.Message)
			}
			if resp.Kind != protocol.KindHandshakeAck {
				return "", errors.New("Didn't receive handshake ack or error from server")

			}
			var ack protocol.HandshakeAck
			if err := json.Unmarshal(resp.Payload, &ack); err != nil {
				return "", fmt.Errorf("could not read the handshake ack: %w", err)
			}
			// Valid.
			return ack.Name, nil
		}
		if inputScanner.Err() == nil {
			// Normal closure
			return "", nil
		}
		// Catch for any other kind of error
		return "", errors.New("Username Failure.")
	}

}

// cleanUserName takes a string from the username prompt and sanitizes the input before
// sending it to the server for further verification.
func cleanUserName(name string) (string, error) {
	// TODO, consider more validation rules. Pass for now with assumptions for MVP.
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("Username cannot be blank.")
	}
	return name, nil
}
