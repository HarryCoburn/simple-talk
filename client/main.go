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

func main() {
	bareConn, err := net.Dial("tcp", "localhost:2069") // TODO, change to ask for a connection string.
	if err != nil {
		log.Fatal("Client could not dial to the server.")
	}
	conn := protocol.NewConn(bareConn)
	defer conn.Close()

	inputScanner := bufio.NewScanner(os.Stdin)
	name, err := setUserName(conn, inputScanner)
	if err != nil {
		fmt.Printf("Problem with setting username: %v", err)
		return
	}

	dead := make(chan struct{})
	go func() {
		defer close(dead)
		for {
			f, err := conn.Recv()
			if err != nil {
				fmt.Printf("\nDisconnected: %v\n", err)
				return
			}
			if f.Kind == protocol.KindChat {
				var msg protocol.Chat
				if err := json.Unmarshal(f.Payload, &msg); err != nil {
					continue
				}
				fmt.Println(msg.Text)
			}
		}
	}()

	for inputScanner.Scan() {
		select {
		case <-dead:
			return
		default:
		}
		if err := conn.SendChat(name, inputScanner.Text()); err != nil {
			fmt.Printf("Send failed: %v\n", err)
			return
		}
	}
	conn.Close()
	<-dead
}

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
			err = conn.SendHandshake(cleaned)
			if err != nil {
				return "", err
			}
			// Check the response
			resp, err := conn.Recv()
			if err != nil {
				fmt.Printf("Receive error while setting username: %v", err)
				return "", err
			}
			if resp.Kind != protocol.KindHandshakeAck {
				return "", errors.New("Didn't receive handshake ack from server")

			}
			var ack protocol.HandshakeAck
			err = json.Unmarshal(resp.Payload, &ack)
			if err != nil {
				return "", nil
			}
			// Valid.
			return ack.Name, nil
		}
		if inputScanner.Err() == nil {
			// Normal closure
			return "", nil
		}
		return "", errors.New("Username Failure.")
	}

}

func cleanUserName(name string) (string, error) {
	// TODO, consider validation rules. Pass for now with assumptions.
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("Username cannot be blank.")
	}
	return name, nil
}
