package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

const userNamePrompt = "Please state your username: \n"

func main() {
	bareConn, err := net.Dial("tcp", "localhost:2069") // TODO, change to ask for a connection string.
	if err != nil {
		log.Fatal("Client could not dial to the server.")
	}

	inputScanner := bufio.NewScanner(os.Stdin)
	conn := protocol.NewConn(bareConn)
	err = setUserName(conn, inputScanner)
	if err != nil {
		log.Fatal(err)
	}
}

func setUserName(conn *protocol.FullConn, inputScanner *bufio.Scanner) error {
	fmt.Println(userNamePrompt)
	for { // To handle reasking if there's a problem. Break if successful.
		// Get a name and clean it properly
		if inputScanner.Scan() {
			input := inputScanner.Text()
			cleaned, err := cleanUserName(input)
			if err != nil {
				fmt.Println("Error in input. Try again")
				fmt.Println(userNamePrompt)
				continue
			}
			// Try to send it to the server for further validation
			err = conn.SendHandshake(cleaned)
			if err != nil {
				log.Fatal(err)
			}
			// Check the response
			resp, err := conn.Recv()
			if err != nil {
				log.Fatalf("Receive error while setting username: %v", err)
			}
			if resp.Kind != protocol.KindHandshakeAck {
				log.Fatalf("Didn't receive handshake ack from server")
			}
			// Valid.
			return nil
		}

		return errors.New("Username Failure.")
	}

}

func cleanUserName(name string) (string, error) {
	// TODO, consider validation rules. Pass for now with assumptions.
	return strings.TrimSuffix(name, "\n"), nil
}
