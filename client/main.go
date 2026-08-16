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

	conn := protocol.NewConn(bareConn)
	userName, err := setUserName()
	if err != nil {
		log.Fatal(err)
	}

	// ch := make(chan struct{})

	// go sendClientInput(inputScanner, conn)
	// go acceptServerOutput(outputReader, ch)
	// <-ch
}

func sendClientInput(inputScanner *bufio.Scanner, conn net.Conn) {
	for {
		if inputScanner.Scan() {
			input := inputScanner.Text()
			fmt.Fprintln(conn, input)
		}
	}
}

func acceptServerOutput(outputReader *bufio.Reader, ch chan struct{}) {
	for {
		status, err := outputReader.ReadString('\n')
		if err != nil {
			fmt.Println("Got an error or exit request")
			ch <- struct{}{}
			return
		}
		fmt.Print(status)
	}
}

func setUserName() (string, error) {
	inputScanner := bufio.NewScanner(os.Stdin)
	fmt.Println(userNamePrompt)
	if inputScanner.Scan() {
		input := inputScanner.Text()
		return strings.TrimSuffix(input, "\n"), nil // Send this in a handshake for validation.
	}
	return "", errors.New("Username Failure.")

}
