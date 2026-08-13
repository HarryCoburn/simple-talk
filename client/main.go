package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:2069") // TODO, change to ask for a connection string.
	inputScanner := bufio.NewScanner(os.Stdin)
	outputReader := bufio.NewReader(conn)
	if err != nil {
		log.Fatal("Client could not dial to the server.")
	}

	ch := make(chan struct{})
	go sendClientInput(inputScanner, conn)
	go acceptServerOutput(outputReader, ch)
	<-ch
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
		fmt.Println(status)
	}
}
