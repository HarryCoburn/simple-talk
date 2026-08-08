package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:2069") // TODO, change to ask for a connection string.
	if err != nil {
		log.Fatal("Client could not dial to the server.")
	}
	fmt.Fprintf(conn, "Hello\n")
	status, err := bufio.NewReader(conn).ReadString('\n')
	fmt.Println(status)
}
