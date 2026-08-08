package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	// Create the raw TCP connection. TODO upgrade to TLS.
	ln, err := net.Listen("tcp", ":2069")
	if err != nil {
		log.Fatal("Could not open server")
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error accepting a connection: %v", err)
		}
		go handleConnection(conn)
	}

}
