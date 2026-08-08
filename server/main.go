package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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
		reader := bufio.NewReader(conn)
		go handleConnection(conn, reader)
	}
}

func handleConnection(conn net.Conn, reader *bufio.Reader) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Connection closed cleanly")
				return
			}
			fmt.Printf("connection broke, not EOF: %v", err)
			return
		}
		fmt.Print(line)
		fmt.Println("Received a connection message.")
		conn.Write((fmt.Appendf([]byte{}, "Received message: %s", line)))
	}
}
