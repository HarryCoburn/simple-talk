package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

type Client struct {
	Connection net.Conn
	Reader     *bufio.Reader
}

func main() {
	// Create the raw TCP connection. TODO upgrade to TLS.
	ln, err := net.Listen("tcp", ":2069")
	if err != nil {
		log.Fatal("Could not open server")
	}

	registerChannel := make(chan *Client)
	unregisterChannel := make(chan *Client)
	broadcastChannel := make(chan []byte)

	killServer := make(chan struct{})
	go runChatHub(registerChannel, unregisterChannel, broadcastChannel)
	go clientListener(registerChannel, unregisterChannel, broadcastChannel, ln)
	<-killServer

}

func clientListener(register chan *Client, unregister chan *Client, broadcast chan []byte, ln net.Listener) {
	for {
		// Listen for incoming connections
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error accepting a connection: %v", err)
		}
		// A connection is detected. Make the client and send it to the hub.
		reader := bufio.NewReader(conn)
		newClient := Client{
			Connection: conn,
			Reader:     reader,
		}
		register <- &newClient
		go handleConnection(&newClient, broadcast, unregister)
	}
}

func runChatHub(register chan *Client, unregister chan *Client, broadcast chan []byte) {
	clientList := make(map[*Client]struct{})
	for {
		select {
		case c := <-register:
			fmt.Println("Received a registration request.")
			clientList[c] = struct{}{}
		case c := <-unregister:
			fmt.Println("Received an unregistration request")
			c.Connection.Close()
			delete(clientList, c)
		case msg := <-broadcast:
			fmt.Println("Received a broadcast request")
			for client := range clientList {
				client.Connection.Write(fmt.Appendf([]byte{}, "%s", msg))
			}
		}
	}
}

func handleConnection(client *Client, broadcast chan []byte, unregister chan *Client) {
	for {
		line, err := client.Reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Connection closed cleanly")
			} else {
				fmt.Printf("connection broke, not EOF: %v", err)
			}
			unregister <- client
			return
		}
		broadcast <- []byte(line)
	}
}
