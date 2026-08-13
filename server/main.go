package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

// A struct for requesting the number of clients connected
type ClientNumReq struct {
	reply chan int
}

func main() {
	// Create the raw TCP connection. TODO upgrade to TLS.
	ln, err := net.Listen("tcp", ":2069")
	if err != nil {
		log.Fatal("Could not open server")
	}

	hub := newHub()

	// Base server loop. Start the goroutines and block on killServer
	// Perhaps turn killServer into a select call.
	killServer := make(chan struct{})
	go hub.run()
	go clientListener(hub, ln)
	<-killServer
}

// Listen for incoming client connections, create them, then start a goroutine to handle them.
func clientListener(hub *Hub, ln net.Listener) {
	for {
		// Listen for incoming connections
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error accepting a connection: %v", err)
			continue
		}
		// A connection is detected. Make the client and send it to the hub.
		newClient := newClient(conn)
		hub.Register <- newClient
		go handleConnection(newClient, hub.Broadcast, hub.Unregister)
		go newClient.writeLoop()
	}
}

// The channels for the chat are processed here, as is the clientList
func (h *Hub) run() {
	clientList := make(map[*Client]struct{})
	for {
		select {
		case c := <-h.Register:
			fmt.Println("Received a registration request.")
			clientList[c] = struct{}{}
		case c := <-h.Unregister:
			fmt.Println("Received an unregistration request")
			c.Connection.Close()
			delete(clientList, c)
		case msg := <-h.Broadcast:
			fmt.Println("Received a broadcast request")
			for client := range clientList {
				client.Out <- msg
			}
		case req := <-h.Query:
			clientNum := len(clientList)
			req.reply <- clientNum

		}
	}
}

func (h *Hub) clientCount() int {
	ch := make(chan int, 1)
	h.Query <- ClientNumReq{reply: ch}
	return <-ch
}

// Reads what the client sends, and closes clients when they are gone.
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
