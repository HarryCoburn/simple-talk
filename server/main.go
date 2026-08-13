package main

import (
	"errors"
	"fmt"
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
	go acceptLoop(hub, ln, killServer)
	<-killServer
}

// Listen for incoming client connections, create them, then start a goroutine to handle them.
func acceptLoop(hub *Hub, ln net.Listener, stopped chan struct{}) {
	defer close(stopped)
	for {
		// Listen for incoming connections
		conn, err := ln.Accept()

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("Error accepting a connection: %v\n", err)
			continue
		}
		// A connection is detected. Make the client and send it to the hub.
		newClient := newClient(conn)

		select {
		case hub.Register <- newClient:
		case <-hub.Done:
			conn.Close()
			return
		}
		go newClient.writeLoop()
		go newClient.readLoop(hub)

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
			if _, ok := clientList[c]; !ok { // Already removed, possibly by the drop path
				continue
			}
			close(c.Out) // Close writer loop
			delete(clientList, c)
		case msg := <-h.Broadcast:
			msg.Data = fmt.Appendf(nil, "<%s> %s", msg.From.Name, msg.Data)
			for client := range clientList {
				select {
				case client.Out <- msg:
				default: // Client has a problem. Start closing the client.
					close(client.Out)
					delete(clientList, client)
				}
			}
		case req := <-h.Query:
			clientNum := len(clientList)
			req.reply <- clientNum
		case <-h.Done: // For safe teardown
			for c := range clientList {
				close(c.Out)
			}
			return
		}
	}
}

func (h *Hub) clientCount() int {
	ch := make(chan int, 1)
	h.Query <- ClientNumReq{reply: ch}
	return <-ch
}
