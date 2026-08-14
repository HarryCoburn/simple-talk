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
		select {
		case <-hub.Done:
			return
		default:
		}
		// Listen for incoming connections
		conn, err := ln.Accept()

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("Error accepting a connection: %v\n", err)
			continue
		}
		go handleConn(hub, conn)
	}
}

func handleConn(hub *Hub, conn net.Conn) {
	// A connection is detected. Make the client
	newClient := newClient(conn)
	err := newClient.setUserName()
	if err != nil {
		fmt.Printf("Error setting user name. Closing. Error received: %v", err)
		conn.Close()
		return
	}

	select {
	case hub.Register <- newClient:
	case <-hub.Done:
		conn.Close()
		return
	}
	go newClient.writeLoop()
	go newClient.readLoop(hub)

}
