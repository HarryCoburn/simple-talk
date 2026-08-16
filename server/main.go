package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// A struct for requesting the number of clients connected
type ClientNumReq struct {
	reply chan int
}

// A struct for asking the hub whether a username is already in use
type NameReq struct {
	name  string
	reply chan bool
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
	go func() {
		<-hub.Done
		ln.Close()
	}()
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
		go handleNewConn(hub, conn)
	}
}

func handleNewConn(hub *Hub, conn net.Conn) {
	// A connection is detected. Make the client
	fullc := protocol.NewConn(conn)
	clientName, err := verifyName(hub, fullc)
	if err != nil {
		fmt.Println(err)
		fullc.Close()
		return
	}

	newClient := newClient(fullc, clientName)

	err = fullc.SendHandshakeAck(clientName)
	if err != nil {
		fmt.Println(err)
		fullc.Close()
		return
	}

	select {
	case hub.Register <- newClient:
	case <-hub.Done:
		conn.Close()
		return
	}
	go newClient.processClientOutQueue()
	go newClient.readClientInput(hub)
}

func verifyName(hub *Hub, fullc *protocol.Conn) (string, error) {
	// Get the frame for name entry.
	fullc.SetReadDeadline(time.Now().Add(protocol.HandshakeTimeout))
	f, err := fullc.Recv()
	if err != nil {
		return "", fmt.Errorf("Problem with handshake frame: %v", err)
	}
	fullc.SetReadDeadline(time.Time{})

	// Is it the right kind?
	if f.Kind != protocol.KindHandshake {
		return "", fmt.Errorf("Wrong frame type sent in handshake! : %v", err)
	}

	var hs protocol.Handshake
	err = json.Unmarshal(f.Payload, &hs)
	if err != nil {
		return "", fmt.Errorf("Something wrong with the name payload: %v", err)
	}
	clientName := hs.Name

	// Ask the hub rather than reading its map: clientList belongs to hub.run's
	// goroutine.
	if hub.nameTaken(clientName) {
		return "", fmt.Errorf("Name already taken. Choose another")
	}

	return clientName, nil
}
