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

// A request to add a client to the hub. reply carries nil on success, or the
// reason the registration was refused.
type RegisterReq struct {
	client *Client
	reply  chan error
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
	clientName, err := verifyName(fullc)
	if err != nil {
		fmt.Println(err)
		fullc.Close()
		return
	}

	newClient := newClient(fullc, clientName)

	if err := hub.register(newClient); err != nil {
		// A taken name is the client's problem to fix, so tell them why before
		// hanging up. A closed hub means there is nothing left to say.
		if errors.Is(err, ErrNameTaken) {
			if sendErr := fullc.SendError(err.Error()); sendErr != nil {
				fmt.Println(sendErr)
			}
		}
		fullc.Close()
		return
	}

	// From here the client is in the hub's list, so every failure has to leave
	// through the hub. The hub owns the socket and closes it.
	err = fullc.SendHandshakeAck(clientName)
	if err != nil {
		fmt.Println(err)
		newClient.leave(hub)
		return
	}
	// Announce successful connection to others.
	f, err := announceConnection(newClient.Name)
	if err != nil {
		fmt.Println(err)
		newClient.leave(hub)
		return
	}
	// Guarded: an unguarded send here blocks forever if the hub stops between
	// registering and announcing, and this goroutine never exits.
	select {
	case hub.Broadcast <- f:
	case <-hub.Done:
		return
	}

	go newClient.processClientOutQueue(hub)
	go newClient.readClientInput(hub)
}

func verifyName(fullc *protocol.Conn) (string, error) {
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
	// The name is only checked for availability at registration time, where the
	// check and the insert are one atomic hub operation.
	return hs.Name, nil
}

func announceConnection(name string) (protocol.Frame, error) {
	msg := fmt.Sprintf("%s has connected.", name)
	f, err := protocol.NewSystemFrame(msg)
	if err != nil {
		return protocol.Frame{}, err
	}
	return f, nil
}
