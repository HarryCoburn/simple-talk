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
		if errors.Is(err, ErrNameTaken) {
			if sendErr := fullc.SendError(err.Error()); sendErr != nil {
				log.Printf("could not report taken name to %s: %v", clientName, sendErr)
			}
		}
		fullc.Close()
		return
	}

	// Send handshake ack
	if err := fullc.SendHandshakeAck(clientName); err != nil {
		log.Printf("handshake ack to %s has failed: %v", clientName, err)
		newClient.leave(hub)
		return
	}

	// Announce successful connection to others.
	f, err := announceConnection(newClient.Name)
	if err != nil {
		log.Printf("could not build connect announcement for %s: %v", newClient.Name, err)
		newClient.leave(hub)
		return
	}
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
		return "", fmt.Errorf("problem with handshake frame: %v", err)
	}
	fullc.SetReadDeadline(time.Time{})

	// Is it the right kind?
	if f.Kind != protocol.KindHandshake {
		return "", fmt.Errorf("wrong frame type sent in handshake! : %v", err)
	}

	var hs protocol.Handshake
	if err := json.Unmarshal(f.Payload, &hs); err != nil {
		return "", fmt.Errorf("something wrong with the name payload: %v", err)
	}

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

func announceDisconnection(name string) (protocol.Frame, error) {
	msg := fmt.Sprintf("%s has disconnected.", name)
	f, err := protocol.NewSystemFrame(msg)
	if err != nil {
		return protocol.Frame{}, err
	}
	return f, nil
}
