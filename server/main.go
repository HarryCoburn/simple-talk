package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

type RegisterReq struct {
	client *Client
	reply  chan error
}

// Run starts the server and blocks until an interrupt or SIGTERM
func Run() {
	// Create the raw TCP connection. TODO upgrade to TLS.
	ln, err := net.Listen("tcp", ":2069")
	if err != nil {
		log.Fatal("Could not open server")
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	serve(ln, signals)
}

func serve(ln net.Listener, shutdown <-chan os.Signal) {

	hub := newHub()

	stopped := make(chan struct{})
	go hub.run()
	go func() {
		<-hub.Done
		ln.Close()
	}()
	go acceptLoop(hub, ln, stopped)

	select {
	case sig := <-shutdown:
		log.Printf("received %v, shutting down", sig)
	case <-stopped:
		// Listener died, tear down the rest
		log.Print("stopped accepting connections, shutting down")
	}

	close(hub.Done)
	<-stopped
	<-hub.Finished
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
		log.Print(err)
		fullc.Close()
		return
	}

	newClient := newClient(fullc, clientName)

	if err := hub.Register(newClient); err != nil {
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
	if err := hub.Broadcast(f); err != nil {
		log.Printf("could not announce %s: %v", newClient.Name, err)
		newClient.leave(hub)
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
		return "", fmt.Errorf("problem with handshake frame: %w", err)
	}
	fullc.SetReadDeadline(time.Time{})

	// Is it the right kind?
	if f.Kind != protocol.KindHandshake {
		return "", fmt.Errorf("wrong frame type sent in handshake! Received kind: %v", f.Kind)
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
