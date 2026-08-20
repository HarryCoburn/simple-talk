package main

import (
	"errors"
	"log"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

var (
	ErrNameTaken = errors.New("that name is already taken")
	ErrHubClosed = errors.New("server is shutting down")
)

const messageFormat = "<%s> %s"

type Hub struct {
	Register   chan RegisterReq     // Register a new client
	Unregister chan *Client         // Unregister a client
	Broadcast  chan protocol.Frame  // Send a frame to all clients
	Query      chan ClientNumReq    // Send a query to the server
	Done       chan struct{}        // Signal we're done with the hub. Begin teardown.
	Finished   chan struct{}        // Signal the hub is completely closed.
	clientList map[*Client]struct{} // Map of all clients.
}

// Create a new hub
func newHub() *Hub {
	hub := Hub{
		Register:   make(chan RegisterReq),
		Unregister: make(chan *Client),
		Broadcast:  make(chan protocol.Frame),
		Query:      make(chan ClientNumReq),
		Done:       make(chan struct{}),
		Finished:   make(chan struct{}),
		clientList: make(map[*Client]struct{}),
	}
	return &hub
}

// Start the hub
func (h *Hub) run() {
	defer close(h.Finished)

	for {
		select {
		case req := <-h.Register:
			// must check the name and insert in the same loop call
			if h.nameInUse(req.client.Name) {
				req.reply <- ErrNameTaken
				continue
			}
			h.clientList[req.client] = struct{}{}
			req.reply <- nil
		case c := <-h.Unregister:
			f, err := announceDisconnection(c.Name)
			if err != nil {
				log.Print(err)
			}
			h.deliver(f)
			h.closeClient(c)
		case f := <-h.Broadcast:
			h.deliver(f)
		case req := <-h.Query:
			// Currenly only handles clientList length requests
			clientNum := len(h.clientList)
			req.reply <- clientNum
		case <-h.Done:
			// Start teardown by closing all clients. h.Finished will signal the rest.
			for c := range h.clientList {
				h.closeClient(c)
			}
			return
		}
	}
}

func (h *Hub) register(c *Client) error {
	// Buffering
	ch := make(chan error, 1)
	select {
	case h.Register <- RegisterReq{client: c, reply: ch}:
	case <-h.Done:
		return ErrHubClosed
	}
	select {
	case err := <-ch:
		return err
	case <-h.Done:
		return ErrHubClosed
	}
}

func (h *Hub) clientCount() int {
	ch := make(chan int, 1)
	select {
	case h.Query <- ClientNumReq{reply: ch}:
	case <-h.Done:
		return 0
	}
	select {
	case n := <-ch:
		return n
	case <-h.Done:
		return 0
	}
}

func (h *Hub) nameInUse(name string) bool {
	for c := range h.clientList {
		if c.Name == name {
			return true
		}
	}
	return false
}

func (h *Hub) closeClient(c *Client) {
	if _, ok := h.clientList[c]; !ok {
		return
	}
	close(c.Out)
	if c.Conn != nil {
		c.Conn.Close()
	}
	delete(h.clientList, c)
}

func (h *Hub) deliver(f protocol.Frame) {
	for c := range h.clientList {
		select {
		case c.Out <- f:
		default: // If a c.Out cannot be reached, assume the client has left and close it.
			h.closeClient(c)
		}
	}
}
