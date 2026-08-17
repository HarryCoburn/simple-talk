package main

import (
	"errors"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// Reasons a registration can be refused. ErrNameTaken is the client's fault and
// is worth reporting back to them; ErrHubClosed means the server is going away
// and there is nobody left to report to.
var (
	ErrNameTaken = errors.New("that name is already taken")
	ErrHubClosed = errors.New("server is shutting down")
)

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

const messageFormat = "<%s> %s"

// Start the hub
func (h *Hub) run() {
	defer close(h.Finished)

	for {
		select {
		case req := <-h.Register:
			// The name check and the insert have to happen in the same turn of
			// this loop. Splitting them lets two clients with the same name both
			// see a free name and both register.
			if h.nameInUse(req.client.Name) {
				req.reply <- ErrNameTaken
				continue
			}
			h.clientList[req.client] = struct{}{}
			req.reply <- nil
		case c := <-h.Unregister:
			h.closeClient(c)
		case f := <-h.Broadcast:

			// Fanout
			for c := range h.clientList {
				select {
				case c.Out <- f:
				default: // If a c.Out cannot be reached, assume the client has left and close it.
					h.closeClient(c)
				}
			}
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

// register adds c to the hub, returning ErrNameTaken if another connected
// client already answers to that name, or ErrHubClosed during shutdown.
func (h *Hub) register(c *Client) error {
	// Buffered, so the hub never blocks handing back an answer nobody is
	// waiting for any more.
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

// nameInUse reports whether a connected client already holds this name. Only
// safe to call from hub.run, which owns clientList.
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
