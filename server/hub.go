package main

import (
	"errors"
	"log"
	"sort"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

var (
	ErrNameTaken = errors.New("that name is already taken")
	ErrHubClosed = errors.New("server is shutting down")
)

const messageFormat = "<%s> %s"
const poseFormat = "%s %s"

type Hub struct {
	Register   chan RegisterReq     // Register a new client
	Unregister chan *Client         // Unregister a client
	Broadcast  chan protocol.Frame  // Send a frame to all clients
	Query      chan func(*Hub)      // Run a read-only query inside run(), where clientList is owned
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
		Query:      make(chan func(*Hub)),
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
			if _, ok := h.clientList[c]; !ok {
				continue // Client is already unregistered.
			}
			h.closeClient(c)
			f, err := announceDisconnection(c.Name)
			if err != nil {
				log.Printf("could not build disconnect announcement for %s: %v", c.Name, err)
			}
			h.deliver(f)
		case f := <-h.Broadcast:
			h.deliver(f)
		case query := <-h.Query:
			// Queries run on the hub goroutine, so they may read clientList
			// directly. They must not retain anything they are handed.
			query(h)
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

// query runs fn on the hub goroutine and returns its result. ok is false if the
// hub shut down before the query could run. Methods cannot be generic, so this
// is a plain function.
func query[T any](h *Hub, fn func(*Hub) T) (T, bool) {
	ch := make(chan T, 1) // Buffered so run() never blocks if we give up on the reply.
	req := func(h *Hub) { ch <- fn(h) }

	var zero T
	select {
	case h.Query <- req:
	case <-h.Done:
		return zero, false
	}
	select {
	case v := <-ch:
		return v, true
	case <-h.Done:
		return zero, false
	}
}

func (h *Hub) clientCount() int {
	n, _ := query(h, func(h *Hub) int {
		return len(h.clientList)
	})
	return n
}

// clientNames returns a sorted snapshot of the names of every connected client.
func (h *Hub) clientNames() []string {
	names, _ := query(h, func(h *Hub) []string {
		names := make([]string, 0, len(h.clientList))
		for c := range h.clientList {
			names = append(names, c.Name)
		}
		sort.Strings(names)
		return names
	})
	return names
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

// sendTo queues a frame for a single client. The send happens on the hub
// goroutine because the hub owns c.Out's lifetime — closeClient closes it, so a
// send from any other goroutine could race with a close and panic.
func (h *Hub) sendTo(c *Client, f protocol.Frame) {
	query(h, func(h *Hub) struct{} {
		h.deliverTo(c, f)
		return struct{}{}
	})
}

// deliverTo queues one frame for one client. Hub goroutine only.
func (h *Hub) deliverTo(c *Client, f protocol.Frame) {
	if _, ok := h.clientList[c]; !ok {
		return // Already gone; nothing to deliver to.
	}
	select {
	case c.Out <- f:
	default: // If a c.Out cannot be reached, assume the client has left and close it.
		h.closeClient(c)
	}
}

func (h *Hub) deliver(f protocol.Frame) {
	for c := range h.clientList {
		h.deliverTo(c, f)
	}
}
