package server

import (
	"errors"
	"log"
	"slices"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

var (
	ErrNameTaken = errors.New("that name is already taken")
	ErrHubClosed = errors.New("server is shutting down")
)

const messageFormat = "<%s> %s"
const poseFormat = "%s %s"

type Hub struct {
	register   chan RegisterReq     // Register a new client
	unregister chan *Client         // Unregister a client
	broadcast  chan protocol.Frame  // Send a frame to all clients
	query      chan func(*Hub)      // Run a read-only query inside Hub.run()
	Done       chan struct{}        // Signal we're done with the hub. Begin teardown.
	Finished   chan struct{}        // Signal the hub is completely closed.
	clientList map[*Client]struct{} // Map of all clients.
}

// Create a new hub
func newHub() *Hub {
	hub := Hub{
		register:   make(chan RegisterReq),
		unregister: make(chan *Client),
		broadcast:  make(chan protocol.Frame),
		query:      make(chan func(*Hub)),
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
		case req := <-h.register:
			// must check the name and insert in the same loop call
			if h.nameInUse(req.client.Name) {
				req.reply <- ErrNameTaken
				continue
			}
			h.clientList[req.client] = struct{}{}
			req.reply <- nil
		case c := <-h.unregister:
			if _, ok := h.clientList[c]; !ok {
				continue // Client is already unregistered.
			}
			h.closeClient(c)
			f, err := announceDisconnection(c.Name)
			if err != nil {
				log.Printf("could not build disconnect announcement for %s: %v", c.Name, err)
			}
			h.deliver(f)
		case f := <-h.broadcast:
			h.deliver(f)
		case query := <-h.query:
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

// Channel Calls
func (h *Hub) Register(c *Client) error {
	// Buffering
	ch := make(chan error, 1)
	select {
	case h.register <- RegisterReq{client: c, reply: ch}:
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

func (h *Hub) Unregister(c *Client) {
	select {
	case h.unregister <- c:
	case <-h.Done:
	}
}

// Broadcast is for external callers only. Use deliver() for internal broadcasts
func (h *Hub) Broadcast(f protocol.Frame) error {
	select {
	case h.broadcast <- f:
	case <-h.Done:
		return ErrHubClosed
	}
	return nil
}

// Internal hub processes

func query[T any](h *Hub, fn func(*Hub) T) (T, bool) {
	ch := make(chan T, 1)
	req := func(h *Hub) { ch <- fn(h) }

	var zero T
	select {
	case h.query <- req:
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

func (h *Hub) clientNames() []string {
	names, _ := query(h, func(h *Hub) []string {
		names := make([]string, 0, len(h.clientList))
		for c := range h.clientList {
			names = append(names, c.Name)
		}
		slices.Sort(names)
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

// sendTo queues a frame for a single client.
func (h *Hub) sendTo(c *Client, f protocol.Frame) {
	query(h, func(h *Hub) struct{} {
		h.deliverTo(c, f)
		return struct{}{}
	})
}

// deliverTo queues one frame for one client in the hub goroutine.
func (h *Hub) deliverTo(c *Client, f protocol.Frame) {
	if _, ok := h.clientList[c]; !ok {
		return // already gone
	}
	select {
	case c.Out <- f:
	default: // assume client is closed if we cannot reach c.Out
		h.closeClient(c)
	}
}

func (h *Hub) deliver(f protocol.Frame) {
	for c := range h.clientList {
		h.deliverTo(c, f)
	}
}
