package server

import (
	"errors"
	"log"
	"slices"
	"sync"

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
	tasks      chan func(*Hub)      // Run a closure inside Hub.run without disrupting serialization
	done       chan struct{}        // Signal we're done with the hub. Begin teardown.
	finished   chan struct{}        // Signal the hub is completely closed.
	stopOnce   sync.Once            // Guards against a second close
	clientList map[*Client]struct{} // Map of all clients.
}

// Create a new hub
func newHub() *Hub {
	hub := Hub{
		register:   make(chan RegisterReq),
		unregister: make(chan *Client),
		broadcast:  make(chan protocol.Frame),
		tasks:      make(chan func(*Hub)),
		done:       make(chan struct{}),
		finished:   make(chan struct{}),
		clientList: make(map[*Client]struct{}),
	}
	return &hub
}

// Start the hub
func (h *Hub) run() {
	defer close(h.finished)

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
		case task := <-h.tasks:
			task(h)
		case <-h.done:
			// Start teardown by closing all clients. h.Finished will signal the rest.
			for c := range h.clientList {
				h.closeClient(c)
			}
			return
		}
	}
}

// Stop signals the hub to begin a teardown. Stop does not wait. Pair with Wait if you need that.
func (h *Hub) Stop() {
	h.stopOnce.Do(func() { close(h.done) })
}

// Wait blocks until run() has torn the hub down. Only returns if run() was started and Stop was called
func (h *Hub) Wait() {
	<-h.finished
}

// Done reports hub shutdown to outside parties.
func (h *Hub) Done() <-chan struct{} {
	return h.done
}

// Channel Calls
func (h *Hub) Register(c *Client) error {
	// Buffering
	ch := make(chan error, 1)
	select {
	case h.register <- RegisterReq{client: c, reply: ch}:
	case <-h.done:
		return ErrHubClosed
	}
	select {
	case err := <-ch:
		return err
	case <-h.done:
		return ErrHubClosed
	}
}

func (h *Hub) Unregister(c *Client) {
	select {
	case h.unregister <- c:
	case <-h.done:
	}
}

// Broadcast is for external callers only. Use deliver() for internal broadcasts
func (h *Hub) Broadcast(f protocol.Frame) error {
	select {
	case h.broadcast <- f:
	case <-h.done:
		return ErrHubClosed
	}
	return nil
}

// Internal hub processes

// submit runs fn on hub and returns the result. The boolis false if the hub is shut down before fn
// runs. Call query and exec to use it.
func submit[T any](h *Hub, fn func(*Hub) T) (T, bool) {
	ch := make(chan T, 1)
	task := func(h *Hub) { ch <- fn(h) }

	var zero T
	select {
	case h.tasks <- task:
	case <-h.done:
		return zero, false
	}
	select {
	case v := <-ch:
		return v, true
	case <-h.done:
		return zero, false
	}
}

// query runs functions on hub that do not mutate hub.
func query[T any](h *Hub, fn func(*Hub) T) (T, bool) {
	return submit(h, fn)
}

// exec mutates the hub state from another goroutine
func exec(h *Hub, fn func(*Hub)) bool {
	_, ok := submit(h, func(h *Hub) struct{} {
		fn(h)
		return struct{}{}
	})
	return ok
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

// sendTo queues a frame for a single client. Because deliverTo can drop a client whose
// Out is not responding, it uses exec
func (h *Hub) sendTo(c *Client, f protocol.Frame) {
	exec(h, func(h *Hub) {
		h.deliverTo(c, f)
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
