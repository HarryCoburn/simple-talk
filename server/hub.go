package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
	"github.com/HarryCoburn/simple-talk/internal/validate"
)

var (
	ErrNameTaken = errors.New("that name is already taken")
	ErrHubClosed = errors.New("server is shutting down")

	// ErrVersionMismatch marks a handshake the server cannot honour because the
	// client speaks a different version. It is reported to the client rather
	// than logged and dropped: only the client can act on it.
	ErrVersionMismatch = errors.New("client and server versions do not match")
)

type registerReq struct {
	session *Session
	reply   chan error
}

type Hub struct {
	register   chan registerReq    // Register a new session
	unregister chan *Session       // Unregister a session
	broadcast  chan protocol.Frame // Send a frame to all sessions
	tasks      chan func(*Hub)     // Run a closure inside Hub.run without disrupting serialization
	sessions   map[string]*Session // All sessions, keyed by the folded form of their name.
	version    string              // The protocol version this server speaks

	// ctx is the hub's lifetime, not a per-call context. Storing one in a
	// struct is usually wrong; it is right here because the hub is a long-lived
	// actor whose callers arrive on their own goroutines with no request of
	// their own to scope. Cancelling it asks for teardown.
	ctx    context.Context
	cancel context.CancelFunc

	// finished answers a different question than ctx.Done(): teardown is
	// complete, not merely requested. serve depends on the difference, and
	// context has no equivalent.
	finished chan struct{}
}

// Create a new hub. Cancelling ctx tears the hub down, as stop does.
func newHub(ctx context.Context, version string) *Hub {
	ctx, cancel := context.WithCancel(ctx)
	hub := Hub{
		register:   make(chan registerReq),
		unregister: make(chan *Session),
		broadcast:  make(chan protocol.Frame),
		tasks:      make(chan func(*Hub)),
		sessions:   make(map[string]*Session),
		version:    version,
		ctx:        ctx,
		cancel:     cancel,
		finished:   make(chan struct{}),
	}
	return &hub
}

// Start the hub
func (h *Hub) run() {
	defer close(h.finished)

	for {
		select {
		case req := <-h.register:
			// The name must be folded, checked and claimed in the same loop
			// call, or two sessions racing for one name could both find it free.
			key, err := validate.NameKey(req.session.Name)
			if err != nil {
				// A name that will not fold cannot be held: it could not be
				// matched against later.
				req.reply <- ErrNameTaken
				continue
			}
			if _, taken := h.sessions[key]; taken {
				req.reply <- ErrNameTaken
				continue
			}
			// The key belongs to the session from here on: every later lookup
			// uses it, so it must not be recomputed from a name that could
			// have moved on.
			req.session.key = key
			h.sessions[key] = req.session
			req.reply <- nil
		case c := <-h.unregister:
			if !h.registered(c) {
				continue // Session is already unregistered.
			}
			h.closeSession(c)
			f, err := announcePresence(c.Name, eventDisconnected)
			if err != nil {
				log.Printf("could not build disconnect announcement for %s: %v", c.Name, err)
			}
			h.deliver(f)
		case f := <-h.broadcast:
			h.deliver(f)
		case task := <-h.tasks:
			// This closure runs inside the hub goroutine, so it must not call
			// back into the hub: every such call waits on this loop. It is why
			// command handlers are kept out of here and run on the caller's
			// goroutine instead.
			task(h)
		case <-h.ctx.Done():
			// Start teardown by closing all sessions. h.Finished will signal the rest.
			for _, c := range h.sessions {
				h.closeSession(c)
			}
			return
		}
	}
}

// stop signals the hub to begin a teardown. stop does not wait. Pair with wait if you need that.
// It is safe to call more than once: context.CancelFunc is idempotent.
func (h *Hub) stop() {
	h.cancel()
}

// closedErr reports a shutdown to a caller. ErrHubClosed stays the sentinel
// callers match on -- it is this package's vocabulary, where context.Canceled
// is plumbing -- but the cause travels with it, so errors.Is finds both.
func (h *Hub) closedErr() error {
	return fmt.Errorf("%w: %w", ErrHubClosed, h.ctx.Err())
}

// wait blocks until run() has torn the hub down. Only returns if run() was started and stop was called
func (h *Hub) wait() {
	<-h.finished
}

// doneChan reports hub shutdown to callers outside the hub goroutine.
func (h *Hub) doneChan() <-chan struct{} {
	return h.ctx.Done()
}

// Channel Calls
//
// These cross the hub goroutine boundary but stay inside the package: nothing
// outside package server holds a *Hub.
func (h *Hub) registerSession(c *Session) error {
	// Buffering
	ch := make(chan error, 1)
	select {
	case h.register <- registerReq{session: c, reply: ch}:
	case <-h.ctx.Done():
		return h.closedErr()
	}
	select {
	case err := <-ch:
		return err
	case <-h.ctx.Done():
		return h.closedErr()
	}
}

func (h *Hub) unregisterSession(c *Session) {
	select {
	case h.unregister <- c:
	case <-h.ctx.Done():
	}
}

// broadcastFrame is for callers outside the hub goroutine. Use deliver() for internal broadcasts
func (h *Hub) broadcastFrame(f protocol.Frame) error {
	select {
	case h.broadcast <- f:
	case <-h.ctx.Done():
		return h.closedErr()
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
	case <-h.ctx.Done():
		return zero, false
	}
	select {
	case v := <-ch:
		return v, true
	case <-h.ctx.Done():
		return zero, false
	}
}

// exec mutates the hub state from another goroutine
func exec(h *Hub, fn func(*Hub)) bool {
	_, ok := submit(h, func(h *Hub) struct{} {
		fn(h)
		return struct{}{}
	})
	return ok
}

func (h *Hub) sessionNames() ([]string, error) {
	names, ok := submit(h, func(h *Hub) []string {
		names := make([]string, 0, len(h.sessions))
		for _, c := range h.sessions {
			// The roster shows the name as the user typed it, not the folded
			// key it is stored under.
			names = append(names, c.Name)
		}
		slices.Sort(names)
		return names
	})
	if !ok {
		return nil, h.closedErr()
	}
	return names, nil
}

// registered reports whether this exact session is the one currently holding its
// key. A name is not a unique handle across a reconnect: both of a session's
// goroutines can call leave, and if the user reconnects under the same name
// between the two, the stale leave would otherwise find the new session's entry
// and tear it down.
func (h *Hub) registered(c *Session) bool {
	current, ok := h.sessions[c.key]
	return ok && current == c
}

func (h *Hub) closeSession(c *Session) {
	if !h.registered(c) {
		return
	}
	close(c.Out)
	if c.Conn != nil {
		c.Conn.Close()
	}
	delete(h.sessions, c.key)
}

// sendTo queues a frame for a single session. Because deliverTo can drop a session whose
// Out is not responding, it uses exec
func (h *Hub) sendTo(c *Session, f protocol.Frame) error {
	if !exec(h, func(h *Hub) {
		h.deliverTo(c, f)
	}) {
		return h.closedErr()
	}
	return nil
}

// deliverTo queues one frame for one session in the hub goroutine.
func (h *Hub) deliverTo(c *Session, f protocol.Frame) {
	if !h.registered(c) {
		return // already gone
	}
	select {
	case c.Out <- f:
	default: // assume the session is gone if we cannot reach c.Out
		h.closeSession(c)
	}
}

func (h *Hub) deliver(f protocol.Frame) {
	for c := range h.sessions {
		h.deliverTo(h.sessions[c], f)
	}
}
