package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

type Hub struct {
	Register   chan *Client         // Register a new client
	Unregister chan *Client         // Unregister a client
	Broadcast  chan protocol.Frame  // Send a frame to all clients
	Query      chan ClientNumReq    // Send a query to the server
	NameTaken  chan NameReq         // Ask if a username is in use
	Done       chan struct{}        // Signal we're done with the hub. Begin teardown.
	Finished   chan struct{}        // Signal the hub is completely closed.
	clientList map[*Client]struct{} // Map of all clients.
}

// Create a new hub
func newHub() *Hub {
	hub := Hub{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan protocol.Frame),
		Query:      make(chan ClientNumReq),
		NameTaken:  make(chan NameReq),
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
		case c := <-h.Register:
			h.clientList[c] = struct{}{}
		case c := <-h.Unregister:
			h.closeClient(c)
		case f := <-h.Broadcast:
			// TODO: Should the hub be doing all of this?
			if f.Kind == protocol.KindChat {
				decorated, err := decorateChat(f)
				if err != nil {
					log.Printf("dropping malformed chat frame: %v", err)
					continue
				}
				f = decorated
			}
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
		case req := <-h.NameTaken:
			taken := false
			for c := range h.clientList {
				if c.Name == req.name {
					taken = true
					break
				}
			}
			req.reply <- taken
		case <-h.Done:
			// Start teardown by closing all clients. h.Finished will signal the rest.
			for c := range h.clientList {
				h.closeClient(c)
			}
			return
		}
	}
}

func (h *Hub) clientCount() int {
	ch := make(chan int, 1)
	h.Query <- ClientNumReq{reply: ch}
	return <-ch
}

func (h *Hub) nameTaken(name string) bool {
	ch := make(chan bool, 1)
	select {
	case h.NameTaken <- NameReq{name: name, reply: ch}:
	case <-h.Done:
		return true
	}
	select {
	case taken := <-ch:
		return taken
	case <-h.Done:
		return true
	}
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

func decorateChat(f protocol.Frame) (protocol.Frame, error) {
	var chat protocol.Chat
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		return protocol.Frame{}, err
	}
	return protocol.NewChatFrame(chat.From, fmt.Sprintf(messageFormat, chat.From, chat.Text))
}
