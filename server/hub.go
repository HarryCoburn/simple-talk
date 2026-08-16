package main

import "fmt"

type Hub struct {
	Register   chan *Client         // Register a new client
	Unregister chan *Client         // Unregister a client
	Broadcast  chan Message         // Send a message to all clients
	Query      chan ClientNumReq    // Send a query to the server
	Done       chan struct{}        // Signal we're done with the hub. Begin teardown.
	Finished   chan struct{}        // Signal the hub is completely closed.
	clientList map[*Client]struct{} // Map of all clients.
}

type Message struct {
	From *Client // Address of the client that sent the message
	Data []byte  // Content of the message
}

// Create a new hub
func newHub() *Hub {
	hub := Hub{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan Message),
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
		case c := <-h.Register:
			fmt.Println("Received a registration request.")
			h.clientList[c] = struct{}{}
		case c := <-h.Unregister:
			fmt.Println("Received an unregistration request")
			h.closeClient(c)
		case msg := <-h.Broadcast:
			msg.Data = fmt.Appendf(nil, messageFormat, msg.From.Name, msg.Data)
			// Fanout. TODO, a field in Message to know to do a private send?
			for c := range h.clientList {
				select {
				case c.Out <- msg:
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

func (h *Hub) clientCount() int {
	ch := make(chan int, 1)
	h.Query <- ClientNumReq{reply: ch}
	return <-ch
}

func (h *Hub) closeClient(c *Client) {
	if _, ok := h.clientList[c]; !ok {
		return
	}
	close(c.Out)
	delete(h.clientList, c)
}
