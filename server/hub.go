package main

import "fmt"

// Chat hub
type Hub struct {
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan Message
	Query      chan ClientNumReq
	Done       chan struct{}
}

type Message struct {
	From *Client
	Data []byte
}

// Create a new hub
func newHub() *Hub {
	hub := Hub{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan Message),
		Query:      make(chan ClientNumReq),
		Done:       make(chan struct{}),
	}
	return &hub
}

// The channels for the chat are processed here, as is the clientList
func (h *Hub) run() {
	clientList := make(map[*Client]struct{})
	for {
		select {
		case c := <-h.Register:
			fmt.Println("Received a registration request.")
			clientList[c] = struct{}{}
		case c := <-h.Unregister:
			fmt.Println("Received an unregistration request")
			if _, ok := clientList[c]; !ok { // Already removed, possibly by the drop path
				continue
			}
			close(c.Out) // Close writer loop
			delete(clientList, c)
		case msg := <-h.Broadcast:
			msg.Data = fmt.Appendf(nil, "<%s> %s", msg.From.Name, msg.Data)
			for client := range clientList {
				select {
				case client.Out <- msg:
				default: // Client has a problem. Start closing the client.
					close(client.Out)
					delete(clientList, client)
				}
			}
		case req := <-h.Query:
			clientNum := len(clientList)
			req.reply <- clientNum
		case <-h.Done: // For safe teardown
			for c := range clientList {
				close(c.Out)
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
