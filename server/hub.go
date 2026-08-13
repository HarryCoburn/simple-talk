package main

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
