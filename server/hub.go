package main

// Chat hub
type Hub struct {
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan []byte
	Query      chan ClientNumReq
}

// Create a new hub
func newHub() *Hub {
	hub := Hub{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan []byte),
		Query:      make(chan ClientNumReq),
	}
	return &hub
}
