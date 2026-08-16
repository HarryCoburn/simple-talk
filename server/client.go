package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// An individual client connection.
type Client struct {
	Conn *protocol.Conn      // The protocol connection
	Name string              // The username of the Client
	Out  chan protocol.Frame // The message channel queue
}

func newClient(conn *protocol.Conn, name string) *Client {
	return &Client{
		Conn: conn,
		Name: name,
		Out:  make(chan protocol.Frame, 256),
	}
}

func (c *Client) processClientOutQueue() {
	// Listens to the client's Out queue, then writes any message received to Writer for display.
	defer c.Conn.Close()
	for frame := range c.Out {
		// Out should hold a completed frame
		err := c.Conn.SendFrame(frame)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func (c *Client) readClientInput(hub *Hub) {
	for {
		// Reads what the client writes. Closes client safely if there is a problem with reading.
		frame, err := c.Conn.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Connection closed cleanly")
			} else {
				fmt.Printf("connection broke, not EOF: %v", err)
			}
			select {
			case hub.Unregister <- c:
				return
			case <-hub.Done:
				return
			}
		}

		if frame.Kind != protocol.KindChat {
			fmt.Printf("Didn't receive a chat. Problem!")
			select {
			case hub.Unregister <- c:
				return
			case <-hub.Done:
				return
			}
		}

		// Restamp the sender. The client supplies its own From field, which we
		// don't trust; the name agreed during the handshake is authoritative.
		var chat protocol.Chat
		if err := json.Unmarshal(frame.Payload, &chat); err != nil {
			fmt.Printf("Discarding unreadable chat payload: %v\n", err)
			continue
		}
		frame, err = protocol.NewChatFrame(c.Name, chat.Text)
		if err != nil {
			fmt.Printf("Could not rebuild the chat frame: %v\n", err)
			continue
		}

		// Send the frame on
		select {
		case hub.Broadcast <- frame:
		case <-hub.Done:
			return
		}
	}
}
