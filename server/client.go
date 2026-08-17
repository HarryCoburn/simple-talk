package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

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
			c.leave(hub)
			return
		}

		switch frame.Kind {
		case protocol.KindChat:
			// And now we decorate here instead.
			decorated, err := decorateChat(frame)
			if err != nil {
				log.Printf("dropping malformed chat frame: %v", err)
				continue
			}

			// Send the frame on
			select {
			case hub.Broadcast <- decorated:
			case <-hub.Done:
				return
			}
		default:
			// A kind this server does not handle is not a reason to hang up:
			// a newer client may send frames this build predates.
			log.Printf("ignoring unsupported frame kind %q from %s", frame.Kind, c.Name)
		}
	}
}

func (c *Client) leave(hub *Hub) {
	select {
	case hub.Unregister <- c:
	case <-hub.Done:
	}
}

func decorateChat(f protocol.Frame) (protocol.Frame, error) {
	var chat protocol.Chat
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		return protocol.Frame{}, err
	}
	return protocol.NewChatFrame(chat.From, fmt.Sprintf(messageFormat, chat.From, chat.Text))
}
