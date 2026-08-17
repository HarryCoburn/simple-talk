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
			switch {
			case errors.Is(err, io.EOF):
				fmt.Println("Connection closed cleanly")
			case errors.Is(err, protocol.ErrBadFrame), errors.Is(err, protocol.ErrFrameTooLarge):
				// The protocol layer resynchronized to the next newline, so the
				// stream is still usable. Tell the client and keep reading.
				fmt.Printf("discarding unusable frame from %s: %v\n", c.Name, err)
				if sendErr := c.Conn.SendError(err.Error()); sendErr != nil {
					fmt.Printf("could not report frame error to %s: %v\n", c.Name, sendErr)
					c.leave(hub)
					return
				}
				continue
			default:
				fmt.Printf("connection broke, not EOF: %v\n", err)
			}
			c.leave(hub)
			return
		}

		if frame.Kind != protocol.KindChat {
			fmt.Printf("Didn't receive a chat. Problem!")
			c.leave(hub)
			return
		}

		var chat protocol.Chat
		if err := json.Unmarshal(frame.Payload, &chat); err != nil {
			fmt.Printf("Discarding unreadable chat payload: %v\n", err)
			continue
		}
		// And now we decorate here instead. We are assuming clients can only send protocol.KindChat
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
