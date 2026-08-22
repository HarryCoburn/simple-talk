package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

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

func (c *Client) processClientOutQueue(hub *Hub) {
	// Listens to the client's Out queue, then writes any message received to Writer for display.
	// The socket belongs to hub.closeClient, and that closes c.Out, so this loop does not need
	// to close.
	for frame := range c.Out {
		// Out should hold a completed frame
		if err := c.Conn.SendFrame(frame); err != nil {
			log.Printf("write to %s failed: %v", c.Name, err)
			// Tell the hub to close since Out won't drain after this loop stops.
			c.leave(hub)
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
				log.Print("Connection closed cleanly")
			case errors.Is(err, protocol.ErrBadFrame), errors.Is(err, protocol.ErrFrameTooLarge):
				log.Printf("discarding unsuable frame from %s: %v", c.Name, err)
				if sendErr := c.Conn.SendError(err.Error()); sendErr != nil {
					log.Printf("could not report frame error to %s: %v", c.Name, sendErr)
					c.leave(hub)
					return
				}
				continue
			default:
				log.Printf("connection broke, not EOF: %v", err)
			}
			c.leave(hub)
			return
		}

		switch frame.Kind {
		case protocol.KindChat:
			decorated, err := decorateChat(c.Name, frame)
			if err != nil {
				log.Printf("dropping malformed chat frame: %v", err)
				continue
			}
			select {
			case hub.Broadcast <- decorated:
			case <-hub.Done:
				return
			}
		case protocol.KindCommand:
			if err := dispatchCommand(c, hub, frame); err != nil {
				log.Printf("command from %s failed: %v", c.Name, err)
			}
		default:
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

// decorateChat formats an incoming chat payload for broadcast. The sender is
// taken from the registered connection, not from the payload, so a client
// cannot claim to be someone else.
func decorateChat(sender string, f protocol.Frame) (protocol.Frame, error) {
	var chat protocol.Chat
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		return protocol.Frame{}, fmt.Errorf("decorate chat: %w", err)
	}

	format := messageFormat
	text := chat.Text
	if strings.HasPrefix(chat.Text, ":") {
		format = poseFormat
		text = chat.Text[1:]
	}

	frame, err := protocol.NewChatFrame(sender, fmt.Sprintf(format, sender, text))
	if err != nil {
		return protocol.Frame{}, err
	}
	return frame, nil

}
