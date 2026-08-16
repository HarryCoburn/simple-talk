package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

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

const userNamePrompt = "Please state your username: \n"

func (c *Client) processClientOutQueue() {
	// Listens to the client's Out queue, then writes any message received to Writer for display.
	defer c.Connection.Close()
	for output := range c.Out {
		c.Writer.Write(output.Data)
		err := c.Writer.Flush() // Writes the output to the console.
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func (c *Client) readClientInput(hub *Hub) {
	for {
		// Reads what the client writes. Closes client safely if there is a problem with reading.
		line, err := c.Reader.ReadString('\n')
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
		// Take what client wrote and place it into a Message struct for sending
		select {
		case hub.Broadcast <- Message{From: c, Data: []byte(line)}:
		case <-hub.Done:
			return
		}
	}
}

func (c *Client) setUserName() error {
	c.Writer.Write([]byte(userNamePrompt))
	err := c.Writer.Flush()
	if err != nil {
		fmt.Println(err)
		return err
	}
	c.Connection.SetReadDeadline(time.Now().Add(30 * time.Second))
	name, err := c.Reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			fmt.Println("Connection closed cleanly")
			return err
		} else {
			fmt.Printf("connection broke, not EOF: %v", err)
			return err
		}
	}
	c.Connection.SetReadDeadline(time.Time{})
	// Add name validation here.
	c.Name = strings.TrimSuffix(name, "\n")
	return nil
}
