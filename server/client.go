package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// An individual client connection.
type Client struct {
	Connection net.Conn      // The TCP connection of the Client
	Name       string        // The username of the Client
	Writer     *bufio.Writer // To write to other TCP connections
	Reader     *bufio.Reader // To receive from other TCP connections
	Out        chan Message  // The message channel queue
}

func newClient(conn net.Conn) *Client {
	return &Client{
		Connection: conn,
		Name:       "Default",
		Writer:     bufio.NewWriter(conn),
		Reader:     bufio.NewReader(conn),
		Out:        make(chan Message, 256),
	}
}

const userNamePrompt = "Please state your username: \n"

func (c *Client) proceessClientOutQueue() {
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
