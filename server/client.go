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
	Connection net.Conn
	Name       string
	Writer     *bufio.Writer
	Reader     *bufio.Reader
	Out        chan Message
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

func (c *Client) writeLoop() {
	defer c.Connection.Close()
	for output := range c.Out {
		_, err := c.Writer.Write(output.Data)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = c.Writer.Flush()
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

// Reads what the client sends, and closes clients when they are gone.
func (c *Client) readLoop(hub *Hub) {
	for {
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
		select {
		case hub.Broadcast <- Message{From: c, Data: []byte(line)}:
		case <-hub.Done:
			return
		}
	}
}

func (c *Client) setUserName() {
	_, err := c.Writer.Write([]byte("Please state your username: \n"))
	if err != nil {
		fmt.Println(err)
		return
	}
	err = c.Writer.Flush()
	if err != nil {
		fmt.Println(err)
		return
	}
	c.Connection.SetReadDeadline(time.Now().Add(30 * time.Second))
	name, err := c.Reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			fmt.Println("Connection closed cleanly")
		} else {
			fmt.Printf("connection broke, not EOF: %v", err)
		}
	}
	c.Connection.SetReadDeadline(time.Time{})
	// Add name validation here.
	c.Name = strings.TrimSuffix(name, "\n")
}
