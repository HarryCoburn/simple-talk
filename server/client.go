package main

import (
	"bufio"
	"net"
)

// An individual client connection.
type Client struct {
	Connection net.Conn
	Reader     *bufio.Reader
	Out        chan []byte
}

func newClient(conn net.Conn) *Client {
	return &Client{
		Connection: conn,
		Reader:     bufio.NewReader(conn),
		Out:        make(chan []byte, 256),
	}
}

func (c *Client) writeLoop() {
	for {
		output := <-c.Out
		c.Connection.Write(output)
	}
}
