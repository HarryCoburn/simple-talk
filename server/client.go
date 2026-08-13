package main

import (
	"bufio"
	"fmt"
	"net"
)

// An individual client connection.
type Client struct {
	Connection net.Conn
	Writer     *bufio.Writer
	Reader     *bufio.Reader
	Out        chan []byte
}

func newClient(conn net.Conn) *Client {
	return &Client{
		Connection: conn,
		Writer:     bufio.NewWriter(conn),
		Reader:     bufio.NewReader(conn),
		Out:        make(chan []byte, 256),
	}
}

func (c *Client) writeLoop() {
	defer c.Connection.Close()
	for output := range c.Out {
		_, err := c.Writer.Write(output)
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
