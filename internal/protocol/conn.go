package protocol

import (
	"bufio"
	"net"
	"sync"
)

type Conn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	mu   sync.Mutex
}

func NewConn(c net.Conn) *Conn {
	return &Conn{
		conn: c,
		r:    bufio.NewReader(c),
		w:    bufio.NewWriter(c),
		mu:   sync.Mutex{},
	}
}
