package protocol

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"sync"
)

type Conn struct {
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex // For guarding write operations.
	enc  *json.Encoder
}

func NewConn(c net.Conn) *Conn {
	return &Conn{
		conn: c,
		r:    bufio.NewReader(c),
		mu:   sync.Mutex{},
		enc:  json.NewEncoder(c),
	}
}

func (c *Conn) SendFrame(f Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enc.Encode(f) // If this starts failing, close the client.
}

func (c *Conn) SendHandshake(name string) error {
	hs := Handshake{
		Name: name,
	}
	hsEnc, err := json.Marshal(hs)
	if err != nil {
		return err
	}

	return c.SendFrame(Frame{
		Kind:    KindHandshake,
		Payload: hsEnc,
	})

}

func (c *Conn) Recv() (Frame, error) {
	jsonPayload, err := c.r.ReadBytes('\n')
	if err != nil {
		log.Fatalf("Something wrong with the payload in Recv: %v", err)
	}
	var result Frame
	err = json.Unmarshal(jsonPayload, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}
