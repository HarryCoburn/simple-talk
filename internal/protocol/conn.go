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
	mu   sync.Mutex
	enc  *json.Encoder
}

func NewConn(c net.Conn) *Conn {
	writer := bufio.NewWriter(c)
	return &Conn{
		conn: c,
		r:    bufio.NewReader(c),
		mu:   sync.Mutex{},
		enc:  json.NewEncoder(writer),
	}
}

func (c *Conn) SendFrame(f Frame) error {
	switch f.Kind {
	case KindHandshake:
		// First, validate the string and cut off any newlines

		marshalledJson, err := json.Marshal(f)
		if err != nil {
			log.Fatalf("Failed to marshal JSON in Send")
		}
		c.mu.Lock()
		c.w.Write(marshalledJson)
		err = c.w.Flush()
		if err != nil {
			log.Fatalf("Failure to flush on send.")
		}
		c.mu.Unlock()
		return nil
	}
	return nil // For now. Return an error if the kind is wrong.
}

func (c *Conn) SendHandshake(name string) error {
	hs := Handshake{
		Name: name,
	}
	hsEnc, err := json.Marshal(hs)
	if err != nil {
		return err
	}

	err = c.SendFrame(Frame{
		Kind:    KindHandshake,
		Payload: hsEnc,
	})

	if err != nil {
		return err
	}
	return nil
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
