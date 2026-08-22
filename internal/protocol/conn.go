package protocol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	MaxFrameSize     = 8192
	maxDiscardSize   = MaxFrameSize * 2
	HandshakeTimeout = (time.Second * 30)
)

var (
	ErrBadFrame      = errors.New("protocol: received a bad frame")
	ErrFrameTooLarge = errors.New("protocol: frame exceeds maximum size")
	ErrUnrecoverable = errors.New("protocol: stream desynchronized, connection unsuable")
)

// Conn is the connection structure for the chat protocol
type Conn struct {
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex // For guarding write operations.
	enc  *json.Encoder
}

func NewConn(c net.Conn) *Conn {
	return &Conn{
		conn: c,
		r:    bufio.NewReaderSize(c, MaxFrameSize),
		mu:   sync.Mutex{},
		enc:  json.NewEncoder(c),
	}
}

// Senders

// SendFrame writes the frame into the pipe
func (c *Conn) SendFrame(f Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enc.Encode(f) // If this starts failing, close the client.
}

func (c *Conn) SendHandshake(name string) error {
	f, err := NewHandshakeFrame(name)
	if err != nil {
		return err
	}
	return c.SendFrame(f)

}

func (c *Conn) SendHandshakeAck(name string) error {
	f, err := NewHandshakeAckFrame(name)
	if err != nil {
		return err
	}
	return c.SendFrame(f)
}

func (c *Conn) SendChat(sender string, msg string) error {
	f, err := NewChatFrame(sender, msg)
	if err != nil {
		return err
	}
	return c.SendFrame(f)
}

func (c *Conn) SendError(e string) error {
	f, err := NewErrorFrame(e)
	if err != nil {
		return err
	}
	return c.SendFrame(f)
}

// Receiving

// Returns one newline-deliminted frame, or one of the special errors.
func (c *Conn) readLine() ([]byte, error) {
	line, err := c.r.ReadSlice('\n')
	if err == nil {
		return line, nil
	}

	// Is the buffer full?
	if !errors.Is(err, bufio.ErrBufferFull) {
		return nil, err
	}

	// The buffer is full. Try to recover.

	// Find out how much we have
	discarded := len(line)
	for {
		// Is it beyond our max size of discarded data?
		if discarded > maxDiscardSize {
			return nil, ErrUnrecoverable
		}
		// No, read another slice and add its length.
		chunk, err := c.r.ReadSlice('\n')
		discarded += len(chunk)

		// What did that read return?
		switch {
		case err == nil:
			return nil, ErrFrameTooLarge
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		default:
			return nil, err
		}
	}

}

func (c *Conn) Recv() (Frame, error) {
	line, err := c.readLine()
	if err != nil {
		return Frame{}, err
	}

	line = bytes.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return Frame{}, fmt.Errorf("%w: empty line", ErrBadFrame)
	}

	var f Frame
	if err := json.Unmarshal(line, &f); err != nil {
		return Frame{}, fmt.Errorf("%w: %v", ErrBadFrame, err)
	}
	if f.Kind == "" {
		return Frame{}, fmt.Errorf("%w: missing kind", ErrBadFrame)
	}

	// Unmarshal may retain a subslice of line, which readLine's buffer will
	// overwrite on the next call. Copy so the frame outlives this call.
	if f.Payload != nil {
		f.Payload = append(json.RawMessage(nil), f.Payload...)
	}
	return f, nil
}

// Utilities

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Conn) Close() error {
	return c.conn.Close()
}
