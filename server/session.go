package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime/debug"
	"strings"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
	"github.com/HarryCoburn/simple-talk/internal/validate"
)

// Session is one connected user: their socket, the name they chose, and the
// queue of frames waiting to go out to them. The client program at the other
// end is not modelled here — anything that speaks the protocol gets a Session.
type Session struct {
	Conn *protocol.Conn // The protocol connection
	Name string         // The username of the Session
	// Out is accessed and closed only from the hub goroutine
	// Sends and closes must go through deliverTo/closeSession, called via exec.
	// Otherwise, there is a risk of a send-on-closed-channel panic.
	Out chan protocol.Frame // The message channel queue
	key string              // The folded name this session is registered under. Set by the hub at registration.
}

func newSession(conn *protocol.Conn, name string) *Session {
	return &Session{
		Conn: conn,
		Name: name,
		Out:  make(chan protocol.Frame, 256),
	}
}

// guard turns a panic on one session's goroutine into that one session leaving.
// Without it a bug anywhere in a session — a command handler, a malformed frame,
// a nil dereference — unwinds past main and takes every other user's session
// with it. The session is dropped rather than resumed: whatever invariant the
// panic broke, this connection is no longer trustworthy.
//
// Deferred at the top of each of a session's goroutines. recover only sees panics
// from the goroutine it is deferred in, so every goroutine needs its own.
func (c *Session) guard(hub *Hub, loop string) {
	r := recover()
	if r == nil {
		return
	}
	log.Printf("panic in the %s for %s: %v\n%s", loop, c.Name, r, debug.Stack())
	c.leave(hub)
}

func (c *Session) writeLoop(hub *Hub) {
	defer c.guard(hub, "write loop")
	// Listens to the session's Out queue, then writes any message received to Writer for display.
	// The socket belongs to hub.closeSession, and that closes c.Out, so this loop does not need
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

func (c *Session) readLoop(hub *Hub) {
	defer c.guard(hub, "read loop")
	for {
		// Reads what the user sends. Closes the session safely if there is a problem with reading.
		frame, err := c.Conn.Recv()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				log.Print("Connection closed cleanly")
			case errors.Is(err, protocol.ErrBadFrame), errors.Is(err, protocol.ErrFrameTooLarge):
				log.Printf("discarding unusable frame from %s: %v", c.Name, err)
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
			if err := c.checkChat(frame); err != nil {
				if sendErr := c.Conn.SendError(err.Error()); sendErr != nil {
					log.Printf("could not report a rejected message to %s: %v", c.Name, sendErr)
					c.leave(hub)
					return
				}
				continue
			}
			decorated, err := decorateChat(c.Name, frame)
			if err != nil {
				log.Printf("dropping malformed chat frame: %v", err)
				continue
			}
			if err := hub.broadcastFrame(decorated); err != nil {
				if errors.Is(err, ErrHubClosed) {
					return
				}
				log.Printf("broadcast from %s failed: %v", c.Name, err)
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

func (c *Session) leave(hub *Hub) {
	hub.unregisterSession(c)
}

// checkChat applies the message rules to an incoming chat frame. A malformed
// payload is reported the same way as a disallowed one: the sender hears about
// it and the room never sees it.
func (c *Session) checkChat(f protocol.Frame) error {
	var chat protocol.Chat
	if err := json.Unmarshal(f.Payload, &chat); err != nil {
		return validate.ErrMessageNotAllowed
	}
	_, err := validate.Message(chat.Text)
	return err
}

const messageFormat = "<%s> %s"
const poseFormat = "%s %s"

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
