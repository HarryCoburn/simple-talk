package server

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// commandHub is the slice of the hub a command handler may touch: reply to the
// caller, speak to the room, read the roster. Nothing else. Handlers hold this
// rather than the *Hub so they cannot reach query, exec or sessions.
//
// Every method here is a round trip through the hub goroutine, so a handler must
// run on the caller's own goroutine and never inside Hub.run. dispatchCommand is
// called from readInput, which is what makes that true today.
type commandHub interface {
	sendTo(c *Session, f protocol.Frame) error
	sessionNames() ([]string, error)
	broadcastFrame(f protocol.Frame) error
}

var _ commandHub = (*Hub)(nil)

// cmdEnv is everything a command handler is allowed to touch.
//
// Receivers and parameters of this type are named cc, not ctx: commands is
// where a real context.Context will want to be threaded, and the two reading
// alike in the same function is the confusion worth avoiding up front.
type cmdEnv struct {
	session *Session   // Who ran the command.
	hub     commandHub // For queries and for replying.
	args    []string   // Everything after the command word.
	roster  []string   // Sorted command names. For /help command to avoid circular dependency
}

// A cmdHandler runs one command. Replies to the caller go through cc.reply;
// anything the whole room should see goes to hub.broadcastFrame.
type cmdHandler func(cc cmdEnv) error

// The command table. Names are lowercase and carry no leading slash — the
// client strips that before building the frame.
var commands = map[string]cmdHandler{
	"who":  cmdWho,
	"help": cmdHelp,
	"quit": cmdQuit,
}

// commandNames is used by /help to send a list of all of the names of the commands.
// This is a read-only variable.
var commandNames = sync.OnceValue(func() []string {
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	slices.Sort(names) // map order is random; /help must be stable
	return names
})

// reply sends a system message to the caller alone.
func (cc cmdEnv) reply(format string, a ...any) error {
	f, err := protocol.NewSystemFrame(fmt.Sprintf(format, a...))
	if err != nil {
		return err
	}
	return cc.hub.sendTo(cc.session, f)
}

// replyError sends an error frame to the caller alone.
func (cc cmdEnv) replyError(format string, a ...any) error {
	f, err := protocol.NewErrorFrame(fmt.Sprintf(format, a...))
	if err != nil {
		return err
	}
	return cc.hub.sendTo(cc.session, f)
}

// dispatchCommand decodes a command frame and runs the matching handler.
// An unknown command is reported to the caller and is not an error here.
//
// This runs on the caller's read goroutine. Every cmdCtx method blocks until the
// hub goroutine services it, so a handler must never be invoked from inside
// Hub.run.
func dispatchCommand(c *Session, hub commandHub, f protocol.Frame) error {
	var cmd protocol.Command
	if err := json.Unmarshal(f.Payload, &cmd); err != nil {
		return fmt.Errorf("dispatch command: %w", err)
	}

	name := strings.ToLower(strings.TrimSpace(cmd.Name))
	cc := cmdEnv{session: c, hub: hub, args: cmd.Args, roster: commandNames()}

	if name == "" {
		return cc.replyError("no command given")
	}

	handler, ok := commands[name]
	if !ok {
		return cc.replyError("unknown command %q. Try help for a list.", name)
	}
	return handler(cc)
}

// cmdWho lists everyone currently connected.
//
// It costs two round trips through the hub goroutine: sessionNames reads the
// roster, then reply queues the answer. Both block on the same loop, so a user
// spamming /who is two units of work each time, not one -- and the sort happens
// inside the hub goroutine too.
func cmdWho(cc cmdEnv) error {
	names, err := cc.hub.sessionNames()
	if err != nil {
		return err
	}
	if len(names) == 0 {
		// The caller is registered, so an empty roster means they were dropped between
		// issuing the command and the hub reading the list
		return cc.reply("Nobody is connected.")
	}
	return cc.reply("Connected (%d): %s", len(names), strings.Join(names, ", "))
}

// cmdHelp lists the available commands.
func cmdHelp(cc cmdEnv) error {

	return cc.reply("Commands: %s", strings.Join(cc.roster, ", "))
}

func cmdQuit(cc cmdEnv) error {
	return cc.session.Conn.Close()
}
