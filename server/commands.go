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

// cmdCtx is everything a command handler is allowed to touch.
type cmdCtx struct {
	session *Session   // Who ran the command.
	hub     commandHub // For queries and for replying.
	args    []string   // Everything after the command word.
	roster  []string   // Sorted command names. For /help command to avoid circular dependency
}

// A cmdHandler runs one command. Replies to the caller go through ctx.reply;
// anything the whole room should see goes to hub.broadcastFrame.
type cmdHandler func(ctx cmdCtx) error

// The command table. Names are lowercase and carry no leading slash — the
// client strips that before building the frame.
var commands = map[string]cmdHandler{
	"who":  cmdWho,
	"help": cmdHelp,
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
func (ctx cmdCtx) reply(format string, a ...any) error {
	f, err := protocol.NewSystemFrame(fmt.Sprintf(format, a...))
	if err != nil {
		return err
	}
	return ctx.hub.sendTo(ctx.session, f)
}

// replyError sends an error frame to the caller alone.
func (ctx cmdCtx) replyError(format string, a ...any) error {
	f, err := protocol.NewErrorFrame(fmt.Sprintf(format, a...))
	if err != nil {
		return err
	}
	return ctx.hub.sendTo(ctx.session, f)
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
	ctx := cmdCtx{session: c, hub: hub, args: cmd.Args, roster: commandNames()}

	if name == "" {
		return ctx.replyError("no command given")
	}

	handler, ok := commands[name]
	if !ok {
		return ctx.replyError("unknown command %q. Try help for a list.", name)
	}
	return handler(ctx)
}

// cmdWho lists everyone currently connected.
func cmdWho(ctx cmdCtx) error {
	names, err := ctx.hub.sessionNames()
	if err != nil {
		return err
	}
	if len(names) == 0 {
		// The caller is registered, so an empty roster means they were dropped between
		// issuing the command and the hub reading the list
		return ctx.reply("Nobody is connected.")
	}
	return ctx.reply("Connected (%d): %s", len(names), strings.Join(names, ", "))
}

// cmdHelp lists the available commands.
func cmdHelp(ctx cmdCtx) error {

	return ctx.reply("Commands: %s", strings.Join(ctx.roster, ", "))
}
