package server

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

// cmdCtx is everything a command handler is allowed to touch.
type cmdCtx struct {
	client *Client  // Who ran the command.
	hub    *Hub     // For queries and for replying.
	args   []string // Everything after the command word.
}

// A cmdHandler runs one command. Replies to the caller go through ctx.reply;
// anything the whole room should see goes to hub.Broadcast.
type cmdHandler func(ctx cmdCtx) error

// The command table. Names are lowercase and carry no leading slash — the
// client strips that before building the frame. Populated in init() rather than
// as a literal because cmdHelp reads the table, which would be an
// initialization cycle.
var commands = map[string]cmdHandler{
	"who":  cmdWho,
	"help": cmdHelp,
}

var commandNames = []string{
	"help",
	"who",
}

// reply sends a system message to the caller alone.
func (ctx cmdCtx) reply(format string, a ...any) error {
	f, err := protocol.NewSystemFrame(fmt.Sprintf(format, a...))
	if err != nil {
		return err
	}
	return ctx.hub.sendTo(ctx.client, f)
}

// replyError sends an error frame to the caller alone.
func (ctx cmdCtx) replyError(format string, a ...any) error {
	f, err := protocol.NewErrorFrame(fmt.Sprintf(format, a...))
	if err != nil {
		return err
	}
	return ctx.hub.sendTo(ctx.client, f)
}

// dispatchCommand decodes a command frame and runs the matching handler.
// An unknown command is reported to the caller and is not an error here.
func dispatchCommand(c *Client, hub *Hub, f protocol.Frame) error {
	var cmd protocol.Command
	if err := json.Unmarshal(f.Payload, &cmd); err != nil {
		return fmt.Errorf("dispatch command: %w", err)
	}

	name := strings.ToLower(strings.TrimSpace(cmd.Name))
	ctx := cmdCtx{client: c, hub: hub, args: cmd.Args}

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
	names, err := ctx.hub.clientNames()
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

	return ctx.reply("Commands: %s", strings.Join(commandNames, ", "))
}
