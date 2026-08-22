# Next time
- ~~Get disconnect message to announce on unclean disconnects to all other parties.~~
- ~~Use log instead of fmt where needed.~~
- ~~Experiment with slash commands or other MUD style commands from the client side.~~ Client parses the slash and sends a `command` frame; the server dispatches through the table in server/commands.go. `/who` and `/help` exist.
- Thoroughly understand the error paths and why they're in there.
- TLS

# Later
- BubbleTea conversion
- Sending more than text?
- MUD-type rooms?
