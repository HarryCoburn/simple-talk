package client

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

type intentKind int

const (
	intentNothing intentKind = iota
	intentChat
	intentCommand
)

type inputIntent struct {
	Intent intentKind
	Text   string
	Cmd    string
	Args   []string
}

// sendLoop sends information from stdin to a protocol.Conn for processing. This will be replaced
// once we begin BubbleTea usage.
func sendLoop(w io.Writer, conn *protocol.Conn, name string, scan *bufio.Scanner, dead chan struct{}) {
	for scan.Scan() {
		select {
		case <-dead:
			return
		default:
		}
		intent := classify(scan.Text())
		var err error
		switch intent.Intent {
		case intentNothing:
			continue
		case intentCommand:
			err = conn.SendCommand(intent.Cmd, intent.Args)
		case intentChat:
			err = conn.SendChat(name, intent.Text)
		default:
			// Consider a log here
			continue
		}
		if err != nil {
			fmt.Fprintf(w, "Send failed: %v\n", err)
			return
		}
	}
}

func classify(line string) inputIntent {
	if strings.TrimSpace(line) == "" {
		return inputIntent{Intent: intentNothing, Text: "", Cmd: "", Args: nil}
	}
	if cmd, args, ok := parseInput(line); ok {
		return inputIntent{Intent: intentCommand, Text: "", Cmd: cmd, Args: args}
	}
	return inputIntent{Intent: intentChat, Text: unescapeInput(line), Cmd: "", Args: nil}
}

func parseInput(line string) (name string, args []string, ok bool) {
	if !strings.HasPrefix(line, "/") || strings.HasPrefix(line, "//") {
		return "", nil, false
	}
	fields := strings.Fields(line[1:])
	if len(fields) == 0 {
		return "", nil, false // handles a lone "/" or "/    ".
	}
	return strings.ToLower(fields[0]), fields[1:], true
}

func unescapeInput(line string) string {
	if strings.HasPrefix(line, "//") {
		return line[1:]
	}
	return line
}
