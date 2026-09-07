package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

const (
	userNamePrompt string     = "Please state your username: "
	DefaultAddr    string     = "localhost:2069"
	ChatFrameErr   string     = "Chat frame error: %w"
	SystemFrameErr string     = "System frame error: %w"
	ErrorFrameErr  string     = "Error frame error: %w"
	MsgFormat      string     = "<%s> %s\n"
	ErrFormat      string     = "Error: %s\n"
	SystemFormat   string     = "%s\n"
	intentNothing  intentKind = 0
	intentChat     intentKind = 1
	intentCommand  intentKind = 2
)

type intentKind int

type inputIntent struct {
	Intent intentKind
	Text   string
	Cmd    string
	Args   []string
}

// Run connects to the server at addr, negotiates a username, then runs a
// receive loop and a send loop.
//
// addr is host:port in net.Dial's form, so an IPv6 literal needs brackets:
// "[::1]:2069". The name is the whole point of taking it as a parameter --
// until it was one, nothing could reach a server on another machine, and
// nothing could test this dial.
func Run(addr string) error {
	bareConn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("client could not dial %s: %w", addr, err)
	}
	conn := protocol.NewConn(bareConn)
	defer conn.Close() // This will also close bareConn

	stdin := bufio.NewScanner(os.Stdin)

	// Handshake
	name, err := negotiateName(conn, stdin, protocol.ProtocolVersion)
	if err != nil {
		return fmt.Errorf("problem with setting username: %v", err)

	}

	dead := make(chan struct{})
	go sendLoop(conn, name, stdin, dead)
	receiveLoop(os.Stdout, conn, dead)
	return nil
}

// receiveLoop listens to a protocol.Conn for frames and passes them to formatFrame.
func receiveLoop(w io.Writer, conn *protocol.Conn, dead chan struct{}) {
	defer func() { fmt.Fprintln(w, "You have been disconnected."); close(dead) }()
	for {
		f, err := conn.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Fprintf(w, "Quitting client...\n")
				return
			}
			fmt.Fprintf(w, "\nDisconnected: %v\n", err)
			return
		}
		msg, err := formatFrame(f)
		if err != nil {
			// faulty frame, decide how to handle the error logging later.
			continue
		}
		fmt.Fprint(w, msg)

	}

}

// formatFrame formats frames received from the server to client-specified strings
func formatFrame(f protocol.Frame) (string, error) {
	switch f.Kind {
	case protocol.KindChat:
		var msg protocol.Chat
		if err := json.Unmarshal(f.Payload, &msg); err != nil {
			return "", fmt.Errorf(ChatFrameErr, err)
		}
		return fmt.Sprintf(MsgFormat, msg.From, msg.Text), nil
	case protocol.KindSystem:
		var msg protocol.System
		if err := json.Unmarshal(f.Payload, &msg); err != nil {
			return "", fmt.Errorf(SystemFrameErr, err)
		}
		return fmt.Sprintf(SystemFormat, msg.Text), nil
	case protocol.KindError:
		var msg protocol.Error
		if err := json.Unmarshal(f.Payload, &msg); err != nil {
			return "", fmt.Errorf(ErrorFrameErr, err)
		}
		return fmt.Sprintf(ErrFormat, msg.Message), nil
	default:
		return "", nil
	}
}

// sendLoop sends information from stdin to a protocol.Conn for processing. This will be replaced
// once we begin BubbleTea usage.
func sendLoop(conn *protocol.Conn, name string, scan *bufio.Scanner, dead chan struct{}) {
	for scan.Scan() {
		select {
		case <-dead:
			return
		default:
		}
		line := scan.Text()
		var err error
		if strings.TrimSpace(line) == "" {
			continue
		}
		if cmd, args, ok := parseInput(line); ok {
			err = conn.SendCommand(cmd, args)
		} else {
			err = conn.SendChat(name, unescapeInput(line))
		}
		if err != nil {
			fmt.Printf("Send failed: %v\n", err)
			return
		}
	}
	conn.Close()
}

func classify(line string) inputIntent {
	if strings.TrimSpace(line) == "" {
		return inputIntent{Intent: intentNothing, Text: "", Cmd: "", Args: nil}
	}
	if cmd, args, ok := parseInput(line); ok {
		return inputIntent{Intent: intentCommand, Text: "", Cmd: cmd, Args: args}
	} else {
		return inputIntent{Intent: intentChat, Text: unescapeInput(line), Cmd: "", Args: nil}
	}
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
