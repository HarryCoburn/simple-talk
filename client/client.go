package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

const userNamePrompt string = "Please state your username: "

// DefaultAddr is the server the client dials when none is given.
const DefaultAddr = "localhost:2069"

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
	receiveLoop(conn, dead)
	return nil
}

// receiveLoop listens to a protocol.Conn for frames. If they are a KindChat or a KindSystem,
// it displays the message. TODO: intercept additional frame types.
func receiveLoop(conn *protocol.Conn, dead chan struct{}) {
	defer func() { fmt.Println("You have been disconnected."); close(dead) }()
	for {
		f, err := conn.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Printf("Quitting client...\n")
				return
			}
			fmt.Printf("\nDisconnected: %v\n", err)
			os.Exit(1)
		}
		switch f.Kind {
		case protocol.KindChat:
			var msg protocol.Chat
			if err := json.Unmarshal(f.Payload, &msg); err != nil {
				continue
			}
			fmt.Println(msg.Text)
		case protocol.KindSystem:
			var msg protocol.System
			if err := json.Unmarshal(f.Payload, &msg); err != nil {
				continue
			}
			fmt.Println(msg.Text)
		case protocol.KindError:
			var msg protocol.Error
			if err := json.Unmarshal(f.Payload, &msg); err != nil {
				continue
			}
			fmt.Printf("Error: %s\n", msg.Message)
		default:
			log.Print("client received frame kind it can't process yet.")
		}
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
