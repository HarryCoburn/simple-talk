package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

const userNamePrompt string = "Please state your username: "

// main connects to the chat server, sends a handshake through setUserName, then runs a receive loop
// and send loop.
func Run() error {
	bareConn, err := net.Dial("tcp", "localhost:2069") // TODO, change to ask for a connection string.
	if err != nil {
		return fmt.Errorf("client could not dial to the server: %v", err)
	}
	conn := protocol.NewConn(bareConn)
	defer conn.Close() // This will also close bareConn

	stdin := bufio.NewScanner(os.Stdin)

	// Handshake
	name, err := sendHandshake(conn, stdin, protocol.ProtocolVersion)
	if err != nil {
		return fmt.Errorf("problem with setting username: %v", err)

	}

	dead := make(chan struct{})
	go receiveLoop(conn, dead)
	sendLoop(conn, name, stdin, dead)
	return nil
}

// receiveLoop listens to a protocol.Conn for frames. If they are a KindChat or a KindSystem,
// it displays the message. TODO: intercept additional frame types.
func receiveLoop(conn *protocol.Conn, dead chan struct{}) {
	defer func() { fmt.Println("You have been disconnected."); close(dead) }()
	for {
		f, err := conn.Recv()
		if err != nil {
			fmt.Printf("\nDisconnected: %v\n", err)
			return
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
	<-dead
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
