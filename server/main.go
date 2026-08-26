package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
	"github.com/HarryCoburn/simple-talk/internal/validate"
)

// Run starts the server and blocks until an interrupt or SIGTERM
func Run() error {
	// Create the raw TCP connection. TODO upgrade to TLS.
	ln, err := net.Listen("tcp", ":2069")
	if err != nil {
		return fmt.Errorf("Could not open server listener: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serve(ln, ctx)
	return nil
}

func serve(ln net.Listener, ctx context.Context) {

	hub := newHub(protocol.ProtocolVersion)

	stopped := make(chan struct{})
	go hub.run()
	go func() {
		<-hub.doneChan()
		ln.Close()
	}()
	go acceptLoop(ctx, hub, ln, stopped)

	select {
	case sig := <-ctx.Done():
		log.Printf("received %v, shutting down", sig)
	case <-stopped:
		// Listener died, tear down the rest
		log.Print("stopped accepting connections, shutting down")
	}

	hub.stop()
	<-stopped
	hub.wait()
}

// Listen for incoming client connections, make a session for each, then start a goroutine to handle it.
func acceptLoop(ctx context.Context, hub *Hub, ln net.Listener, stopped chan struct{}) {
	defer close(stopped)
	for {
		// Listen for incoming connections
		conn, err := ln.Accept()

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("Error accepting a connection: %v\n", err)
			continue
		}

		go buildNewSession(context.Background(), hub, conn)
	}
}

func buildNewSession(ctx context.Context, hub *Hub, conn net.Conn) {
	// A connection is detected. Make the session
	pConn := protocol.NewConn(conn)
	clientName, err := verifyName(pConn, hub.version)
	if err != nil {
		log.Print(err)
		// A version mismatch is the client's to fix, so tell it what happened
		// instead of hanging up on a bare EOF. The client's own version is not
		// echoed back: it is unvalidated input and would print to a terminal.
		if errors.Is(err, ErrVersionMismatch) {
			if sendErr := pConn.SendError(fmt.Sprintf("this server speaks version %s. Update your client.", hub.version)); sendErr != nil {
				log.Printf("could not report the version mismatch: %v", sendErr)
			}
			if errors.Is(err, ErrNameTaken) {
				if sendErr := pConn.SendError("this name is already taken. Please choose another."); sendErr != nil {
					log.Printf("could not report a name was already taken: %v", sendErr)
				}
			}
		}
		// A name the rules reject is the client's to fix too, so it gets the
		// reason rather than a bare EOF. recvHandshake already handles KindError.
		if reason, ok := nameRejection(err); ok {
			if sendErr := pConn.SendError(reason); sendErr != nil {
				log.Printf("could not report an invalid name: %v", sendErr)
			}
		}
		pConn.Close()
		return
	}

	sess := newSession(pConn, clientName)

	if err := hub.registerSession(sess); err != nil {
		if errors.Is(err, ErrNameTaken) {
			if sendErr := pConn.SendError(err.Error()); sendErr != nil {
				log.Printf("could not report taken name to %s: %v", clientName, sendErr)
			}
		}
		pConn.Close()
		return
	}

	// Send handshake ack
	if err := pConn.SendHandshakeAck(clientName); err != nil {
		log.Printf("handshake ack to %s has failed: %v", clientName, err)
		sess.leave(hub)
		return
	}

	// Announce successful connection to others.
	f, err := announceConnection(sess.Name)
	if err != nil {
		log.Printf("could not build connect announcement for %s: %v", sess.Name, err)
		sess.leave(hub)
		return
	}
	if err := hub.broadcastFrame(f); err != nil {
		log.Printf("could not announce %s: %v", sess.Name, err)
		sess.leave(hub)
		return
	}

	go sess.writeLoop(hub)
	go sess.readInput(hub)
}

func verifyName(fullc *protocol.Conn, version string) (string, error) {
	// Get the frame for name entry.
	if err := fullc.SetReadDeadline(time.Now().Add(protocol.HandshakeTimeout)); err != nil {
		return "", fmt.Errorf("Something went wrong setting a read deadline: %v", err)
	}
	f, err := fullc.Recv()
	if err != nil {
		return "", fmt.Errorf("problem with handshake frame: %w", err)
	}

	if err := fullc.SetReadDeadline(time.Time{}); err != nil {
		return "", fmt.Errorf("Something went wrong with releasing a read deadline: %v", err)
	}

	// Is it the right kind?
	if f.Kind != protocol.KindHandshake {
		return "", fmt.Errorf("wrong frame type sent in handshake received kind: %v", f.Kind)
	}

	var hs protocol.Handshake
	if err := json.Unmarshal(f.Payload, &hs); err != nil {
		return "", fmt.Errorf("something wrong with the name payload: %w", err)
	}

	// The version is settled before the name: there is no point validating a
	// name for a client this server cannot talk to.
	if hs.Version != version {
		return "", fmt.Errorf("%w: client sent %q, server speaks %q", ErrVersionMismatch, hs.Version, version)
	}

	// The client validates too, for a faster prompt, but a hand-written client
	// is under no obligation to. This is where the rule is enforced.
	name, err := validate.Name(hs.Name)
	if err != nil {
		return "", err
	}
	return name, nil
}

// nameRejection reports whether err is a name that failed validate.Name, and
// returns the reason to send back.
//
// The reasons are matched against a fixed list rather than passing err.Error()
// through: every message here is a constant that never embeds the name, so a
// hostile name cannot get itself echoed to a terminal. verifyName's other
// errors do quote their input, which is exactly why they are not sent.
func nameRejection(err error) (string, bool) {
	for _, reason := range []error{
		validate.ErrNameEmpty,
		validate.ErrNameTooLong,
		validate.ErrNameNotAllowed,
	} {
		if errors.Is(err, reason) {
			return reason.Error(), true
		}
	}
	return "", false
}

func announceConnection(name string) (protocol.Frame, error) {
	msg := fmt.Sprintf("%s has connected.", name)
	f, err := protocol.NewSystemFrame(msg)
	if err != nil {
		return protocol.Frame{}, err
	}
	return f, nil
}

func announceDisconnection(name string) (protocol.Frame, error) {
	msg := fmt.Sprintf("%s has disconnected.", name)
	f, err := protocol.NewSystemFrame(msg)
	if err != nil {
		return protocol.Frame{}, err
	}
	return f, nil
}
