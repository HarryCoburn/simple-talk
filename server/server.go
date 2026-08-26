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

// notifyFunc is signal.NotifyContext's shape. Run takes it as a parameter so a
// test can supply a fake and assert which signals were asked for, without the
// test binary having to receive any.
type notifyFunc func(context.Context, ...os.Signal) (context.Context, context.CancelFunc)

// Run starts the server and blocks until an interrupt or SIGTERM
func Run() error {
	return run(context.Background(), ":2069", signal.NotifyContext)
}

// run opens the listener and hands it to runOn. Split from Run so a test can
// name its own address rather than gambling on a fixed port being free.
func run(ctx context.Context, addr string, notify notifyFunc) error {
	// Create the raw TCP connection. TODO upgrade to TLS.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("could not open server listener: %w", err)
	}
	return runOn(ctx, ln, notify)
}

// runOn is where the signals are wired to a context. Split from run so a test
// can supply a listener it already holds the address of.
func runOn(ctx context.Context, ln net.Listener, notify notifyFunc) error {
	ctx, stop := notify(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	serve(ctx, ln)
	return nil
}

func serve(ctx context.Context, ln net.Listener) {

	hub := newHub(ctx, protocol.ProtocolVersion)

	stopped := make(chan struct{})
	go hub.run()
	go func() {
		<-hub.doneChan()
		ln.Close()
	}()
	go acceptLoop(ctx, hub, ln, stopped)

	select {
	case <-ctx.Done():
		log.Print("shutdown requested, shutting down")
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

		// A connection accepted after teardown started has nowhere to go: the
		// hub is closing sessions, not registering them. The listener close
		// unblocks Accept during shutdown, so this is belt and braces -- and it
		// is what makes ctx load-bearing here rather than decorative.
		select {
		case <-ctx.Done():
			conn.Close()
			return
		default:
		}

		go buildNewSession(ctx, hub, conn)
	}
}

func buildNewSession(ctx context.Context, hub *Hub, conn net.Conn) {
	// A connection is detected. Make the session
	pConn := protocol.NewConn(conn)

	// A shutdown has to reach a handshake parked in Recv. Closing the socket is
	// the only thing that unblocks it: acceptHandshake's read deadline is thirty
	// seconds out and Recv has no context of its own. The window ends at
	// registration, because from there the socket belongs to hub.closeSession
	// and a second closer would only muddy that ownership.
	hsCtx, endHandshake := context.WithCancel(ctx)
	defer endHandshake()
	stopClose := context.AfterFunc(hsCtx, func() { pConn.Close() })
	defer stopClose()

	clientName, err := acceptHandshake(pConn, hub.version)
	if err != nil {
		log.Print(err)
		// A version mismatch is the client's to fix, so tell it what happened
		// instead of hanging up on a bare EOF. The client's own version is not
		// echoed back: it is unvalidated input and would print to a terminal.
		if errors.Is(err, ErrVersionMismatch) {
			if sendErr := pConn.SendError(fmt.Sprintf("this server speaks version %s. Update your client.", hub.version)); sendErr != nil {
				log.Printf("could not report the version mismatch: %v", sendErr)
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

	// Registered: the socket is hub.closeSession's from here on. Disarm
	// explicitly rather than leaning on the defers, which fire far too late.
	stopClose()
	endHandshake()

	// Send handshake ack
	if err := pConn.SendHandshakeAck(clientName); err != nil {
		log.Printf("handshake ack to %s has failed: %v", clientName, err)
		hub.unregisterSession(sess)
		return
	}

	// Announce successful connection to others.
	f, err := announcePresence(sess.Name, eventConnected)
	if err != nil {
		log.Printf("could not build connect announcement for %s: %v", sess.Name, err)
		hub.unregisterSession(sess)
		return
	}
	if err := hub.broadcastFrame(f); err != nil {
		log.Printf("could not announce %s: %v", sess.Name, err)
		hub.unregisterSession(sess)
		return
	}

	go sess.writeLoop(hub)
	go sess.readLoop(hub)
}

func acceptHandshake(fullc *protocol.Conn, version string) (string, error) {
	// Get the frame for name entry.
	if err := fullc.SetReadDeadline(time.Now().Add(protocol.HandshakeTimeout)); err != nil {
		return "", fmt.Errorf("could not set a read deadline: %w", err)
	}
	f, err := fullc.Recv()
	if err != nil {
		return "", fmt.Errorf("problem with handshake frame: %w", err)
	}

	if err := fullc.SetReadDeadline(time.Time{}); err != nil {
		return "", fmt.Errorf("could not release the read deadline: %w", err)
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
// hostile name cannot get itself echoed to a terminal. acceptHandshake's other
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

// The two things that can happen to a session's presence, as the room hears
// them. Constants rather than bare strings so a caller cannot invent a third.
const (
	eventConnected    = "connected"
	eventDisconnected = "disconnected"
)

// announcePresence builds the system frame telling the room a session arrived
// or left. The two differed only in a verb.
func announcePresence(name, event string) (protocol.Frame, error) {
	return protocol.NewSystemFrame(fmt.Sprintf("%s has %s.", name, event))
}
