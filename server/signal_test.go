//go:build !windows

package server

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

// Turning a real signal into a cancelled context is signal.NotifyContext's job,
// and it is the one part of shutdown no other test can reach: everything else
// cancels a context directly. Commit 1 of the context conversion removed the
// coverage this restores.
//
// Not parallel: it raises a signal at the whole test process.
func TestRunShutsDownOnSIGTERM(t *testing.T) {
	// Installed before runOn's own handler, so a SIGTERM that somehow arrives
	// early is caught rather than killing the test binary by default.
	early := make(chan os.Signal, 1)
	signal.Notify(early, syscall.SIGTERM)
	t.Cleanup(func() { signal.Stop(early) })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not open the listener: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	returned := make(chan error, 1)
	go func() { returned <- runOn(context.Background(), ln, signal.NotifyContext) }()

	// runOn installs its handler before serve starts accepting, so a listener
	// that accepts means the signal cannot be missed.
	waitUntilServing(t, ln.Addr().String())

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("Could not raise SIGTERM: %v", err)
	}

	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("runOn returned %v after SIGTERM, wanted nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runOn did not return after SIGTERM")
	}

	if _, err := net.Dial("tcp", ln.Addr().String()); err == nil {
		t.Error("The listener still accepted a connection after SIGTERM")
	}
}
