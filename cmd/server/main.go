package main

import (
	"flag"
	"log"
	"net"

	"github.com/HarryCoburn/simple-talk/server"
)

func main() {
	port := flag.String("port", server.DefaultPort, "port to listen on")
	flag.Parse()

	// An empty host accepts on every interface, which is what reaching this
	// server from another machine needs. JoinHostPort rather than ":"+port so
	// the one place that builds an address does it the same way the client does.
	addr := net.JoinHostPort("", *port)

	// The library reports; the command decides. log.Fatal belongs here, where
	// exiting the process is the caller's own business.
	if err := server.Run(addr); err != nil {
		log.Fatal(err)
	}
}
