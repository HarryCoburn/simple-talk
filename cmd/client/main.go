package main

import (
	"flag"
	"log"

	"github.com/HarryCoburn/simple-talk/client"
)

func main() {
	addr := flag.String("addr", client.DefaultAddr, "server to connect to, as host:port")
	flag.Parse()

	// The library reports; the command decides. log.Fatal belongs here, where
	// exiting the process is the caller's own business.
	if err := client.Run(*addr); err != nil {
		log.Fatal(err)
	}
}
