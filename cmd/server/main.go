package main

import (
	"log"

	"github.com/HarryCoburn/simple-talk/server"
)

func main() {
	// The library reports; the command decides. log.Fatal belongs here, where
	// exiting the process is the caller's own business.
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}
