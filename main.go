package main

import (
	"log"
	"net/http"
)

func main() {
	servMux := http.NewServeMux()
	server := &http.Server{
		Addr:    ":2069",
		Handler: servMux,
	}

	log.Fatal(server.ListenAndServe())
}
