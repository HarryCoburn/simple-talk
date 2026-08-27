# SimpleTalk

SimpleTalk is a terminal-based chat server and client written in Go. It is "simple" because it follows an older, less secure style of network communication akin to tallker MUDs. Just open port on your firewall, start the server and point it to the port, then anyone with your IP and port can communicate with each other by text over the channel.

There is no encryption in this program, so do not use this for anything you want private. 

## To Build

First clone the project, cd to the project directory root, and then type the following:

Server:

```go build ./server```

Client:

```go build ./client```

## Usage

Using the -h flag when running either the server or the client will bring up help, but here are the simple basics

### Server

- Choose a port you want to open on your server and open it through your firewall.
- Run ```./server -port <port>```. The default port is 2069.
- Kill the server with Ctrl-C.
- Add it to a service if you want it to persist on a VPS or wherever you put it.

### Client

- Get the IP and port from whoever is running the server.
- Run ```./client -addr <ip>:<port>``` to start it, or just ```./client``` to start on localhost:2069 for local testing.
- Kill the client with Ctrl-C.

### Testing

If you just want to check it out, run ./server on your local machine and then two ./client instances in another window.

### Current commands

- A bare text line will broadcast to all connected clients
- A text line starting with ```:``` will do a pose command.
- ```/help``` will bring up a help menu
- ```/who``` will tell you the names of all connected clients and the number of connected clients

## Why I Built This

This is the second capstone project for the Boot.dev Backend Developer pathway and my first greenfield project in Go. I chose the topic and built it using the knowledge I learned there, then used LLM assistance to fill in testing gaps and discover edge cases I would not know about with my current level of Go and backend programmer experience.

I grew up in an older era of the internet where protocols, not platforms, were the name of the game. I wanted to see if I could create one and get a client and server to talk using it. The truly old-school way would have been something like the Telnet specification or copying the protocol that a MUD platform would use. However, that would cut out a lot of the things I learned in Boot.Dev, which focused a lot on marshalling and unmarshalling JSON, so I went with that route.

It is intended to be a portfolio project for a possible future job, so I did not want to make to too complicated for an interviewer to study or too difficult to test (e.g. adding encryption and forcing the use of a TLS certificate.)

## How it Functions

I'm using newline-delimited JSON to send frames of data over a TCP connection. Depending on the ```Kind``` of the frame, a central ```Hub``` routes the frame to appropriate methods to format the string, run the command, close the program, etc.

## What I've Learned

First, this gave me a good workout in learning how to use channels safely without locking them. Second, I learned quite a bit about what needs to happen to close down multiple layers of a networked program safely. The test suites are quite large and will give me a good template to use in future projects for robust testing.

I also learned more details about several new-to-me libraries in Go, like signals, precis, and sync.

It's also *very* cool to me to have built even this little bit of a chat client from scratch and to see how I would extend it. I also have a much greater appreciation for the complexity of sending data across a wire without problems and for creating networked applications without creating memory leaks.

## How I Would/Will Improve the Program

- Both the server and the client really do need a proper /quit command
- It would also be good to add /nick to change the display nickname
- Other things listed in TODO.md
