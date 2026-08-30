# SimpleTalk

SimpleTalk is a terminal-based chat server and client written in Go for people who just want to talk without too much fuss. 

Remember when phones were all we had and we communicated just fine? It was simple. I wanted a way to chat with a similar level of simplicity. SimpleTalk lets you chat with other people online in a way that:

- Doesn't have the presumptions of IRC and didn't immediately scare off people like IRC can
- Doesn't have the federation presumptions and XML-dependence of XMPP
- Lets users talk and pose similar to a talker-type MUD but without the presumptions of MUDs
- Doesn't have a presumption of an unknown party listening in like the entire rest of the interent does these days (I'm thinking businesses. If you've got government attention on you, you've got a whole different set of problems.)

At the current stage, the closest thing this client/server/protocol is similar to is [Internet Citizen's Band](https://en.wikipedia.org/wiki/Internet_Citizen's_Band). This project was created as the second capstone of the Boot.dev backend developer course.

## Quick Start

You will need Go 1.25 or later to build the project. Binary releases to come soon.

First clone the project, cd to the project directory root, and then type the following:

Server:

```go build ./server```

Client:

```go build ./client```

### Local testing

Run ```./server``` and one or more ```./client``` in different terminals on your machine. By default, the server will spawn at localhost:2069.

### Creating a server

Upload ```./server``` to your location of choice and open a TCP port in your firewall you want to use for SimpleTalk. Run ```./server -port <port>``` to set the port, or use the default port of 2069.

If you want to make it persistent, create a service on your VPS or wherever you're hosting to start the server and keep it running.

### Connecting to a remote client

Get the IP and port from whoever is running the server. Run ```./client -addr <ip>:<port>``` to start it. Use ```/help``` for commands.

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
