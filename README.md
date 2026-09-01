# SimpleTalk

SimpleTalk is a terminal-based chat server and client written in Go for people who just want to talk without too much fuss. 

## Motivation

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

Run ```./server``` and one or more ```./client``` in different terminals on your machine. By default, the server will spawn at localhost:2069.


## Usage

Available flags:

- `-h` - Show help
- `-port <port>` - For the server. Sets the port to what you want.
- `-addr <ip>:<port>` - For the client. Connect to an ip and port. Mind the colon between the two.

## Client commands after loading

- A bare text line will broadcast to all connected clients
- A text line starting with ```:``` will do a pose command.
- ```/help``` will bring up a help menu
- ```/who``` will tell you the names of all connected clients and the number of connected clients
- ```/quit``` quits the client

## Examples

### Creating a server

Upload ```./server``` to your location of choice and open a TCP port in your firewall you want to use for SimpleTalk. Run ```./server -port <port>``` to set the port, or use the default port of 2069.

If you want to make it persistent, create a service on your VPS or wherever you're hosting to start the server and keep it running.

### Connecting to a remote client

Get the IP and port from whoever is running the server. Run ```./client -addr <ip>:<port>``` to start it. Use ```/help``` for commands.

The client will ask for a username. You cannot conflict with another user name already connected to the same server.

## Contributing

Clone the repo, then follow the Quick Start instructions. If you'd like to contribute, fork the repository and open a pull request to the 'main' branch.

## What I've Learned

I'm using newline-delimited JSON to send frames of data over a TCP connection. Depending on the ```Kind``` of the frame, a central ```Hub``` routes the frame to appropriate methods to format the string, run the command, close the program, etc.

First, this project gave me a good workout in learning how to use channels safely without locking them. Second, I learned quite a bit about what needs to happen to close down multiple layers of a networked program safely. The test suites are quite large and will give me a good template to use in future projects for robust testing.

I also learned more details about several new-to-me libraries in Go, like signals, precis, and sync.

It's also *very* cool to me to have built even this little bit of a chat client from scratch and to see how I would extend it. I also have a much greater appreciation for the complexity of sending data across a wire without problems and for creating networked applications without creating memory leaks.

## How I Would/Will Improve the Program

- It would be good to add /nick to change the display nickname- 
- Other things listed in TODO.md
- Look over the ICB protocol and nab any good features from that while still keeping things simple
