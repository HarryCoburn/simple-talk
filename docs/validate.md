# Validate library

This holds the rules for user-supplied text. It uses the precis library to handle nicknames and the keys that clientList uses to uniquely identify clients.

It also holds constants for the length of allowed names and the maximum text length. This is separate from the maximum message size, which also includes JSON and decoration.

Primarily, we are checking for:

- valid UTF-8
- no control characters
- no escape sequences
- no blank lines
- not too long

Name() verifies a username is okay.
NameKey() verifies that a key for clientList is okay.
Message() verifies that a string sent by a client for chat is okay.
