// Package validate holds the rules for user-supplied text. The server enforces
// them; the client calls the same functions so a user learns about a bad name
// at the prompt instead of after a round trip. The server never trusts that the
// client ran them.
package validate

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/secure/precis"
)

const (
	MaxNameRunes    = 32  // username length
	MaxMessageRunes = 512 // maximum message length while allowing decoration and JSON
)

var (
	ErrNameEmpty      = errors.New("a username cannot be blank")
	ErrNameTooLong    = fmt.Errorf("a username cannot be longer than %d characters", MaxNameRunes)
	ErrNameNotAllowed = errors.New("that username contains characters that are not allowed")

	ErrMessageEmpty      = errors.New("a message cannot be blank")
	ErrMessageTooLong    = fmt.Errorf("a message cannot be longer than %d characters", MaxMessageRunes)
	ErrMessageNotAllowed = errors.New("that message contains characters that are not allowed")
)

// precis.Nickname handles trimming, spaces, control characters, escape sequences, and other shenanigans.
// Spaces and emoji allowed inside names. TODO: Check if BubbleTea will have problems displaying Unicode
var nickname = precis.Nickname

// Creates the comparison form of a name so Harry and harry do not collide.
var nameFold = precis.NewFreeform(precis.FoldCase())

func Name(raw string) (string, error) {
	// Trim before run
	raw = strings.TrimSpace(raw)
	name, err := nickname.String(raw)
	if err != nil {
		if raw == "" {
			return "", ErrNameEmpty
		}
		return "", ErrNameNotAllowed
	}
	if name == "" {
		return "", ErrNameEmpty
	}
	if utf8.RuneCountInString(name) > MaxNameRunes {
		return "", ErrNameTooLong
	}
	return name, nil
}

func NameKey(name string) (string, error) {
	key, err := nameFold.String(name)
	if err != nil {
		return "", ErrNameNotAllowed
	}
	return key, nil
}

// Message returns the cleaned form of one chat message. PRECIS has no profile
// for free text a human reads, so we manually check for: valid UTF-8,
// no control characters or escape sequences, not blank, not too long.
func Message(raw string) (string, error) {
	if !utf8.ValidString(raw) {
		return "", ErrMessageNotAllowed
	}
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", ErrMessageEmpty
	}
	if utf8.RuneCountInString(text) > MaxMessageRunes {
		return "", ErrMessageTooLong
	}
	for _, r := range text {
		// Control runes cover \n, \r and the ESC that starts an ANSI sequence.
		// Any of them would let a sender write to another user's terminal.
		if unicode.IsControl(r) {
			return "", ErrMessageNotAllowed
		}
	}
	return text, nil
}
