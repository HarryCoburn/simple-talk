package validate

import (
	"errors"
	"strings"
	"testing"
)

func TestName(t *testing.T) {
	cases := []struct {
		desc    string
		input   string
		want    string
		wantErr error
	}{
		{"plain", "Harry", "Harry", nil},
		{"keeps case", "HARRY", "HARRY", nil},
		{"trims", "  Harry  ", "Harry", nil},
		{"collapses inner runs", "Harry  Coburn", "Harry Coburn", nil},
		{"allows spaces", "Harry Coburn", "Harry Coburn", nil},
		{"allows emoji", "🐍snake", "🐍snake", nil},
		{"allows accents", "Alicé", "Alicé", nil},
		{"blank", "", "", ErrNameEmpty},
		{"only spaces", "   ", "", ErrNameEmpty},
		{"trims tabs and newlines", "\t Harry \n", "Harry", nil},
		{"rejects an inner tab", "Har\try", "", ErrNameNotAllowed},
		{"newline", "Ali\nce", "", ErrNameNotAllowed},
		{"ansi escape", "Ali\x1b[31mce", "", ErrNameNotAllowed},
		{"zero width", "Ali​ce", "", ErrNameNotAllowed},
		{"too long", strings.Repeat("a", MaxNameRunes+1), "", ErrNameTooLong},
		{"at the limit", strings.Repeat("a", MaxNameRunes), strings.Repeat("a", MaxNameRunes), nil},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := Name(tc.input)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Name(%q) returned error %v, wanted %v", tc.input, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("Name(%q) = %q, wanted %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestNameKeyCollides(t *testing.T) {
	cases := []struct {
		desc     string
		a, b     string
		wantSame bool
	}{
		{"case differs", "Harry", "harry", true},
		{"case differs in the middle", "McHarry", "mcharry", true},
		{"different people", "Harry", "Bob", false},
		{"different scripts look alike", "Alice", "Аlice", false},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ka, err := NameKey(tc.a)
			if err != nil {
				t.Fatalf("NameKey(%q) failed: %v", tc.a, err)
			}
			kb, err := NameKey(tc.b)
			if err != nil {
				t.Fatalf("NameKey(%q) failed: %v", tc.b, err)
			}
			if (ka == kb) != tc.wantSame {
				t.Errorf("NameKey(%q)==NameKey(%q) was %v, wanted %v", tc.a, tc.b, ka == kb, tc.wantSame)
			}
		})
	}
}

func TestMessage(t *testing.T) {
	cases := []struct {
		desc    string
		input   string
		want    string
		wantErr error
	}{
		{"plain", "Hello", "Hello", nil},
		{"trims", "  Hello  ", "Hello", nil},
		{"keeps inner spacing", "Hello  there", "Hello  there", nil},
		{"pose", ":waves", ":waves", nil},
		{"emoji", "🐍", "🐍", nil},
		{"blank", "", "", ErrMessageEmpty},
		{"only spaces", "   ", "", ErrMessageEmpty},
		{"newline", "one\ntwo", "", ErrMessageNotAllowed},
		{"carriage return", "one\rtwo", "", ErrMessageNotAllowed},
		{"ansi escape", "\x1b[2J", "", ErrMessageNotAllowed},
		{"tab", "one\ttwo", "", ErrMessageNotAllowed},
		{"invalid utf8", "\xff\xfe", "", ErrMessageNotAllowed},
		{"too long", strings.Repeat("a", MaxMessageRunes+1), "", ErrMessageTooLong},
		{"at the limit", strings.Repeat("a", MaxMessageRunes), strings.Repeat("a", MaxMessageRunes), nil},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := Message(tc.input)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Message(%q) returned error %v, wanted %v", tc.input, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("Message(%q) = %q, wanted %q", tc.input, got, tc.want)
			}
		})
	}
}
