package main

import (
	"encoding/json"
	"testing"

	"github.com/HarryCoburn/simple-talk/internal/protocol"
)

func TestDecorateChat(t *testing.T) {
	tests := []struct {
		name   string
		sender string
		text   string
		want   string
	}{
		{"plain message", "zoe", "hello", "<zoe> hello"},
		{"pose", "zoe", ":waves", "zoe waves"},
		{"empty pose", "zoe", ":", "zoe "},
		{"slash is just text", "zoe", "/who", "<zoe> /who"},
		{"colon later in line", "zoe", "well: no", "<zoe> well: no"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decorateChat(tc.sender, mustChatFrame(t, tc.sender, tc.text))
			if err != nil {
				t.Fatalf("decorateChat: %v", err)
			}
			if text := chatText(t, got); text != tc.want {
				t.Errorf("Got %q, want %q", text, tc.want)
			}
		})
	}
}

// The server is authoritative on identity: a client that lies about From in the
// payload must still be attributed to its registered name.
func TestDecorateChatIgnoresClaimedSender(t *testing.T) {
	f := mustChatFrame(t, "impostor", "hello")

	got, err := decorateChat("zoe", f)
	if err != nil {
		t.Fatalf("decorateChat: %v", err)
	}

	if text := chatText(t, got); text != "<zoe> hello" {
		t.Errorf("Got %q, want %q", text, "<zoe> hello")
	}

	var chat protocol.Chat
	if err := json.Unmarshal(got.Payload, &chat); err != nil {
		t.Fatalf("Could not unpack the chat payload: %v", err)
	}
	if chat.From != "zoe" {
		t.Errorf("From is %q, want the registered name %q", chat.From, "zoe")
	}
}

func TestDecorateChatRejectsMalformedPayload(t *testing.T) {
	bad := protocol.Frame{Kind: protocol.KindChat, Payload: json.RawMessage(`{"text":`)}
	if _, err := decorateChat("zoe", bad); err == nil {
		t.Fatal("Expected an error from a malformed chat payload")
	}
}
