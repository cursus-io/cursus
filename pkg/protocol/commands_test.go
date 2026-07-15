package protocol

import (
	"strings"
	"testing"
)

func TestIsTextCommandRecognizesLongRawCommand(t *testing.T) {
	command := "REPLICATE_MESSAGE payload=" + strings.Repeat("x", 22000)
	if !IsTextCommand(command) {
		t.Fatal("long raw replication command was not recognized")
	}
}

func TestIsTextCommandRequiresCommandToken(t *testing.T) {
	for _, value := range []string{"", "LISTEN", "\x00\x00HELP", "not-a-command"} {
		if IsTextCommand(value) {
			t.Fatalf("unexpected command match for %q", value)
		}
	}
}
