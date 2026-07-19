package controller

import (
	"strings"
	"testing"

	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
)

func TestRegisteredCommandsAreRecognizedByTransport(t *testing.T) {
	ch, _ := newTestHandler(t)
	for _, entry := range ch.commands {
		name := strings.TrimSpace(entry.prefix)
		if !wireprotocol.IsTextCommand(name) {
			t.Errorf("registered command %s is not recognized by the transport", name)
		}
	}
	for _, name := range []string{"STREAM", "CONSUME"} {
		if !wireprotocol.IsTextCommand(name) {
			t.Errorf("special command %s is not recognized by the transport", name)
		}
	}
}

func TestHelpOnlyListsRoutableOrClientLocalCommands(t *testing.T) {
	ch, _ := newTestHandler(t)
	routable := map[string]bool{"STREAM": true, "CONSUME": true, "EXIT": true}
	for _, entry := range ch.commands {
		routable[strings.TrimSpace(entry.prefix)] = true
	}

	response := ch.handleHelp()
	for _, name := range strings.Split(strings.TrimPrefix(response, "OK commands="), ",") {
		if !routable[name] {
			t.Errorf("HELP exposes unroutable command %s", name)
		}
	}
}
