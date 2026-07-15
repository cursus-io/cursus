package server

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/util"
)

func TestProcessMessageNegotiatesAndDecoratesSubsequentErrors(t *testing.T) {
	client, server := newTestConnPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	handler := controller.NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	ctx := controller.NewClientContext("default-group", 0)

	done := make(chan struct{})
	go func() {
		defer close(done)
		shouldExit, err := processMessage(
			util.EncodeMessage("", "NEGOTIATE version=1 features=structured_errors_v1"),
			handler,
			ctx,
			server,
		)
		if err != nil || shouldExit {
			t.Errorf("negotiate process result: shouldExit=%v err=%v", shouldExit, err)
		}
	}()
	if got := readFramed(t, client); got != "OK protocol_version=1 enabled=structured_errors_v1 unsupported=" {
		t.Fatalf("unexpected negotiation response: %s", got)
	}
	<-done

	done = make(chan struct{})
	go func() {
		defer close(done)
		shouldExit, err := processMessage(util.EncodeMessage("", "HELP extra"), handler, ctx, server)
		if err != nil || shouldExit {
			t.Errorf("error process result: shouldExit=%v err=%v", shouldExit, err)
		}
	}()
	want := `ERROR: unknown_command class=validation retryable=false command="HELP extra"`
	if got := readFramed(t, client); got != want {
		t.Fatalf("structured error = %q, want %q", got, want)
	}
	<-done
}

func TestProtocolCommandsAreRecognizedAsRawCommands(t *testing.T) {
	for _, command := range []string{"PROTOCOL_INFO", "NEGOTIATE version=1"} {
		if !isCommand(command) {
			t.Fatalf("%s was not recognized", command)
		}
	}
}
