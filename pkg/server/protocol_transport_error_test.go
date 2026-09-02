package server

import (
	"net"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/wire"
)

func TestWireTransportErrorsAreStructured(t *testing.T) {
	client, server := newTestConnPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	handler := controller.NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	ctx := controller.NewClientContext("default-group", 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		shouldExit, err := processMessage([]byte("not-a-command"), handler, ctx, server)
		if err != nil || !shouldExit {
			t.Errorf("process result: shouldExit=%v err=%v", shouldExit, err)
		}
	}()
	want := "ERROR: malformed_input class=validation retryable=false reason=command_payload_required"
	if got := readFramed(t, client); got != want {
		t.Fatalf("transport error = %q, want %q", got, want)
	}
	<-done
}

func TestStreamClosePayloadIsCaseInsensitive(t *testing.T) {
	for _, payload := range []string{
		"STREAM_CONTROL type=close",
		"STREAM_CONTROL type=CLOSE",
		"stream_control reason=complete TYPE=Close",
	} {
		if !isStreamClosePayload([]byte(payload)) {
			t.Fatalf("stream close payload %q was not recognized", payload)
		}
	}
	if isStreamClosePayload([]byte("STREAM_CONTROL type=open")) {
		t.Fatal("stream open payload was recognized as close")
	}
}

func TestReadStreamTextErrorBecomesTypedWireError(t *testing.T) {
	clientRaw, serverRaw := net.Pipe()
	t.Cleanup(func() { _ = clientRaw.Close() })
	t.Cleanup(func() { _ = serverRaw.Close() })

	serverResult := make(chan *serverWireConn, 1)
	serverError := make(chan error, 1)
	go func() {
		connection, err := wire.ServerHandshake(serverRaw, []wire.Compression{wire.CompressionNone})
		if err != nil {
			serverError <- err
			return
		}
		serverResult <- newServerWireConn(serverRaw, connection)
	}()
	client, err := wire.ClientHandshake(clientRaw, []wire.Compression{wire.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	server := <-serverResult
	server.setRequest(wire.Frame{Command: wire.CommandReadStream, RequestID: 17})
	go func() { serverError <- server.WritePayload([]byte("ERROR: invalid_from_version")) }()

	frame, err := client.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if frame.Status != wire.StatusError || frame.Command != wire.CommandReadStream || frame.RequestID != 17 {
		t.Fatalf("unexpected response frame: %+v", frame)
	}
	payload, err := wire.DecodeError(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if payload.Code != "invalid_from_version" || payload.Class != wire.ErrorClassValidation {
		t.Fatalf("unexpected error payload: %+v", payload)
	}
	if err := <-serverError; err != nil {
		t.Fatal(err)
	}
}
