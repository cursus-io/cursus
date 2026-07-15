package server

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/util"
)

func TestNegotiatedTransportErrorsAreStructured(t *testing.T) {
	client, server := newTestConnPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	handler := controller.NewCommandHandler(nil, config.DefaultConfig(), nil, nil, nil)
	ctx := controller.NewClientContext("default-group", 0)
	ctx.SetProtocol(1, []wireprotocol.Feature{wireprotocol.FeatureStructuredErrorsV1})
	done := make(chan struct{})
	go func() {
		defer close(done)
		shouldExit, err := processMessage(util.EncodeMessage("topic", "not-a-command"), handler, ctx, server)
		if err != nil || !shouldExit {
			t.Errorf("process result: shouldExit=%v err=%v", shouldExit, err)
		}
	}()
	want := "ERROR: malformed_input class=validation retryable=false reason=missing_topic_or_payload"
	if got := readFramed(t, client); got != want {
		t.Fatalf("transport error = %q, want %q", got, want)
	}
	<-done
}
