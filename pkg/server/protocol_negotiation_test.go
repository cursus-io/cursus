package server

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/controller"
)

func TestApplicationNegotiationCommandsAreRejectedByTransportClassifier(t *testing.T) {
	for _, command := range []string{"PROTOCOL_INFO", "NEGOTIATE", "NEGOTIATE version=2"} {
		if isCommand(command) {
			t.Fatalf("obsolete application command %q was recognized", command)
		}
	}
	for _, command := range []string{"AUTH principal=alice token=secret", "LIST_OFFSETS topic=events"} {
		if !isCommand(command) {
			t.Fatalf("Wire v2 application command %q was not recognized", command)
		}
	}
}

func TestDecorateServerResponsePreservesSuccess(t *testing.T) {
	response := "OK generation=2 member=member-1 assignments=[0]"
	if got := decorateServerResponse(response, controller.NewClientContext("group", 0)); got != response {
		t.Fatalf("decorated success = %q, want %q", got, response)
	}
}

func TestDecorateServerResponseAlwaysEnrichesWireErrors(t *testing.T) {
	want := "ERROR: GEN_MISMATCH class=fencing retryable=false expected=2 actual=1"
	if got := decorateServerResponse("ERROR: GEN_MISMATCH expected=2 actual=1", nil); got != want {
		t.Fatalf("decorated error = %q, want %q", got, want)
	}
}
