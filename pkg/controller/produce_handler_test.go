package controller

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
)

func TestAppendedLeaderMessagesExcludesDuplicateNoops(t *testing.T) {
	messages := []types.Message{
		{Payload: "duplicate", SeqNum: 1},
		{Payload: "new", SeqNum: 2},
	}
	markLeaderOffsetsUnassigned(messages)
	messages[1].Offset = 7

	appended := appendedLeaderMessages(messages)
	if len(appended) != 1 {
		t.Fatalf("appendedLeaderMessages() returned %d messages, want 1", len(appended))
	}
	if appended[0].Payload != "new" || appended[0].Offset != 7 {
		t.Fatalf("appendedLeaderMessages() returned %+v, want new message at offset 7", appended[0])
	}
}
