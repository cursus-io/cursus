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
}

func TestHelpOnlyListsRoutableOrClientLocalCommands(t *testing.T) {
	ch, _ := newTestHandler(t)
	routable := map[string]bool{"EXIT": true}
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

func TestHelpResponseOrderContract(t *testing.T) {
	ch, _ := newTestHandler(t)
	const want = "OK commands=CREATE,DELETE,LIST,PUBLISH,CONSUME,STREAM,JOIN_GROUP,SYNC_GROUP,LEAVE_GROUP,HEARTBEAT,COMMIT_OFFSET,BATCH_COMMIT,FETCH_OFFSET,LIST_OFFSETS,INIT_PRODUCER_ID,BEGIN_TXN,TXN_PUBLISH,SEND_OFFSETS_TO_TXN,END_TXN,TXN_STATUS,REGISTER_GROUP,GROUP_STATUS,LIST_GROUPS,DESCRIBE,APPEND_STREAM,READ_STREAM,SAVE_SNAPSHOT,READ_SNAPSHOT,STREAM_VERSION,METADATA,FIND_COORDINATOR,PROTOCOL_INFO,NEGOTIATE,LIST_CLUSTER,CLUSTER_STATUS,ELECT_LEADER,HELP,EXIT"
	if got := ch.handleHelp(); got != want {
		t.Fatalf("HELP response changed: got=%q want=%q", got, want)
	}
}
