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

func TestIsTextCommandRecognizesServerCommands(t *testing.T) {
	for _, value := range []string{
		"AUTH principal=alice token=secret",
		"LIST_OFFSETS topic=events partition=0",
	} {
		if !IsTextCommand(value) {
			t.Fatalf("server command was not recognized: %q", value)
		}
	}
}

func TestIsTextCommandRequiresCommandToken(t *testing.T) {
	for _, value := range []string{"", "LISTEN", "\x00\x00HELP", "not-a-command", strings.Repeat("a", 1<<20)} {
		if IsTextCommand(value) {
			t.Fatalf("unexpected command match for %q", value)
		}
	}
}

func TestIsTextCommandClusterObservationTokens(t *testing.T) {
	for _, value := range []string{
		"LIST_CLUSTER",
		"list_cluster",
		"  CLUSTER_STATUS",
		"CLUSTER_STATUS extra=x",
	} {
		if !IsTextCommand(value) {
			t.Errorf("cluster observation command was not recognized: %q", value)
		}
	}
	for _, value := range []string{"LIST_CLUSTERED", "CLUSTER_STATUS_X"} {
		if IsTextCommand(value) {
			t.Errorf("similar token was recognized as a command: %q", value)
		}
	}
}
