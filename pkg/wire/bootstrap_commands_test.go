package wire

import "testing"

func TestBootstrapCommandIDsAppend(t *testing.T) {
	if CommandReassignPartition != 57 || CommandBootstrapBegin != 58 || CommandBootstrapChunk != 59 || CommandBootstrapRelease != 60 {
		t.Fatal("wire command IDs changed")
	}
	for _, command := range []Command{CommandBootstrapBegin, CommandBootstrapChunk, CommandBootstrapRelease} {
		parsed, err := ParseCommand(command.String())
		if err != nil || parsed != command {
			t.Fatalf("command round trip %v: %v", command, err)
		}
	}
}
