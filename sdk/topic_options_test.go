package sdk

import (
	"strings"
	"testing"
)

func TestBuildCreateTopicCommandWithOptions(t *testing.T) {
	command, err := buildCreateTopicCommand("state", TopicOptions{
		Partitions:     3,
		CleanupPolicy:  "compact,delete",
		RetentionHours: 24,
		RetentionBytes: 1024,
		Partitioner:    "hash_key",
		AuthPolicy:     "acl",
		ReadACL:        []string{"reader"},
		WriteACL:       []string{"writer"},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE topic=state partitions=3 idempotent=true cleanup_policy=delete,compact retention_hours=24 retention_bytes=1024 partitioner=hash_key auth_policy=acl read_acl=reader write_acl=writer"
	if command != want {
		t.Fatalf("command = %q, want %q", command, want)
	}
}

func TestBuildCreateTopicCommandRejectsUnsafeOrInvalidOptions(t *testing.T) {
	tests := []TopicOptions{
		{Partitions: 0},
		{Partitions: 1, CleanupPolicy: "unknown"},
		{Partitions: 1, RetentionHours: -1},
		{Partitions: 1, RetentionBytes: -1},
		{Partitions: 1, Partitioner: "random"},
		{Partitions: 1, AuthPolicy: "unknown"},
		{Partitions: 1, ReadACL: []string{"reader principal"}},
	}
	for _, options := range tests {
		if _, err := buildCreateTopicCommand("state", options, false); err == nil {
			t.Fatalf("expected options to fail: %+v", options)
		}
	}
	if _, err := buildCreateTopicCommand("state command=DELETE", TopicOptions{Partitions: 1}, false); err == nil {
		t.Fatal("unsafe topic name was accepted")
	}
}

func TestBuildCreateTopicCommandUsesPortableTopicNameContract(t *testing.T) {
	for _, name := range []string{
		"orders",
		"orders.v2",
		"orders-v2",
		"__consumer_offsets",
		"exactly-once-acks=all_broker_failure",
	} {
		if _, err := buildCreateTopicCommand(name, TopicOptions{Partitions: 1}, false); err != nil {
			t.Fatalf("valid topic name %q was rejected: %v", name, err)
		}
	}

	for _, name := range []string{
		"",
		".",
		"..",
		"orders/2026",
		"orders\\2026",
		"orders 2026",
		"한글",
		strings.Repeat("a", 250),
	} {
		if _, err := buildCreateTopicCommand(name, TopicOptions{Partitions: 1}, false); err == nil {
			t.Fatalf("invalid topic name %q was accepted", name)
		}
	}
}

func TestBuildCreateTopicCommandOmitsInheritedValues(t *testing.T) {
	command, err := buildCreateTopicCommand("state", TopicOptions{Partitions: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(command, "cleanup_policy=") || strings.Contains(command, "retention_") {
		t.Fatalf("inherited options unexpectedly serialized: %s", command)
	}
}
