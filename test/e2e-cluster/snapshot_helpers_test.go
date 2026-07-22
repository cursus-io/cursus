package e2e_cluster

import (
	"reflect"
	"testing"
)

func TestParseLocalClusterStatus(t *testing.T) {
	status, err := parseLocalClusterStatus(`OK cluster={"raft_state":"Follower","raft_applied_index":42,"raft_commit_index":43,"raft_last_log_index":44,"raft_last_snapshot_index":40,"raft_last_snapshot_term":3}`)
	if err != nil {
		t.Fatal(err)
	}
	if status.RaftState != "Follower" || status.RaftAppliedIndex != 42 ||
		status.RaftLastSnapshotIndex != 40 || status.RaftLastSnapshotTerm != 3 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestParseLocalClusterStatusRejectsNonLocalResponse(t *testing.T) {
	for _, response := range []string{
		"ERROR: raft_status_unavailable",
		`OK cluster={"raft_state":""}`,
	} {
		if _, err := parseLocalClusterStatus(response); err == nil {
			t.Fatalf("expected error for %q", response)
		}
	}
}

func TestParseLocalTopicDefinition(t *testing.T) {
	definition, err := parseLocalTopicDefinition("OK topic=orders partitions=3 leaders=a,b,c epochs=1,1,1 cleanup_policy=delete partitioner=hash")
	if err != nil {
		t.Fatal(err)
	}
	want := LocalTopicDefinition{Topic: "orders", Partitions: 3, CleanupPolicy: "delete"}
	if definition != want {
		t.Fatalf("definition=%+v want=%+v", definition, want)
	}
}

func TestComposeCommandArgsIncludesSnapshotOverlay(t *testing.T) {
	got := composeCommandArgs(
		"cursus-snapshot-e2e",
		[]string{"docker-compose.yml", "docker-compose.snapshot.yml"},
		"up", "-d",
	)
	want := []string{
		"-p", "cursus-snapshot-e2e",
		"-f", "docker-compose.yml",
		"-f", "docker-compose.snapshot.yml",
		"up", "-d",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compose args=%v want=%v", got, want)
	}
}
