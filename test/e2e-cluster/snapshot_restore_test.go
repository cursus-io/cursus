package e2e_cluster

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

const maxSnapshotFillerTopics = 32

func TestRaftSnapshotRestorePreservesAndAdvancesTopicState(t *testing.T) {
	if os.Getenv("RUN_E2E_RAFT_SNAPSHOT") != "1" {
		t.Skip("set RUN_E2E_RAFT_SNAPSHOT=1 to run Raft snapshot restore validation")
	}

	ctx := GivenSnapshotClusterRestart(t).WithClusterSize(3)
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()

	follower := waitForRaftFollower(t, actions)
	keep := LocalTopicDefinition{Topic: "snapshot-keep", Partitions: 2, CleanupPolicy: "delete"}
	deleted := LocalTopicDefinition{Topic: "snapshot-delete", Partitions: 1, CleanupPolicy: "delete"}
	created := LocalTopicDefinition{Topic: "post-restore-create", Partitions: 3, CleanupPolicy: "delete"}

	if err := actions.CreateNamedTopic(keep.Topic, keep.Partitions); err != nil {
		t.Fatalf("create baseline topic %s: %v", keep.Topic, err)
	}
	if err := actions.CreateNamedTopic(deleted.Topic, deleted.Partitions); err != nil {
		t.Fatalf("create baseline topic %s: %v", deleted.Topic, err)
	}
	if err := actions.WaitForLocalTopicDefinition(follower, keep, true); err != nil {
		t.Fatal(err)
	}
	if err := actions.WaitForLocalTopicDefinition(follower, deleted, true); err != nil {
		t.Fatal(err)
	}
	followerStatus, err := actions.LocalClusterStatus(follower)
	if err != nil {
		t.Fatalf("read follower status before snapshot: %v", err)
	}
	targetAppliedIndex := followerStatus.RaftAppliedIndex
	if targetAppliedIndex == 0 {
		t.Fatal("follower applied index remained zero after baseline topic materialization")
	}

	for filler := 0; filler < maxSnapshotFillerTopics; filler++ {
		status, statusErr := actions.LocalClusterStatus(follower)
		if statusErr == nil && status.RaftLastSnapshotIndex >= targetAppliedIndex {
			break
		}
		name := fmt.Sprintf("snapshot-filler-%02d", filler)
		if err := actions.CreateNamedTopic(name, 1); err != nil {
			t.Fatalf("create bounded filler topic %s: %v", name, err)
		}
	}
	preRestart, err := actions.WaitForLocalSnapshot(follower, targetAppliedIndex)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("broker-%d snapshot ready: target=%d snapshot=%d applied=%d", follower, targetAppliedIndex, preRestart.RaftLastSnapshotIndex, preRestart.RaftAppliedIndex)

	actions.StopBroker(follower)
	actions.StartBroker(follower)

	restored, err := actions.WaitForLocalSnapshot(follower, targetAppliedIndex)
	if err != nil {
		t.Fatal(err)
	}
	if restored.RaftLastSnapshotIndex < preRestart.RaftLastSnapshotIndex {
		t.Fatalf("broker-%d snapshot index regressed after restart: before=%d after=%d", follower, preRestart.RaftLastSnapshotIndex, restored.RaftLastSnapshotIndex)
	}
	if err := actions.WaitForLocalTopicDefinition(follower, keep, true); err != nil {
		t.Fatal(err)
	}
	if err := actions.WaitForLocalTopicDefinition(follower, deleted, true); err != nil {
		t.Fatal(err)
	}

	beforeMutation := captureLocalAppliedIndices(t, actions)
	if err := actions.CreateNamedTopic(created.Topic, created.Partitions); err != nil {
		t.Fatalf("create topic immediately after restore: %v", err)
	}
	if err := actions.DeleteNamedTopic(deleted.Topic); err != nil {
		t.Fatalf("delete topic immediately after restore: %v", err)
	}

	if err := waitForAllLocalTopicState(actions, beforeMutation, keep, created, deleted); err != nil {
		t.Fatal(err)
	}
}

func waitForRaftFollower(t *testing.T, actions *ClusterActions) int {
	t.Helper()
	follower := 0
	if err := eventually(t, "direct-node Raft follower", clusterReadyTimeout, func() (bool, string, error) {
		details := make([]string, 0, actions.ctx.clusterSize)
		for node := 1; node <= actions.ctx.clusterSize; node++ {
			status, err := actions.LocalClusterStatus(node)
			if err != nil {
				return false, fmt.Sprintf("broker-%d status failed", node), err
			}
			details = append(details, fmt.Sprintf("broker-%d=%s", node, status.RaftState))
			if follower == 0 && strings.EqualFold(status.RaftState, "Follower") {
				follower = node
			}
		}
		return follower != 0, strings.Join(details, " "), nil
	}); err != nil {
		t.Fatal(err)
	}
	return follower
}

func captureLocalAppliedIndices(t *testing.T, actions *ClusterActions) map[int]uint64 {
	t.Helper()
	indices := make(map[int]uint64, actions.ctx.clusterSize)
	for node := 1; node <= actions.ctx.clusterSize; node++ {
		status, err := actions.LocalClusterStatus(node)
		if err != nil {
			t.Fatalf("read broker-%d status before mutation: %v", node, err)
		}
		indices[node] = status.RaftAppliedIndex
	}
	return indices
}

func waitForAllLocalTopicState(actions *ClusterActions, before map[int]uint64, keep, created, deleted LocalTopicDefinition) error {
	return eventually(actions.ctx.GetT(), "exact post-restore topic state on every broker", clusterReadyTimeout, func() (bool, string, error) {
		for node := 1; node <= actions.ctx.clusterSize; node++ {
			status, err := actions.LocalClusterStatus(node)
			if err != nil {
				return false, fmt.Sprintf("broker-%d CLUSTER_STATUS failed", node), err
			}
			if status.RaftAppliedIndex <= before[node] {
				return false, fmt.Sprintf("broker-%d applied=%d before=%d", node, status.RaftAppliedIndex, before[node]), nil
			}
			for _, expected := range []LocalTopicDefinition{keep, created} {
				actual, found, err := actions.LocalTopicDefinition(node, expected.Topic)
				if err != nil {
					return false, fmt.Sprintf("broker-%d topic=%s", node, expected.Topic), err
				}
				if !found || actual != expected {
					return false, fmt.Sprintf("broker-%d topic=%s found=%t actual=%+v", node, expected.Topic, found, actual), nil
				}
			}
			if actual, found, err := actions.LocalTopicDefinition(node, deleted.Topic); err != nil {
				return false, fmt.Sprintf("broker-%d deleted topic query", node), err
			} else if found {
				return false, fmt.Sprintf("broker-%d deleted topic still present: %+v", node, actual), nil
			}
		}
		return true, "all brokers converged", nil
	})
}
