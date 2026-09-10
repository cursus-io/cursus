package e2e_cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
)

const runGroupCoordinatorFailoverE2E = "RUN_E2E_GROUP_COORDINATOR_FAILOVER"

// TestGroupCoordinatorFailoverUsesDurableMembership reproduces coordinator
// loss during a live group. It is opt-in because it stops and restarts a real
// compose broker. Discovery must change only after the replicated inactive
// membership transition, and no offset commit is accepted on the failed path.
func TestGroupCoordinatorFailoverUsesDurableMembership(t *testing.T) {
	if os.Getenv(runGroupCoordinatorFailoverE2E) != "1" {
		t.Skipf("set %s=1 to run the compose coordinator failover test", runGroupCoordinatorFailoverE2E)
	}

	startFailureTestCluster(t)
	waitForAllBrokerReadiness(t, []int{1, 2, 3})
	waitForSingleRaftLeader(t, []int{1, 2, 3}, 0)

	topic := "group-coordinator-failover"
	group := "group-coordinator-failover-workers"
	client := e2e.NewBrokerClient(clusterBrokerAddrs(3))
	defer client.Close()
	if err := client.CreateTopic(topic, 1, false); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	generation, member, err := client.JoinGroup(topic, group)
	if err != nil {
		t.Fatalf("join group: %v", err)
	}
	if _, err := client.SyncGroup(topic, group, generation, member); err != nil {
		t.Fatalf("sync group: %v", err)
	}

	oldID := findGroupCoordinator(t, 1, group)
	oldNode := coordinatorNode(t, oldID)
	stopComposeBroker(t, oldNode)
	survivors := survivorNodes(oldNode)
	waitForSingleRaftLeader(t, survivors, oldNode)
	waitForAllBrokerReadiness(t, survivors)

	var newID string
	if err := eventually(t, "durable group coordinator failover", clusterReadyTimeout, func() (bool, string, error) {
		for _, node := range survivors {
			id, err := findGroupCoordinatorFrom(node, group)
			if err != nil {
				return false, fmt.Sprintf("broker-%d: %v", node, err), nil
			}
			if id == oldID {
				return false, fmt.Sprintf("broker-%d still returns %s", node, oldID), nil
			}
			if newID != "" && id != newID {
				return false, fmt.Sprintf("inconsistent coordinators %s and %s", newID, id), nil
			}
			newID = id
		}
		return newID != "", fmt.Sprintf("coordinator=%s", newID), nil
	}); err != nil {
		t.Fatal(err)
	}

	commitClient := e2e.NewBrokerClient(clusterBrokerAddrsForNodes(survivors))
	commitClient.SetMemberID(member)
	if _, err := commitClient.SyncGroup(topic, group, generation, member); err != nil {
		// Membership fencing is an allowed fail-closed outcome. Rejoin before
		// committing rather than treating the prior generation as successful.
		generation, member, err = commitClient.JoinGroup(topic, group)
		if err != nil {
			commitClient.Close()
			t.Fatalf("rejoin after coordinator fence: %v", err)
		}
		if _, err := commitClient.SyncGroup(topic, group, generation, member); err != nil {
			commitClient.Close()
			t.Fatalf("sync after coordinator fence: %v", err)
		}
	}
	commitResp, err := commitClient.SendCommand("", fmt.Sprintf("COMMIT_OFFSET topic=%s partition=0 group=%s offset=1 generation=%d member=%s", topic, group, generation, member), 15*time.Second)
	if err != nil || strings.HasPrefix(commitResp, "ERROR:") {
		commitClient.Close()
		t.Fatalf("commit through recovered coordinator: response=%q err=%v", commitResp, err)
	}
	offset, err := commitClient.FetchCommittedOffset(topic, 0, group)
	commitClient.Close()
	if err != nil || offset != 1 {
		t.Fatalf("durable recovered offset=%d err=%v, want 1", offset, err)
	}

	// Recovery registers a new broker incarnation and reintroduces it only
	// after the leader commits active membership.
	startComposeBroker(t, oldNode)
	waitForAllBrokerReadiness(t, []int{1, 2, 3})
}

func findGroupCoordinator(t *testing.T, node int, group string) string {
	t.Helper()
	id, err := findGroupCoordinatorFrom(node, group)
	if err != nil {
		t.Fatalf("find coordinator from broker-%d: %v", node, err)
	}
	return id
}

func findGroupCoordinatorFrom(node int, group string) (string, error) {
	client := e2e.NewBrokerClient([]string{fmt.Sprintf("127.0.0.1:%d", brokerPort(node))})
	defer client.Close()
	resp, err := client.SendCommand("", fmt.Sprintf("FIND_COORDINATOR group=%s", group), 2*time.Second)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(resp, "ERROR:") {
		return "", fmt.Errorf("%s", resp)
	}
	for _, part := range strings.Fields(resp) {
		if strings.HasPrefix(part, "coordinator_id=") {
			return strings.TrimPrefix(part, "coordinator_id="), nil
		}
	}
	return "", fmt.Errorf("missing coordinator_id in %q", resp)
}

func coordinatorNode(t *testing.T, coordinatorID string) int {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(coordinatorID, "broker-"), "-")
	node, err := strconv.Atoi(parts[0])
	if err != nil || node < 1 || node > 3 {
		t.Fatalf("unexpected coordinator ID %q", coordinatorID)
	}
	return node
}

func clusterBrokerAddrsForNodes(nodes []int) []string {
	addrs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		addrs = append(addrs, fmt.Sprintf("localhost:%d", brokerPort(node)))
	}
	return addrs
}

func startComposeBroker(t *testing.T, node int) {
	t.Helper()
	service := fmt.Sprintf("broker-%d", node)
	if output, err := e2e.RunCompose("-f", composeFile, "start", service).CombinedOutput(); err != nil {
		t.Fatalf("start %s: %v\n%s", service, err, output)
	}
}
