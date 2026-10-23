package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestTruncateReregistrationConvergesDelayedFollower(t *testing.T) {
	if os.Getenv("RUN_E2E_CLEANUP_RECOVERY") != "1" {
		t.Skip("set RUN_E2E_CLEANUP_RECOVERY=1")
	}
	ctx := givenCleanupCluster(t, composeFile, snapshotComposeFile, "docker-compose.consumer-cleanup.yml", "docker-compose.consumer-cleanup-recovery.yml").WithClusterSize(3).WithTopic(cleanupRecoveryTopic).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	addrs := ctx.GetBrokerAddrs()
	sendClusterTopicCommand(t, addrs, "CREATE topic="+cleanupRecoveryTopic+" partitions=1 replication_factor=3 idempotent=true")
	requireClusterDefinitionEventually(t, addrs, cleanupRecoveryTopic, map[string]string{"revision": "1"})
	requireClusterStatusMaterialized(t, addrs)
	old, generation, member := joinRecoveryGroup(t, addrs, cleanupRecoveryTopic, cleanupRecoveryGroup)
	defer old.Close()
	commitGroupOffset(t, old, cleanupRecoveryTopic, cleanupRecoveryGroup, 9, generation, member)
	owner := coordinatorNode(t, findGroupCoordinator(t, 1, cleanupRecoveryGroup))
	delayed := survivorNodes(owner)[0]
	cleanupBrokerCommand(t, "restart", delayed)
	require.NoError(t, actions.waitForNodeHealth(delayed, clusterHealthCheckAddrs(3)[delayed-1]))
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "follower cached old group")
	leaveCleanupGroup(t, old, generation, member)
	setCleanupReadFault(t, []int{delayed}, true)
	sendClusterTopicCommand(t, addrs, "TRUNCATE topic="+cleanupRecoveryTopic+" expected_revision=1")
	requireCleanupReadFaultObserved(t, []string{addrs[delayed-1]}, []int{delayed})
	available := make([]string, 0, 2)
	for node, addr := range addrs {
		if node+1 != delayed {
			available = append(available, addr)
		}
	}
	require.NoError(t, eventually(t, "owner and other broker completed truncate", clusterReadyTimeout, func() (bool, string, error) {
		for _, addr := range available {
			client := e2e.NewBrokerClient([]string{addr})
			response, err := client.SendCommand("", "CLUSTER_STATUS", 5*time.Second)
			client.Close()
			var state struct {
				Pending int `json:"topic_materialization_pending"`
			}
			if err != nil || json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &state) != nil || state.Pending != 0 {
				return false, response, nil
			}
		}
		return true, "other brokers completed local truncate", nil
	}))
	requireCleanupGroupAbsent(t, available)
	logCleanupRecoveryWork(t, []int{delayed}, "before delayed follower restart")
	// A pending truncate rejects replica appends while the node remains in ISR.
	// Take only the delayed test node offline so the healthy pair can commit
	// real new-generation traffic before that node restarts and catches up.
	cleanupBrokerCommand(t, "stop", delayed)
	waitForBrokerEvictedFromISR(t, ctx, delayed)
	fresh, newGeneration, newMember := joinRecoveryGroup(t, available, cleanupRecoveryTopic, cleanupRecoveryGroup)
	defer fresh.Close()
	require.Zero(t, fetchCommittedOffset(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup))
	publishTruncateTarget(t, available, "new-generation-record")
	commitGroupOffset(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup, 1, newGeneration, newMember)
	cleanupBrokerCommand(t, "start", delayed)
	// The fault file survives restart. Wait until the new process reaches the
	// old operation with the newer registration, then release only that fault.
	requireCleanupReadFaultObserved(t, []string{addrs[delayed-1]}, []int{delayed})
	setCleanupReadFault(t, []int{delayed}, false)
	requireClusterStatusMaterializedWithin(t, addrs, clusterReadyTimeout, func() {
		heartbeatGroup(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup, newGeneration, newMember)
	})
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "old truncate preserved new registration")
	require.NoError(t, actions.waitForNodeHealth(delayed, clusterHealthCheckAddrs(3)[delayed-1]))
	heartbeatGroup(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup, newGeneration, newMember)
	require.Equal(t, uint64(1), fetchCommittedOffset(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup))
	status, err := fresh.GetConsumerGroupStatus(cleanupRecoveryGroup)
	require.NoError(t, err)
	t.Logf("preserved active registration: %+v", status)
	logCleanupRecoveryWork(t, []int{1, 2, 3}, "full ISR recovered")
	response, err := fresh.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s generation=%d member=%s", cleanupRecoveryTopic, cleanupRecoveryGroup, newGeneration, newMember), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)
}
