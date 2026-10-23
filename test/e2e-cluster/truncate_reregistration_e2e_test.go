package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
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
	for _, mode := range []string{"raft-replay", "snapshot-restore"} {
		t.Run(mode, func(t *testing.T) { runTruncateReregistrationRecovery(t, mode == "snapshot-restore") })
	}
}

func runTruncateReregistrationRecovery(t *testing.T, snapshot bool) {
	files := []string{composeFile}
	if snapshot {
		files = append(files, snapshotComposeFile)
	}
	files = append(files, "docker-compose.consumer-cleanup.yml", "docker-compose.consumer-cleanup-recovery.yml")
	ctx := givenCleanupCluster(t, files...).WithClusterSize(3).WithTopic(cleanupRecoveryTopic).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	addrs := ctx.GetBrokerAddrs()
	sendClusterTopicCommand(t, addrs, "CREATE topic="+cleanupRecoveryTopic+" partitions=1 replication_factor=3 idempotent=true")
	requireClusterDefinitionEventually(t, addrs, cleanupRecoveryTopic, map[string]string{"revision": "1"})
	requireClusterStatusMaterialized(t, addrs)
	old, generation, member := joinTruncateReregistrationGroup(t, addrs)
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
	// Hold the target's local open across restart so metadata recovery installs
	// the new registration before final topic cleanup can inspect local groups.
	// This prevents an empty startup overlay from taking the no-reference path.
	setTruncateTargetOpenFault(t, delayed, true)
	// A pending truncate rejects replica appends while the node remains in ISR.
	// Take only the delayed test node offline so the healthy pair can commit
	// real new-generation traffic before that node restarts and catches up.
	cleanupBrokerCommand(t, "stop", delayed)
	waitForBrokerEvictedFromISR(t, ctx, delayed)
	fresh, newGeneration, newMember := joinTruncateReregistrationGroup(t, available)
	defer fresh.Close()
	require.Zero(t, fetchCommittedOffset(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup))
	publishTruncateTarget(t, available, "new-generation-record")
	commitGroupOffset(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup, 1, newGeneration, newMember)
	cleanupBrokerCommand(t, "start", delayed)
	require.NoError(t, eventually(t, "new group recovered before local truncate", clusterReadyTimeout, func() (bool, string, error) {
		heartbeatGroup(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup, newGeneration, newMember)
		_, body, err := readReadiness(delayed)
		var report struct {
			Checks map[string]string `json:"checks"`
		}
		if err != nil || json.Unmarshal([]byte(body), &report) != nil {
			return false, fmt.Sprint(err), nil
		}
		pending := report.Checks["topic_materialization"]
		return report.Checks["consumer_metadata"] == "ok" &&
			(strings.Contains(pending, "handler-open") || strings.Contains(pending, "consumer cleanup")), body, nil
	}))
	setTruncateTargetOpenFault(t, delayed, false)
	require.NoError(t, eventually(t, "old truncate reaches recovered group", clusterReadyTimeout, func() (bool, string, error) {
		heartbeatGroup(t, fresh, cleanupRecoveryTopic, cleanupRecoveryGroup, newGeneration, newMember)
		_, body, err := readReadiness(delayed)
		return err == nil && strings.Contains(body, "consumer cleanup"), body, nil
	}))
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
	require.Equal(t, newGeneration, status.Generation)
	require.Equal(t, 1, status.MemberCount)
	require.Len(t, status.Members, 1)
	require.Equal(t, newMember, status.Members[0].MemberID)
	t.Logf("preserved active registration: %+v", status)
	logCleanupRecoveryWork(t, []int{1, 2, 3}, "full ISR recovered")
	response, err := fresh.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s generation=%d member=%s", cleanupRecoveryTopic, cleanupRecoveryGroup, newGeneration, newMember), 15*time.Second)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(response, "OK"), response)
}

func setTruncateTargetOpenFault(t *testing.T, node int, enabled bool) {
	t.Helper()
	dir := "/run/cursus-e2e-faults/handler-open"
	output, err := exec.Command("docker", "exec", cleanupContainerName(node), "mkdir", "-p", dir).CombinedOutput()
	require.NoError(t, err, string(output))
	operation := "touch"
	if !enabled {
		operation = "rm"
	}
	output, err = exec.Command("docker", "exec", cleanupContainerName(node), operation, dir+"/"+cleanupRecoveryTopic).CombinedOutput()
	require.NoError(t, err, string(output))
}

// Fault recovery includes coordinator movement and snapshot installation. Use
// the recovery fixture's 15s command deadline, including Wire negotiation, rather
// than the general client helper's 2s deadline for a healthy broker.
func joinTruncateReregistrationGroup(t *testing.T, addrs []string) (*e2e.BrokerClient, int, string) {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	member := fmt.Sprintf("truncate-recovery-%d", time.Now().UnixNano())
	client.SetMemberID(member)
	response, err := client.SendCommand("", fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=%s", cleanupRecoveryTopic, cleanupRecoveryGroup, member), 15*time.Second)
	var generation int
	if err != nil && strings.Contains(err.Error(), "write succeeded but read failed") {
		generation, member = recoverAmbiguousGroupMember(t, client, cleanupRecoveryGroup, member)
	} else {
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(response, "OK"), response)
		for _, field := range strings.Fields(response) {
			if strings.HasPrefix(field, "generation=") {
				_, err = fmt.Sscanf(field, "generation=%d", &generation)
				require.NoError(t, err)
			}
			if strings.HasPrefix(field, "member=") {
				member = strings.TrimPrefix(field, "member=")
			}
		}
	}
	client.SetMemberID(member)
	require.NoError(t, eventually(t, "sync re-registered group", clusterReadyTimeout, func() (bool, string, error) {
		_, err := client.SyncGroup(cleanupRecoveryTopic, cleanupRecoveryGroup, generation, member)
		return err == nil, fmt.Sprint(err), nil
	}))
	return client, generation, member
}
