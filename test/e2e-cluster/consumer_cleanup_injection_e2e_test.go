package e2e_cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// Exercise real committed appends and local page reads in three fault-image
// brokers. Unit reader mocks do not establish this integration contract.
func TestCommittedConsumerCleanupLostAcknowledgementAndReadFailure(t *testing.T) {
	if os.Getenv("RUN_E2E_CLEANUP_INJECTION") != "1" {
		t.Skip("set RUN_E2E_CLEANUP_INJECTION=1 to run cleanup injection")
	}
	for _, operation := range []string{"TRUNCATE", "DELETE"} {
		t.Run(operation, func(t *testing.T) {
			ctx := givenCleanupCluster(t, composeFile, faultComposeFile, "docker-compose.consumer-cleanup.yml").
				WithClusterSize(3).WithTopic(cleanupRecoveryTopic).WithPartitions(1).WithAcks("all")
			defer ctx.Cleanup()
			actions := ctx.WhenCluster().StartCluster()
			addrs := ctx.GetBrokerAddrs()
			for _, name := range []string{cleanupRecoveryTopic, cleanupRetainedMessages, cleanupRetainedEvents} {
				command := "CREATE topic=" + name + " partitions=1 replication_factor=3"
				if name == cleanupRetainedEvents {
					command += " aggregate_replay=true"
				}
				sendClusterTopicCommand(t, addrs, command)
				requireClusterDefinitionEventually(t, addrs, name, map[string]string{"revision": "1"})
				requireReplicaOffsetsEventually(t, addrs, name, 0)
			}
			requireClusterStatusMaterialized(t, addrs)
			seedLargeCleanupHistory(t, ctx)
			proof := &cleanupRetainedProof{}
			proof.advance(t, addrs)
			client, generation, member := joinRecoveryGroup(t, addrs, cleanupRecoveryTopic, cleanupRecoveryGroup)
			defer client.Close()
			owner := coordinatorNode(t, findGroupCoordinator(t, 1, cleanupRecoveryGroup))
			for _, node := range survivorNodes(owner) {
				heartbeatGroup(t, client, cleanupRecoveryTopic, cleanupRecoveryGroup, generation, member)
				cleanupBrokerCommand(t, "restart", node)
				require.NoError(t, actions.waitForNodeHealth(node, clusterHealthCheckAddrs(3)[node-1]))
				waitForStableFullISRAndZeroUnderReplicated(t, ctx, "injection follower recovered live membership")
			}
			// Admission runs on the Raft leader. Put the unrelated live session
			// there too so its clock assertion always exercises admission replay.
			raftLeader := waitForStorageFaultRaftLeader(t, 3)
			targetPartition, partitionCount := consumerOffsetPartition(t, addrs, cleanupRecoveryGroup)
			liveGroup := consumerGroupOnDifferentOffsetPartition("cleanup-injection-live", targetPartition, partitionCount)
			livePartition, _ := consumerOffsetPartition(t, addrs, liveGroup)
			requireReplicaPartitionOffsetsConverge(t, addrs, config.ConsumerOffsetsTopicName, livePartition)
			sendClusterTopicCommand(t, addrs, fmt.Sprintf("ELECT_LEADER topic=%s partition=%d broker=broker-%d-9000",
				config.ConsumerOffsetsTopicName, livePartition, raftLeader))
			require.NoError(t, eventually(t, "unrelated session owner is admission broker", clusterReadyTimeout, func() (bool, string, error) {
				for node := 1; node <= 3; node++ {
					id, err := findGroupCoordinatorFrom(node, liveGroup)
					if err != nil || id != fmt.Sprintf("broker-%d-9000", raftLeader) {
						return false, fmt.Sprintf("node=%d owner=%s err=%v", node, id, err), nil
					}
				}
				return true, "route converged", nil
			}))
			require.Equal(t, owner, coordinatorNode(t, findGroupCoordinator(t, 1, cleanupRecoveryGroup)))
			t.Logf("admission broker=%d live group=%s offsets partition=%d target partition=%d", raftLeader, liveGroup, livePartition, targetPartition)
			live, liveGeneration, liveMember := joinRecoveryGroup(t, addrs, cleanupRetainedMessages, liveGroup)
			defer live.Close()
			// Force multiple bounded pages using valid unrelated metadata, not
			// direct storage mutation. The unrelated offset remains live.
			for offset := uint64(1); offset <= 140; offset++ {
				if offset%10 == 1 {
					heartbeatGroup(t, client, cleanupRecoveryTopic, cleanupRecoveryGroup, generation, member)
					heartbeatGroup(t, live, cleanupRetainedMessages, liveGroup, liveGeneration, liveMember)
				}
				commitGroupOffset(t, live, cleanupRetainedMessages, liveGroup, offset, liveGeneration, liveMember)
			}
			requireReplicaPartitionOffsetsConverge(t, addrs, config.ConsumerOffsetsTopicName, livePartition)
			liveState, err := live.GetConsumerGroupStatus(liveGroup)
			require.NoError(t, err)
			require.Len(t, liveState.Members, 1)
			var liveRequestLatency []time.Duration
			defer func() {
				if len(liveRequestLatency) == 0 {
					return
				}
				sort.Slice(liveRequestLatency, func(i, j int) bool { return liveRequestLatency[i] < liveRequestLatency[j] })
				t.Logf("%s unrelated status+heartbeat+offset+status latency samples=%d p50=%s p99=%s max=%s", operation, len(liveRequestLatency), liveRequestLatency[len(liveRequestLatency)/2], liveRequestLatency[(len(liveRequestLatency)-1)*99/100], liveRequestLatency[len(liveRequestLatency)-1])
			}()
			verifyLive := func() {
				started := time.Now()
				defer func() { liveRequestLatency = append(liveRequestLatency, time.Since(started)) }()
				status, err := live.GetConsumerGroupStatus(liveGroup)
				require.NoError(t, err)
				require.Equal(t, liveState.Generation, status.Generation)
				require.Equal(t, liveState.LastRebalance, status.LastRebalance)
				require.Len(t, status.Members, 1)
				require.Equal(t, liveState.Members[0].MemberID, status.Members[0].MemberID)
				require.Equal(t, liveState.Members[0].Assignments, status.Members[0].Assignments)
				require.Equal(t, liveState.Members[0].LastHeartbeat, status.Members[0].LastHeartbeat,
					"admission and cleanup retries must preserve unrelated heartbeat time")
				heartbeatGroup(t, live, cleanupRetainedMessages, liveGroup, liveGeneration, liveMember)
				require.Equal(t, uint64(140), fetchCommittedOffset(t, live, cleanupRetainedMessages, liveGroup))
				liveState, err = live.GetConsumerGroupStatus(liveGroup)
				require.NoError(t, err)
			}
			verifyLive()
			leaveCleanupGroup(t, client, generation, member)
			// Avoid the Raft leader: administrative admission on that broker
			// refreshes its cache and need not retain the stale follower state.
			require.Equal(t, raftLeader, waitForStorageFaultRaftLeader(t, 3), "admission broker must still own the observed live session")
			follower := 0
			for _, node := range survivorNodes(owner) {
				if node != raftLeader {
					follower = node
					break
				}
			}
			require.NotZero(t, follower)
			cleanupFaultFile(t, owner, "arm-ack")
			if os.Getenv("RUN_E2E_CLEANUP_LARGE") == "1" {
				output, err := exec.Command("docker", "exec", cleanupContainerName(follower), "touch", faultSentinelRoot+"/consumer-cleanup-after-large").CombinedOutput()
				require.NoError(t, err, string(output))
			}
			cleanupFaultFile(t, follower, "arm-read")
			command := operation + " topic=" + cleanupRecoveryTopic
			if operation == "TRUNCATE" {
				command += " expected_revision=1"
			}
			response := sendClusterTopicCommand(t, addrs, command)
			t.Logf("committed %s intent: %s; owner=%d read-fault follower=%d", operation, response, owner, follower)
			verifyLive()
			require.NoError(t, eventually(t, "committed tombstone acknowledgement was lost", clusterReadyTimeout, func() (bool, string, error) {
				execCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				out, err := exec.CommandContext(execCtx, "docker", "exec", cleanupContainerName(owner), "test", "-f", faultSentinelRoot+"/consumer-cleanup-ack/"+cleanupRecoveryTopic+".fired").CombinedOutput()
				return err == nil, string(out), nil
			}))
			pending := waitCleanupReadPending(t, addrs[follower-1], 0, verifyLive)
			// The tombstone can arrive after the reader has passed its partition,
			// moving the active retry back from the separate live-group partition.
			// Freeze the comparison only after committed ends converge and a new
			// failed attempt observes them; retain all accumulated-work assertions.
			requireReplicaPartitionOffsetsConverge(t, addrs, config.ConsumerOffsetsTopicName, targetPartition)
			first := waitCleanupReadPending(t, addrs[follower-1], pending.Reads+1, verifyLive)
			require.Equal(t, pending.Records, first.Records)
			require.Equal(t, pending.Bytes, first.Bytes)
			if os.Getenv("RUN_E2E_CLEANUP_LARGE") == "1" {
				require.Greater(t, first.Bytes, uint64(1<<20), "fault must follow validated large-record progress")
				require.GreaterOrEqual(t, first.Records, uint64(2))
			} else {
				require.GreaterOrEqual(t, first.Records, uint64(128))
			}
			require.Positive(t, first.Bytes)
			second := waitCleanupReadPending(t, addrs[follower-1], first.Reads+2, verifyLive)
			require.Equal(t, first.Partition, second.Partition)
			require.Equal(t, first.Next, second.Next)
			require.Equal(t, first.End, second.End)
			require.Equal(t, first.Segment, second.Segment)
			require.Equal(t, first.Position, second.Position)
			require.Equal(t, first.Records, second.Records, "failed retries must not rescan the completed prefix")
			require.Equal(t, first.Bytes, second.Bytes)
			t.Logf("%s pending read retries: records=%d bytes=%d reads=%d->%d partition=%d cursor=%d/%d segment=%d position=%d", operation, first.Records, first.Bytes, first.Reads, second.Reads, first.Partition, first.Next, first.End, first.Segment, first.Position)
			verifyLive()
			proof.verify(t, addrs)
			logCleanupRecoveryWork(t, []int{1, 2, 3}, operation+" fault held")
			cleanupFaultFile(t, follower, "release-read")
			if os.Getenv("RUN_E2E_CLEANUP_LARGE") == "1" {
				cleanupBrokerCommand(t, "restart", follower)
				require.NoError(t, actions.waitForNodeHealth(follower, clusterHealthCheckAddrs(3)[follower-1]))
			}
			requireClusterStatusMaterializedWithin(t, addrs, 2*time.Minute, verifyLive)
			requireCleanupGroupAbsent(t, addrs)
			verifyLive()
			if operation == "TRUNCATE" {
				requireClusterDefinitionEventually(t, addrs, cleanupRecoveryTopic, map[string]string{"revision": "2", "lifecycle_epoch": "2"})
				requireReplicaOffsetsEventually(t, addrs, cleanupRecoveryTopic, 0)
				// Retry the old request: it must not create a second truncate.
				require.Error(t, sendClusterTopicCommandError(t, addrs, command))
				requireClusterDefinitionEventually(t, addrs, cleanupRecoveryTopic, map[string]string{"revision": "2"})
			} else {
				for node := 1; node <= 3; node++ {
					require.NoError(t, actions.WaitForLocalTopicDefinition(node, LocalTopicDefinition{Topic: cleanupRecoveryTopic}, false))
				}
				retry := sendClusterTopicCommand(t, addrs, command+" if_exists=true")
				require.Contains(t, retry, "deleted=false")
			}
			verifyLive()
			proof.advance(t, addrs)
			cleanupFaultFile(t, owner, "assert-single-append")
			logCleanupRecoveryWork(t, []int{1, 2, 3}, operation+" faults recovered")
		})
	}
}

func waitCleanupReadPending(t *testing.T, addr string, minimumReads uint64, verifyLive func()) coordinator.TopicCleanupPending {
	t.Helper()
	var pending coordinator.TopicCleanupPending
	require.NoError(t, eventually(t, "scoped cleanup page-read failure remains pending", 2*time.Minute, func() (bool, string, error) {
		// Error retries back off independently. Keep the unrelated session alive
		// while still checking that replay itself never changes its heartbeat.
		verifyLive()
		client := e2e.NewBrokerClient([]string{addr})
		response, err := client.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
		client.Close()
		if err != nil {
			return false, "cluster status unavailable", err
		}
		var status struct {
			Pending []struct {
				ConsumerCleanup *coordinator.TopicCleanupPending `json:"consumer_cleanup"`
			} `json:"topic_materialization_issues"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status); err != nil {
			return false, response, err
		}
		for _, issue := range status.Pending {
			if p := issue.ConsumerCleanup; p != nil && p.Topic == cleanupRecoveryTopic && strings.Contains(p.Reason, "injected transient consumer cleanup page read failure") && p.Reads >= minimumReads {
				pending = *p
				return true, p.Error(), nil
			}
		}
		return false, response, nil
	}))
	return pending
}

// Commands contain only fixed test-owned paths; never accept arbitrary shell
// text or modify shared storage. Container teardown removes the sentinels.
func cleanupFaultFile(t *testing.T, node int, operation string) {
	t.Helper()
	ack := faultSentinelRoot + "/consumer-cleanup-ack/" + cleanupRecoveryTopic
	commands := map[string]string{
		"arm-ack":              "mkdir -p " + faultSentinelRoot + "/consumer-cleanup-ack && touch " + ack,
		"arm-read":             "touch " + faultSentinelRoot + "/consumer-cleanup-read",
		"release-read":         "rm " + faultSentinelRoot + "/consumer-cleanup-read",
		"assert-single-append": "test -f " + ack + ".fired && test ! -e " + ack + ".duplicate",
	}
	command, ok := commands[operation]
	require.True(t, ok)
	execCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(execCtx, "docker", "exec", cleanupContainerName(node), "sh", "-c", command).CombinedOutput()
	require.NoError(t, err, string(out))
}
