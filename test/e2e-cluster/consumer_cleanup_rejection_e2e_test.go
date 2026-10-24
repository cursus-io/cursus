package e2e_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// Reject bad read evidence on a real stale follower while the other replicas
// complete the committed intent. No invalid record is written into storage.
func TestConsumerCleanupRejectsInvalidEvidenceOnThreeBrokers(t *testing.T) {
	if os.Getenv("RUN_E2E_CLEANUP_REJECTION") != "1" {
		t.Skip("set RUN_E2E_CLEANUP_REJECTION=1")
	}
	ctx := givenCleanupCluster(t, composeFile, faultComposeFile, "docker-compose.consumer-cleanup.yml").WithClusterSize(3).WithTopic(cleanupRecoveryTopic).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	addrs := ctx.GetBrokerAddrs()
	for _, name := range []string{cleanupRetainedMessages, cleanupRetainedEvents} {
		command := "CREATE topic=" + name + " partitions=1 replication_factor=3"
		if name == cleanupRetainedEvents {
			command += " aggregate_replay=true"
		}
		sendClusterTopicCommand(t, addrs, command)
		requireReplicaOffsetsEventually(t, addrs, name, 0)
	}
	proof := &cleanupRetainedProof{}
	proof.advance(t, addrs)
	for _, mode := range []string{"missing", "malformed", "conflicting", "uncommitted", "obsolete", "transactional"} {
		if !t.Run(mode, func(t *testing.T) {
			sendClusterTopicCommand(t, addrs, "CREATE topic="+cleanupRecoveryTopic+" partitions=1 replication_factor=3")
			requireReplicaOffsetsEventually(t, addrs, cleanupRecoveryTopic, 0)
			client, generation, member := joinRecoveryGroup(t, addrs, cleanupRecoveryTopic, cleanupRecoveryGroup)
			defer client.Close()
			require.NoError(t, client.PublishIdempotentToPartition(cleanupRecoveryTopic, "recreated-producer", 0, 1, 0, "new-instance-record", "all", true))
			requireReplicaOffsetsEventually(t, addrs, cleanupRecoveryTopic, 1)
			owner := coordinatorNode(t, findGroupCoordinator(t, 1, cleanupRecoveryGroup))
			raftLeader := waitForStorageFaultRaftLeader(t, 3)
			follower := 0
			for _, n := range survivorNodes(owner) {
				if n != raftLeader {
					follower = n
					break
				}
			}
			require.NotZero(t, follower)
			cleanupBrokerCommand(t, "restart", follower)
			require.NoError(t, actions.waitForNodeHealth(follower, clusterHealthCheckAddrs(3)[follower-1]))
			waitForStableFullISRAndZeroUnderReplicated(t, ctx, "negative matrix stale follower")
			requireReplicaOffsetsEventually(t, addrs, cleanupRecoveryTopic, 1)
			heartbeatGroup(t, client, cleanupRecoveryTopic, cleanupRecoveryGroup, generation, member)
			epochResponse, err := client.SendCommand("admin", "FETCH_OFFSET topic="+cleanupRecoveryTopic+" partition=0 group="+cleanupRecoveryGroup+" include_found=true include_epoch=true", 15*time.Second)
			require.NoError(t, err, epochResponse)
			var registrationEpoch uint64
			for _, field := range strings.Fields(epochResponse) {
				if strings.HasPrefix(field, "registration_epoch=") {
					registrationEpoch, err = strconv.ParseUint(strings.TrimPrefix(field, "registration_epoch="), 10, 64)
					require.NoError(t, err)
				}
			}
			require.NotZero(t, registrationEpoch)
			const path = "/run/cursus-e2e-faults/consumer-cleanup-evidence.json"
			control, err := json.Marshal(map[string]any{"Group": cleanupRecoveryGroup, "Mode": mode, "Epoch": registrationEpoch + 1})
			require.NoError(t, err)
			cmd := exec.Command("docker", "exec", "-i", cleanupContainerName(follower), "sh", "-c", "cat > "+path)
			cmd.Stdin = strings.NewReader(string(control))
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, string(out))
			leaveCleanupGroup(t, client, generation, member)
			sendClusterTopicCommand(t, addrs, "TRUNCATE topic="+cleanupRecoveryTopic+" expected_revision=1")
			local := e2e.NewBrokerClient([]string{addrs[follower-1]})
			defer local.Close()
			var pending coordinator.TopicCleanupPending
			rejection := map[string]string{
				"missing":       "waiting for a newer committed group tombstone",
				"malformed":     "malformed consumer metadata",
				"conflicting":   "conflicting lifecycle records",
				"uncommitted":   "metadata record outside committed page",
				"obsolete":      "waiting for a newer committed group tombstone",
				"transactional": "transactional consumer lifecycle evidence",
			}[mode]
			require.NoError(t, eventually(t, "invalid evidence keeps follower pending", clusterReadyTimeout, func() (bool, string, error) {
				response, err := local.SendCommand("admin", "CLUSTER_STATUS", 5*time.Second)
				if err != nil {
					return false, response, nil
				}
				var status struct {
					Issues []struct {
						Cleanup *coordinator.TopicCleanupPending `json:"consumer_cleanup"`
					} `json:"topic_materialization_issues"`
				}
				if err := json.Unmarshal([]byte(strings.TrimPrefix(response, "OK cluster=")), &status); err != nil {
					return false, response, err
				}
				for _, issue := range status.Issues {
					if p := issue.Cleanup; p != nil && p.Topic == cleanupRecoveryTopic && p.Reads > 0 && strings.Contains(p.Reason, rejection) {
						if output, err := exec.Command("docker", "exec", cleanupContainerName(follower), "cat", path+".observed").CombinedOutput(); err == nil {
							pending = *p
							return true, string(output), nil
						}
					}
				}
				return false, response, nil
			}))
			require.Equal(t, registrationEpoch, pending.Epoch)
			for range 3 {
				_, err := local.SendCommand("", "LIST_OFFSETS topic="+cleanupRecoveryTopic+" partition=0", 5*time.Second)
				require.Error(t, err, "an invalid-evidence follower must not publish completion")
				proof.verify(t, addrs)
				time.Sleep(2 * time.Second)
			}
			t.Logf("mode=%s node=%d pending=%+v", mode, follower, pending)
			out, err = exec.Command("docker", "exec", cleanupContainerName(follower), "rm", path, path+".observed").CombinedOutput()
			require.NoError(t, err, string(out))
			// Restore the honest view via normal recovery; never clear checkpoints
			// or authoritative metadata to make a corrupted view look valid.
			cleanupBrokerCommand(t, "restart", follower)
			require.NoError(t, actions.waitForNodeHealth(follower, clusterHealthCheckAddrs(3)[follower-1]))
			requireClusterStatusMaterialized(t, addrs)
			requireReplicaOffsetsEventually(t, addrs, cleanupRecoveryTopic, 0)
			requireCleanupGroupAbsent(t, addrs)
			proof.advance(t, addrs)
			sendClusterTopicCommand(t, addrs, "DELETE topic="+cleanupRecoveryTopic)
			for node := 1; node <= 3; node++ {
				require.NoError(t, actions.WaitForLocalTopicDefinition(node, LocalTopicDefinition{Topic: cleanupRecoveryTopic}, false))
			}
			requireClusterStatusMaterialized(t, addrs)
		}) {
			return
		}
	}
}

// Routed admin calls must preserve both active and inactive shared/pattern
// subscriptions. Exercise every seed broker, not only the owning coordinator.
func TestTopicLifecycleRejectsSubscriptionsOnThreeBrokers(t *testing.T) {
	if os.Getenv("RUN_E2E_CLEANUP_REJECTION") != "1" {
		t.Skip("set RUN_E2E_CLEANUP_REJECTION=1")
	}
	ctx := givenCleanupCluster(t, composeFile, "docker-compose.consumer-cleanup.yml").WithClusterSize(3).WithTopic("guard-orders").WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	ctx.WhenCluster().StartCluster()
	addrs := ctx.GetBrokerAddrs()
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	for _, mode := range []string{"single", "shared", "pattern"} {
		orders, payments := "guard-"+mode+"-orders", "guard-"+mode+"-payments"
		for _, name := range []string{orders, payments} {
			sendClusterTopicCommand(t, addrs, "CREATE topic="+name+" partitions=1 replication_factor=3")
			requireReplicaOffsetsEventually(t, addrs, name, 0)
			require.NoError(t, client.PublishIdempotentToPartition(name, "guard-producer", 0, 1, 0, "preserve", "all", true))
		}
		group := "guard-" + mode
		registration := "REGISTER_GROUP group=" + group + " topic=" + orders
		if mode == "shared" {
			registration = "REGISTER_GROUP group=" + group + " topics=" + orders + "," + payments
		}
		if mode == "pattern" {
			registration = "REGISTER_GROUP group=" + group + " pattern=guard-" + mode + "-*"
		}
		response, err := client.SendCommand("", registration, 15*time.Second)
		require.NoError(t, err, response)
		response, err = client.SendCommand("", "JOIN_GROUP topic="+orders+" group="+group+" member=guard-member", 15*time.Second)
		require.NoError(t, err, response)
		before, err := client.GetConsumerGroupStatus(group)
		require.NoError(t, err)
		require.Len(t, before.Members, 1)
		check := func() {
			for _, addr := range addrs {
				seed := e2e.NewBrokerClient([]string{addr})
				for _, operation := range []string{"TRUNCATE topic=" + orders + " expected_revision=1", "DELETE topic=" + orders} {
					_, err := seed.SendCommand("admin", operation, 15*time.Second)
					require.Error(t, err, "%s via %s", operation, addr)
					require.Contains(t, err.Error(), "blocked")
				}
				seed.Close()
			}
			requireClusterDefinitionEventually(t, addrs, orders, map[string]string{"revision": "1", "lifecycle_epoch": "1"})
			requireReplicaOffsetsEventually(t, addrs, orders, 1)
			requireReplicaOffsetsEventually(t, addrs, payments, 1)
		}
		check()
		after, err := client.GetConsumerGroupStatus(group)
		require.NoError(t, err)
		require.Equal(t, before.Generation, after.Generation)
		require.Equal(t, before.Members, after.Members)
		response, err = client.SendCommand("", fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d", orders, group, before.Members[0].MemberID, before.Generation), 15*time.Second)
		require.NoError(t, err, response)
		if mode != "single" {
			check()
		}
		t.Logf("mode=%s all seeds rejected; active membership and data preserved", mode)
	}
}
