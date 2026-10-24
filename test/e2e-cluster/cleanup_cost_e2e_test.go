package e2e_cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

type cleanupTrafficResult struct {
	Count     int     `json:"acknowledged"`
	Latencies []int64 `json:"publish_commit_heartbeat_ns"`
	Err       error   `json:"-"`
}

// Profile the entire live TRUNCATE operation, including a held read fault and
// convergence of every replica. Startup/setup is explicitly outside this phase.
func TestTruncateRecoveryCostWithForegroundTraffic(t *testing.T) {
	if os.Getenv("RUN_E2E_CLEANUP_PROFILE") != "1" {
		t.Skip("set RUN_E2E_CLEANUP_PROFILE=1")
	}
	setupStart := time.Now()
	ctx := givenCleanupCluster(t, composeFile, faultComposeFile, "docker-compose.consumer-cleanup.yml", "docker-compose.cleanup-profile.yml").WithClusterSize(3).WithTopic(cleanupRecoveryTopic).WithPartitions(1).WithAcks("all")
	defer ctx.Cleanup()
	actions := ctx.WhenCluster().StartCluster()
	addrs := ctx.GetBrokerAddrs()
	for _, name := range []string{cleanupRecoveryTopic, "profile-traffic"} {
		sendClusterTopicCommand(t, addrs, "CREATE topic="+name+" partitions=1 replication_factor=3")
		requireReplicaOffsetsEventually(t, addrs, name, 0)
	}
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	for i := 1; i <= 100; i++ {
		require.NoError(t, client.PublishIdempotentToPartition(cleanupRecoveryTopic, "profile-reset", 0, uint64(i), 0, fmt.Sprintf("old-%d", i), "all", true))
	}
	target, generation, member := joinRecoveryGroup(t, addrs, cleanupRecoveryTopic, cleanupRecoveryGroup)
	defer target.Close()
	commitGroupOffset(t, target, cleanupRecoveryTopic, cleanupRecoveryGroup, 100, generation, member)
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
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "profile stale follower ready")
	heartbeatGroup(t, target, cleanupRecoveryTopic, cleanupRecoveryGroup, generation, member)
	leaveCleanupGroup(t, target, generation, member)
	live, liveGeneration, liveMember := joinRecoveryGroup(t, addrs, "profile-traffic", "profile-live")
	defer live.Close()
	for i := 0; i < 256; i++ {
		commitGroupOffset(t, live, "profile-traffic", "profile-live", 0, liveGeneration, liveMember)
	}
	beforeLive, err := live.GetConsumerGroupStatus("profile-live")
	require.NoError(t, err)
	setCleanupReadFault(t, []int{follower}, true)
	stopTraces := make([]func() string, 3)
	traceStarted, traceStopped := make([]time.Time, 3), make([]time.Time, 3)
	before := make([]cleanupProfileSample, 3)
	for node := 1; node <= 3; node++ {
		traceStarted[node-1] = time.Now().UTC()
		stopTraces[node-1] = startCleanupSyncTrace(t, node)
		before[node-1] = sampleCleanupProfile(t, node)
	}
	setupDuration := time.Since(setupStart)
	stopTraffic := make(chan struct{})
	done := make(chan cleanupTrafficResult, 1)
	go func() {
		result := cleanupTrafficResult{}
		for {
			started := time.Now()
			n := result.Count + 1
			err := client.PublishIdempotentToPartition("profile-traffic", "profile-writer", 0, uint64(n), 0, fmt.Sprintf("live-%d", n), "all", true)
			if err == nil {
				_, err = live.SendCommand("", fmt.Sprintf("COMMIT_OFFSET topic=profile-traffic partition=0 group=profile-live offset=%d generation=%d member=%s", n, liveGeneration, liveMember), 10*time.Second)
			}
			if err == nil {
				_, err = live.SendCommand("", fmt.Sprintf("HEARTBEAT topic=profile-traffic group=profile-live generation=%d member=%s", liveGeneration, liveMember), 10*time.Second)
			}
			if err != nil {
				result.Err = err
				done <- result
				return
			}
			result.Count = n
			result.Latencies = append(result.Latencies, time.Since(started).Nanoseconds())
			select {
			case <-stopTraffic:
				done <- result
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	// A sampler keeps phase peaks separate from lifetime process high-watermarks.
	stopSamples := make(chan struct{})
	samplesDone := make(chan []map[string]float64, 1)
	go func() {
		peaks := []map[string]float64{{}, {}, {}}
		for {
			for node := 1; node <= 3; node++ {
				if values, err := readCleanupProfileMetrics(node); err == nil {
					for _, key := range []string{"process_resident_memory_bytes", "go_memstats_heap_alloc_bytes"} {
						if value, present := values[key]; present {
							peaks[node-1][key] = max(peaks[node-1][key], value)
						}
					}
				}
			}
			select {
			case <-stopSamples:
				samplesDone <- peaks
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	}()
	// Even a failing assertion joins background traffic before cluster teardown.
	trafficStopped, samplesStopped := false, false
	defer func() {
		if !trafficStopped {
			close(stopTraffic)
			<-done
		}
		if !samplesStopped {
			close(stopSamples)
			<-samplesDone
		}
	}()
	operationStart := time.Now().UTC()
	response := sendClusterTopicCommand(t, addrs, "TRUNCATE topic="+cleanupRecoveryTopic+" expected_revision=1")
	intentAck := time.Now().UTC()
	local := e2e.NewBrokerClient([]string{addrs[follower-1]})
	defer local.Close()
	var pending coordinator.TopicCleanupPending
	require.NoError(t, eventually(t, "profile sees repeated held read attempts", clusterReadyTimeout, func() (bool, string, error) {
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
			if p := issue.Cleanup; p != nil && p.Topic == cleanupRecoveryTopic && p.Reads >= 3 && strings.Contains(p.Reason, "injected consumer cleanup read failure") {
				pending = *p
				return true, p.Error(), nil
			}
		}
		return false, response, nil
	}))
	held := make([]cleanupProfileSample, 3)
	for node := 1; node <= 3; node++ {
		held[node-1] = sampleCleanupProfile(t, node)
	}
	releaseStart := time.Now().UTC()
	setCleanupReadFault(t, []int{follower}, false)
	requireClusterStatusMaterialized(t, addrs)
	requireReplicaOffsetsEventually(t, addrs, cleanupRecoveryTopic, 0)
	recoveredAt := time.Now().UTC()
	close(stopTraffic)
	traffic := <-done
	trafficStopped = true
	require.NoError(t, traffic.Err)
	require.Positive(t, traffic.Count)
	close(stopSamples)
	peaks := <-samplesDone
	samplesStopped = true
	after := make([]cleanupProfileSample, 3)
	syncs := make([]string, 3)
	for node := 1; node <= 3; node++ {
		after[node-1] = sampleCleanupProfile(t, node)
		syncs[node-1] = stopTraces[node-1]()
		traceStopped[node-1] = time.Now().UTC()
	}
	writeCleanupProfile(t, map[string]any{"schema": 1, "setup_ns": setupDuration.Nanoseconds(), "operation_start": operationStart, "intent_ack": intentAck, "release_start": releaseStart, "all_materialized": recoveredAt, "recovery_ns": recoveredAt.Sub(releaseStart).Nanoseconds(), "response": response, "pending": pending, "before": before, "held": held, "after": after, "sync_trace": syncs, "trace_started": traceStarted, "trace_stopped": traceStopped, "sampled_peaks": peaks, "traffic": traffic})
	// Verification is outside the measured reset and requires useful fresh work.
	requireCleanupGroupAbsent(t, addrs)
	require.Equal(t, uint64(traffic.Count), fetchCommittedOffset(t, live, "profile-traffic", "profile-live"))
	afterLive, err := live.GetConsumerGroupStatus("profile-live")
	require.NoError(t, err)
	require.Equal(t, beforeLive.Generation, afterLive.Generation)
	require.Equal(t, beforeLive.Members[0].MemberID, afterLive.Members[0].MemberID)
	admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: addrs, MaxRetries: 4, RequestTimeoutMS: 15000})
	require.NoError(t, err)
	var offset uint64
	for offset < uint64(traffic.Count) {
		page, err := admin.BrowseMessages(context.Background(), sdk.BrowseRequest{Topic: "profile-traffic", Partition: 0, FromOffset: offset, MaxRecords: 128, MaxBytes: 65536})
		require.NoError(t, err)
		require.NotEmpty(t, page.Messages)
		for _, m := range page.Messages {
			require.Equal(t, fmt.Sprintf("live-%d", offset+1), m.Payload)
			offset++
		}
	}
	require.Equal(t, uint64(traffic.Count), offset)
	require.NoError(t, client.PublishIdempotentToPartition(cleanupRecoveryTopic, "profile-reset", 0, 1, 0, "fresh", "all", true))
	requireReplicaOffsetsEventually(t, addrs, cleanupRecoveryTopic, 1)
}
