package e2e_cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func publishTruncateTarget(t *testing.T, addrs []string, payload string) {
	t.Helper()
	client := e2e.NewBrokerClient(addrs)
	defer client.Close()
	response, err := client.SendCommand("", fmt.Sprintf("PUBLISH topic=%s partition=0 acks=all producerId=reset-producer seqNum=1 epoch=1 isIdempotent=true message=%s", cleanupRecoveryTopic, payload), 15*time.Second)
	require.NoError(t, err, "publish broker detail: %#v", err)
	var result struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal([]byte(response), &result))
	require.Equal(t, "OK", result.Status)
}

func startTruncateConcurrentTraffic(t *testing.T, addrs []string) func() int {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	type result struct {
		count int
		err   error
	}
	done := make(chan result, 1)
	ready := make(chan error, 1)
	go func() {
		client := e2e.NewBrokerClient(addrs)
		defer client.Close()
		count := 0
		for {
			err := client.PublishIdempotentToPartition("cleanup-concurrent", "traffic-writer", 0, uint64(count+1), 1, fmt.Sprintf("traffic-%d", count+1), "all", true)
			if count == 0 {
				ready <- err
			}
			if err != nil {
				done <- result{count, err}
				return
			}
			count++
			select {
			case <-ctx.Done():
				done <- result{count, nil}
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	require.NoError(t, <-ready, "concurrent producer must start before truncate")
	return func() int {
		cancel()
		outcome := <-done
		require.NoError(t, outcome.err)
		require.Positive(t, outcome.count)
		t.Logf("acknowledged concurrent records=%d", outcome.count)
		return outcome.count
	}
}

func verifyTruncateConcurrentTraffic(t *testing.T, addrs []string, count int) {
	t.Helper()
	admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: addrs, MaxRetries: 5, RequestTimeoutMS: 15000})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The concurrent window is short; bound the request and fail if it outgrows
	// the verified range instead of silently checking only a prefix.
	require.LessOrEqual(t, count, 256)
	page, err := admin.BrowseMessages(ctx, sdk.BrowseRequest{Topic: "cleanup-concurrent", Partition: 0, MaxRecords: 256, MaxBytes: 65536})
	require.NoError(t, err)
	require.Len(t, page.Messages, count)
	for i, message := range page.Messages {
		require.Equal(t, fmt.Sprintf("traffic-%d", i+1), message.Payload)
	}
}

func verifyTruncateReceiptAfterRaftLeaderLoss(t *testing.T, ctx *ClusterTestContext, actions *ClusterActions, options sdk.TruncateTopicOptions) {
	t.Helper()
	findLeader := func(nodes []int) int {
		leader := 0
		require.NoError(t, eventually(t, "one Raft leader", 2*clusterReadyTimeout, func() (bool, string, error) {
			candidate, count := 0, 0
			for _, node := range nodes {
				status, err := actions.LocalClusterStatus(node)
				if err != nil {
					return false, err.Error(), nil
				}
				if status.RaftState == "Leader" {
					candidate, count = node, count+1
				}
			}
			if count == 1 {
				leader = candidate
			}
			return count == 1, fmt.Sprint(candidate), nil
		}))
		require.NotZero(t, leader)
		return leader
	}
	oldLeader := findLeader([]int{1, 2, 3})
	cleanupBrokerCommand(t, "stop", oldLeader)
	survivors := survivorNodes(oldLeader)
	newLeader := findLeader(survivors)
	require.NotEqual(t, oldLeader, newLeader)
	for _, node := range survivors {
		if node == newLeader {
			continue
		}
		admin, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: []string{ctx.GetBrokerAddrs()[node-1]}, MaxRetries: 5, RequestTimeoutMS: 15000})
		require.NoError(t, err)
		result, err := admin.TruncateTopic(cleanupRecoveryTopic, options)
		require.NoError(t, err)
		require.True(t, result.Replayed, "follower routes receipt retry after Raft leader loss")
	}
	t.Logf("Raft leader failover proven: old=%d new=%d", oldLeader, newLeader)
	cleanupBrokerCommand(t, "start", oldLeader)
	require.NoError(t, actions.waitForNodeHealth(oldLeader, clusterHealthCheckAddrs(3)[oldLeader-1]))
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "Raft leader receipt replay")
	requireClusterStatusMaterialized(t, ctx.GetBrokerAddrs())
}
