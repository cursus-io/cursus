package e2e_cluster

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

func TestChaosSDKObservationSurvivesLeaderRestart(t *testing.T) {
	if os.Getenv("RUN_E2E_CHAOS") != "1" {
		t.Skip("set RUN_E2E_CHAOS=1 to run SDK observation failover validation")
	}
	ctx := GivenClusterRestart(t).WithClusterSize(3).WithTopic("sdk-observation-failover").WithPartitions(1)
	actions := ctx.WhenCluster().StartCluster()
	_, err := ctx.GetClient().SendCommand("", "CREATE topic="+ctx.GetTopic()+" partitions=1 replication_factor=3 event_sourcing=true", 5*time.Second)
	require.NoError(t, err)
	actions.WaitForTopicMetadata()
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "observation topic creation")
	description, err := ctx.GetClient().SendCommand("", "DESCRIBE topic="+ctx.GetTopic(), 5*time.Second)
	require.NoError(t, err)
	leader, err := leaderNodeFromDescribe(description, 3)
	require.NoError(t, err)
	writer := e2e.NewBrokerClient([]string{ctx.GetBrokerAddrs()[leader-1]})
	defer writer.Close()
	for version := 1; version <= 2; version++ {
		_, err := writer.SendCommand("", fmt.Sprintf("APPEND_STREAM topic=%s key=order version=%d message=event-%d", ctx.GetTopic(), version, version), 5*time.Second)
		require.NoError(t, err)
	}

	verify := func(excluded int) {
		t.Helper()
		for i, addr := range ctx.GetBrokerAddrs() {
			if i+1 == excluded {
				continue
			}
			// A single seed forces followers to return a usable advertised leader.
			client, err := sdk.NewAdminClient(&sdk.AdminConfig{BrokerAddrs: []string{addr}, RequestTimeoutMS: 3000, MaxRetries: 5, RetryBackoffMS: 100})
			require.NoError(t, err)
			requestCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			t.Cleanup(cancel)
			capabilities, err := client.Capabilities(requestCtx)
			require.NoError(t, err, "seed=%s", addr)
			require.Equal(t, 2, capabilities.Version)
			browse, err := client.BrowseMessages(requestCtx, sdk.BrowseRequest{Topic: ctx.GetTopic(), Partition: 0, MaxRecords: 10, MaxBytes: 4096})
			require.NoError(t, err, "seed=%s", addr)
			require.Len(t, browse.Messages, 2)
			require.Equal(t, "event-1", browse.Messages[0].Payload)
			require.Equal(t, "event-2", browse.Messages[1].Payload)
			first, err := client.ReadStreamHistory(requestCtx, sdk.HistoryRequest{Topic: ctx.GetTopic(), Key: "order", FromVersion: 1, MaxRecords: 1, MaxBytes: 4096})
			require.NoError(t, err, "seed=%s", addr)
			require.Len(t, first.Events, 1)
			require.Equal(t, "event-1", first.Events[0].Payload)
			require.True(t, first.HasMore)
			require.Equal(t, sdk.HistoryComplete, first.Completeness)
			second, err := client.ReadStreamHistory(requestCtx, sdk.HistoryRequest{Topic: ctx.GetTopic(), Key: "order", FromVersion: first.NextVersion, MaxRecords: 1, MaxBytes: 4096})
			cancel()
			require.NoError(t, err, "seed=%s", addr)
			require.Len(t, second.Events, 1)
			require.Equal(t, "event-2", second.Events[0].Payload)
			require.False(t, second.HasMore)
			require.Equal(t, sdk.HistoryComplete, second.Completeness)
		}
	}
	verify(0)
	stopped, _ := actions.SimulateLeaderFailure()
	verify(stopped)
	actions.StartBroker(stopped)
	actions.WaitForTopicMetadata()
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "observation leader restart")
	verify(0)
}
