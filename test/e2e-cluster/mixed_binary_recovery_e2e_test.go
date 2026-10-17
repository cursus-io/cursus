package e2e_cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// The compose build must initially contain the earlier broker binary. The new
// binary is copied into each isolated test container without replacing its data.
func TestMixedBinaryRollingRecoveryPreservesQuorumWrites(t *testing.T) {
	if os.Getenv("RUN_E2E_MIXED_BINARY") != "1" {
		t.Skip("set RUN_E2E_MIXED_BINARY=1 with an old compose image and CURSUS_E2E_NEW_BROKER_BINARY")
	}
	binary := os.Getenv("CURSUS_E2E_NEW_BROKER_BINARY")
	data, err := os.ReadFile(binary)
	require.NoError(t, err)
	expected := sha256.Sum256(data)
	ctx := GivenClusterRestart(t).WithTopic("mixed-binary-recovery").WithPartitions(1).WithAcks("all").WithIdempotent(true)
	actions := ctx.WhenCluster().StartCluster().CreateTopic().WaitForTopicMetadata()
	client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
	defer client.Close()
	assertCapabilities := func(upgraded int) {
		t.Helper()
		require.NoError(t, eventually(t, "mixed process capabilities", clusterReadyTimeout, func() (bool, string, error) {
			response, err := client.SendCommand("", "LIST_CLUSTER", 3*time.Second)
			if err != nil {
				return false, "registration unavailable", nil
			}
			var brokers []fsm.BrokerInfo
			if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(response), "OK brokers=")), &brokers); err != nil {
				return false, response, err
			}
			current, legacy := 0, 0
			for _, broker := range brokers {
				if broker.Status != "active" {
					return false, "inactive registration", nil
				}
				if broker.RecoveryProtocol >= 2 {
					current++
				} else if broker.RecoveryProtocol == 0 {
					legacy++
				}
			}
			return current == upgraded && legacy == 3-upgraded, fmt.Sprintf("current=%d legacy=%d", current, legacy), nil
		}))
	}
	assertCapabilities(0)
	var published uint64
	publish := func() {
		t.Helper()
		sequence := published + 1
		require.NoError(t, eventually(t, "quorum write during rolling upgrade", clusterReadyTimeout, func() (bool, string, error) {
			err := client.PublishIdempotentToPartition(ctx.GetTopic(), "mixed-producer", 0, sequence, 0, fmt.Sprintf("committed-%d", sequence), "all", true)
			return err == nil, fmt.Sprintf("sequence=%d error=%v", sequence, err), nil
		}))
		published = sequence
	}
	publish()
	for node := 1; node <= 3; node++ {
		container := fmt.Sprintf("%sbroker-%d", os.Getenv("CURSUS_E2E_CONTAINER_PREFIX"), node)
		// #nosec G204 -- explicitly opted-in test binary and generated test container; no shell interpolation.
		output, err := exec.Command("docker", "cp", binary, container+":/app/broker.upgrade").CombinedOutput()
		require.NoError(t, err, string(output))
		// #nosec G204 -- fixed command operates only on the isolated test container's copied binary.
		output, err = exec.Command("docker", "exec", "--user", "0", container, "sh", "-c", "chmod 755 /app/broker.upgrade && mv /app/broker.upgrade /app/broker").CombinedOutput()
		require.NoError(t, err, string(output))
		// #nosec G204 -- fixed binary path and generated test container.
		output, err = exec.Command("docker", "exec", container, "sha256sum", "/app/broker").CombinedOutput()
		require.NoError(t, err, string(output))
		require.True(t, strings.HasPrefix(string(output), hex.EncodeToString(expected[:])))
		actions.StopBroker(node)
		waitForBrokerEvictedFromISR(t, ctx, node)
		publish() // Two replicas must still satisfy minISR while this node is down.
		actions.StartBroker(node)
		assertCapabilities(node)
		publish()
		waitForStableFullISRAndZeroUnderReplicated(t, ctx, fmt.Sprintf("upgrade %d", node))
		requireReplicaOffsetsEventually(t, ctx.GetBrokerAddrs(), ctx.GetTopic(), published)
	}
	verifyLiveConsumerOffsetCatchup(t, ctx, actions)
}
