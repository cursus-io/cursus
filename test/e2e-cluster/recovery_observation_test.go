package e2e_cluster

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObserveRecoveryFenceWaitsForEveryBroker(t *testing.T) {
	const recovered = `OK cluster={"healthy":false,"recovery_pending_partitions":1,"partitions":[{"topic":"history","partition":0,"leader_epoch":7,"recovery_replicas":["broker-2-9000"],"recovery_pending":true}]}`
	addrs := []string{"broker-1", "broker-2", "broker-3"}
	for _, restarted := range addrs {
		t.Run(restarted, func(t *testing.T) {
			for _, stale := range []string{
				`OK cluster={"healthy":false,"recovery_pending_partitions":0,"partitions":[]}`,
				`OK cluster={"healthy":false,"recovery_pending_partitions":1,"partitions":[]}`,
				strings.Replace(recovered, `"leader_epoch":7`, `"leader_epoch":6`, 1),
				strings.Replace(recovered, `"recovery_pending":true`, `"recovery_pending":false`, 1),
			} {
				node, err := observeRecoveryFence(addrs, "history", 0, 7, func(addr string) (string, error) {
					if addr == restarted {
						return stale, nil
					}
					return recovered, nil
				})
				require.Error(t, err, "a survivor's view must not mask a restarting peer")
				require.Zero(t, node)
			}
		})
	}
	visited := []string{}
	node, err := observeRecoveryFence(addrs, "history", 0, 7, func(addr string) (string, error) {
		visited = append(visited, addr)
		return recovered, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, node)
	require.Equal(t, addrs, visited)

	_, err = observeRecoveryFence(addrs, "history", 0, 7, func(addr string) (string, error) {
		if addr == "broker-2" {
			return "", errors.New("restarting")
		}
		return recovered, nil
	})
	require.ErrorContains(t, err, "broker-2")
}
