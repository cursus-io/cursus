package topic

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func intPtr(value int) *int {
	return &value
}

func TestPolicyMinInSyncReplicasOverrideAndFallback(t *testing.T) {
	policy, err := (Policy{MinInSyncReplicas: intPtr(2)}).Normalize()
	require.NoError(t, err)
	require.Equal(t, 2, policy.EffectiveMinInSyncReplicas(3))
	require.Equal(t, 3, DefaultPolicy().EffectiveMinInSyncReplicas(3))
	require.Equal(t, 1, DefaultPolicy().EffectiveMinInSyncReplicas(0))
}

func TestPolicyRejectsNonPositiveMinInSyncReplicasOverride(t *testing.T) {
	for _, value := range []int{0, -1} {
		_, err := (Policy{MinInSyncReplicas: intPtr(value)}).Normalize()
		require.ErrorContains(t, err, "min_in_sync_replicas")
	}
}

func TestLegacyDefinitionWithoutMinInSyncReplicasUsesFallback(t *testing.T) {
	var definition Definition
	require.NoError(t, json.Unmarshal([]byte(`{
		"name":"legacy",
		"partitions":1,
		"idempotent":false,
		"event_sourcing":false,
		"policy":{"cleanup_policy":"delete","partitioner":"hash_key","auth_policy":"open"}
	}`), &definition))

	normalized, err := definition.Normalize()
	require.NoError(t, err)
	require.Nil(t, normalized.Policy.MinInSyncReplicas)
	require.Equal(t, 2, normalized.Policy.EffectiveMinInSyncReplicas(2))
}

func TestPolicySnapshotIsSafeDuringMinInSyncReplicaChanges(t *testing.T) {
	topic := &Topic{Policy: DefaultPolicy()}
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			policy := DefaultPolicy()
			if i%2 == 0 {
				policy.MinInSyncReplicas = intPtr(1)
			}
			topic.ApplyPolicy(policy)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			policy := topic.PolicySnapshot()
			effective := policy.EffectiveMinInSyncReplicas(2)
			if effective != 1 && effective != 2 {
				t.Errorf("unexpected effective min ISR %d", effective)
			}
		}
	}()

	wg.Wait()
}
