package transaction

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPruneTopicReferencesBlocksActiveAndFiltersTerminalTransactions(t *testing.T) {
	manager := NewManager()
	manager.ApplySnapshot(&Snapshot{
		ID: "active", State: StateOpen,
		Messages: []MessageOperation{{Topic: "orders", Partition: 0}},
	})

	_, err := manager.PruneTopicReferences("orders")
	require.ErrorContains(t, err, "active transaction")

	manager.ApplySnapshot(&Snapshot{
		ID: "active", State: StateCommitted,
		Messages: []MessageOperation{{Topic: "orders", Partition: 0}, {Topic: "audit", Partition: 0}},
		Offsets:  []OffsetOperation{{Topic: "orders", Group: "workers"}, {Topic: "audit", Group: "auditors"}},
	})
	affected, err := manager.PruneTopicReferences("orders")
	require.NoError(t, err)
	require.Equal(t, []string{"active"}, affected)

	state := manager.ExportState()["active"]
	require.Equal(t, []MessageOperation{{Topic: "audit", Partition: 0}}, state.Messages)
	require.Equal(t, []OffsetOperation{{Topic: "audit", Group: "auditors"}}, state.Offsets)
}
