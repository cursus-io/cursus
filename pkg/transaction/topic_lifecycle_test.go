package transaction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReinitializationCannotDiscardUnresolvedV1State(t *testing.T) {
	for _, state := range []State{StateOpen, StatePrepareCommit, StatePrepareAbort, StateCommitted} {
		t.Run(string(state), func(t *testing.T) {
			manager := NewManagerWithExpiration(time.Hour)
			old := time.Now().Add(-2 * time.Hour)
			manager.ApplySnapshot(&Snapshot{
				ID: "txn", Producer: "producer", Mode: ModeProcessingV1, State: state,
				Epoch: 7, Revision: 10, CreatedAt: old, UpdatedAt: old,
				Participants: []Participant{{Topic: "orders"}},
				Offsets:      []OffsetOperation{{Topic: "orders", Group: "workers", Offset: 9}},
			})
			before := manager.ExportState()
			_, _, err := manager.InitProducerWithMode("txn", ModeProcessingV1)
			require.Error(t, err)
			require.Equal(t, before, manager.ExportState(), "rejected reinitialization must preserve recovery state even beyond retention")
		})
	}
}

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

func TestTopicPruningRejectsRecoveryPendingTransactions(t *testing.T) {
	states := []State{StatePrepareCommit, StatePrepareAbort}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			_, _, err := stateWithoutTopicReferencesLocked(map[string]*Snapshot{
				"txn": {ID: "txn", Mode: ModeProcessingV1, State: state, Messages: []MessageOperation{{Topic: "orders"}}},
			}, "orders")
			require.ErrorContains(t, err, "active transaction")
		})
	}

	_, _, err := stateWithoutTopicReferencesLocked(map[string]*Snapshot{
		"txn": {
			ID:                  "txn",
			Mode:                ModeProcessingV1,
			State:               StateCommitted,
			OffsetsMaterialized: false,
			Offsets:             []OffsetOperation{{Topic: "orders"}},
		},
	}, "orders")
	require.ErrorContains(t, err, "active transaction")
}

func TestTopicPruningCoversEveryV1Reference(t *testing.T) {
	for _, state := range []State{StateOpen, StatePrepareCommit, StatePrepareAbort} {
		for name, populate := range map[string]func(*Snapshot){
			"streams":      func(s *Snapshot) { s.Streams = []StreamOperation{{Topic: "orders"}} },
			"participants": func(s *Snapshot) { s.Participants = []Participant{{Topic: "orders"}} },
			"assignments":  func(s *Snapshot) { s.RequestAssignments = map[string]RequestAssignment{"request": {Topic: "orders"}} },
			"sequences":    func(s *Snapshot) { s.SequenceByPartition = map[string]uint64{"orders:0": 1} },
		} {
			t.Run(string(state)+"/"+name, func(t *testing.T) {
				snap := &Snapshot{ID: "txn", Mode: ModeProcessingV1, State: state}
				populate(snap)
				_, _, err := stateWithoutTopicReferencesLocked(map[string]*Snapshot{"txn": snap}, "orders")
				require.ErrorContains(t, err, "active transaction")
			})
		}
	}
}

func TestTopicPruningRemovesTerminalV1ReferencesWithoutTouchingOtherTopics(t *testing.T) {
	manager := NewManager()
	manager.ApplySnapshot(&Snapshot{
		ID: "txn", Mode: ModeProcessingV1, State: StateAborted,
		Streams:             []StreamOperation{{Topic: "orders"}, {Topic: "orders:archive"}},
		Participants:        []Participant{{Topic: "orders"}, {Topic: "orders:archive"}},
		RequestAssignments:  map[string]RequestAssignment{"remove": {Topic: "orders"}, "keep": {Topic: "orders:archive"}},
		SequenceByPartition: map[string]uint64{"orders:0": 1, "orders:archive:0": 2},
	})
	before := manager.ExportState()
	next, affected, err := manager.StateWithoutTopicReferences("orders")
	require.NoError(t, err)
	require.Equal(t, []string{"txn"}, affected)
	require.Equal(t, before, manager.ExportState(), "preparing deletion must not mutate live state")
	require.False(t, transactionReferencesTopic(next["txn"], "orders"))
	require.True(t, transactionReferencesTopic(next["txn"], "orders:archive"))
	_, err = manager.PruneTopicReferences("orders")
	require.NoError(t, err)
	after := manager.ExportState()["txn"]
	require.Equal(t, []StreamOperation{{Topic: "orders:archive"}}, after.Streams)
	require.Equal(t, []Participant{{Topic: "orders:archive"}}, after.Participants)
	require.Equal(t, map[string]RequestAssignment{"keep": {Topic: "orders:archive"}}, after.RequestAssignments)
	require.Equal(t, map[string]uint64{"orders:archive:0": 2}, after.SequenceByPartition)
}
