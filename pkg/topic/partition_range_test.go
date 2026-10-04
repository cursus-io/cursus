package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReadCommittedRangeKeepsCommitVisibilitySeparateFromPageBound(t *testing.T) {
	for _, tc := range []struct {
		name, state, marker string
		hwm, flushed, end   uint64
		scan, visible       bool
	}{
		{"marker outside page", "committed", "commit", 3, 3, 1, true, true},
		{"marker inside page", "committed", "commit", 3, 3, 2, true, true},
		{"prepared decision", "prepare_commit", "commit", 3, 3, 1, false, false},
		{"abort marker", "aborted", "abort", 3, 3, 1, true, false},
		{"marker not replicated", "committed", "commit", 1, 3, 1, false, false},
		{"marker not flushed", "committed", "commit", 3, 1, 1, false, false},
		{"empty page", "committed", "commit", 3, 3, 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dh := new(MockStorageHandler)
			dh.On("GetLatestOffset").Return(uint64(0)).Once()
			dh.On("GetFlushedOffset").Return(tc.flushed).Once()
			dh.On("GetFirstOffset").Return(uint64(0)).Once()
			event := types.Message{Offset: 0, Payload: "event", TransactionalID: "range", TransactionState: types.TransactionStateOpen, Epoch: 2}
			marker := types.Message{Offset: 1, TransactionalID: "range", TransactionMarker: tc.marker, Epoch: 2}
			if tc.scan {
				// Returning a record at the exclusive bound also checks that the
				// range reader never leaks records from the following page.
				dh.On("ReadMessages", uint64(0), int(tc.end)).Return([]types.Message{event, marker, {Offset: 2, Payload: "later"}}, nil).Once()
			}
			p := NewPartition(0, "range", dh, nil, config.DefaultConfig())
			p.SetHWM(tc.hwm)
			p.indexTransactionMessage(event)
			p.indexTransactionMessage(marker)
			p.SetTransactionDecisionResolver(&testTransactionDecisionResolver{state: tc.state, known: true})
			messages, err := p.ReadCommittedRange(0, tc.end, 10)
			require.NoError(t, err)
			if tc.visible {
				require.Equal(t, []types.Message{event}, messages)
			} else {
				require.Empty(t, messages)
			}
			dh.AssertExpectations(t)
		})
	}
}
