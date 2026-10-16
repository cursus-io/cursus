package topic

import (
	"context"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

type rawRecoveryState struct {
	first, end uint64
	markers    map[transactionMarkerKey]transactionMarkerInfo
	open       map[transactionMarkerKey]uint64
	producers  map[string]producerStateCheckpointEntry
}

// Build detached maps under storage exclusion. Resolve transaction authority
// only after the disk scan has released all storage locks.
func (p *Partition) scanRawRecoveryState() (*rawRecoveryState, error) {
	return p.scanRawRecoveryStateContext(context.Background())
}

func (p *Partition) scanRawRecoveryStateContext(ctx context.Context) (*rawRecoveryState, error) {
	state := &rawRecoveryState{markers: make(map[transactionMarkerKey]transactionMarkerInfo), open: make(map[transactionMarkerKey]uint64), producers: make(map[string]producerStateCheckpointEntry)}
	if p.dh == nil {
		return state, nil
	}
	if storage, ok := p.dh.(interface {
		GetFirstOffsetContext(context.Context) (uint64, error)
	}); ok && !p.BootstrapPending() {
		var err error
		state.first, err = storage.GetFirstOffsetContext(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		state.first = p.dh.GetFirstOffset()
	}
	state.end = p.dh.GetAbsoluteOffset()
	visit := func(msg types.Message) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if msg.ProducerID != "" && msg.SeqNum > 0 {
			candidate := producerStateCheckpointEntry{Epoch: msg.Epoch, Seq: msg.SeqNum, Offset: msg.Offset}
			if current, ok := state.producers[msg.ProducerID]; !ok || producerStateAfter(candidate, current) {
				state.producers[msg.ProducerID] = candidate
			}
		}
		if msg.TransactionalID == "" {
			return nil
		}
		key := messageTransactionMarkerKey(msg)
		if msg.TransactionMarker != types.TransactionMarkerNone {
			marker := transactionMarkerInfo{marker: msg.TransactionMarker, offset: msg.Offset, coordinatorEpoch: msg.ControlBatchCoordinatorEpoch}
			if offset, ok := state.open[key]; ok && offset < msg.Offset {
				marker.openOffset = offset
				marker.hasOpenOffset = true
				delete(state.open, key)
			}
			state.markers[key] = marker
		} else if msg.TransactionState == types.TransactionStateOpen {
			if _, ok := state.open[key]; !ok {
				state.open[key] = msg.Offset
			}
		}
		return nil
	}
	if scanner, ok := p.dh.(interface {
		ScanRetainedRange(context.Context, uint64, uint64, func(types.Message) error) error
	}); ok {
		if err := scanner.ScanRetainedRange(ctx, state.first, state.end, visit); err != nil {
			return nil, err
		}
	} else {
		for offset := state.first; offset < state.end; {
			records, err := p.dh.ReadMessages(offset, 1024)
			if err != nil {
				return nil, err
			}
			if len(records) == 0 {
				return nil, fmt.Errorf("empty recovery page before durable tail")
			}
			for _, m := range records {
				if m.Offset < offset || m.Offset >= state.end {
					return nil, fmt.Errorf("invalid recovery offset")
				}
				if err = visit(m); err != nil {
					return nil, err
				}
				offset = m.Offset + 1
			}
		}
	}
	p.txnMarkerMu.RLock()
	resolver := p.txnResolver
	p.txnMarkerMu.RUnlock()
	for key, marker := range state.markers {
		if marker.hasOpenOffset && !transactionDecisionMatchesMarker(key, marker, resolver) {
			// Keep a later transaction independently; the unresolved marker's
			// earlier boundary is also considered when computing the LSO.
			if _, exists := state.open[key]; !exists {
				state.open[key] = marker.openOffset
			}
		}
	}
	return state, nil
}
func (p *Partition) installTransactionState(state *rawRecoveryState) {
	p.txnMarkerMu.Lock()
	p.txnMarkers, p.txnOpenOffsets, p.txnRetentionFloor = state.markers, state.open, state.first
	p.advanceTransactionEvidenceRevisionLocked()
	p.txnMarkerMu.Unlock()
}
func (p *Partition) installProducerState(state *rawRecoveryState) {
	now := time.Now()
	p.producerStateMu.Lock()
	p.producerStateIndex = state.producers
	p.producerState.Clear()
	for id, entry := range state.producers {
		p.producerState.Store(id, &producerEntry{lastEpoch: entry.Epoch, lastSeq: entry.Seq, lastSeen: now})
	}
	p.producerStateCoveredOffset = state.end
	p.producerStateMu.Unlock()
}
