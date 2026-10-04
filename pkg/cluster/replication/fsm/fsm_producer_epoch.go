package fsm

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/transaction"
)

// Version-nine snapshots predate the bounded allocator watermark. Their
// replicated producer index and transaction/group state retain every epoch
// that can still fence a producer or identify a retained transactional record.
// Topic deletion and truncation remove both the records and the corresponding
// producer index, so those epochs no longer need to constrain reuse.
func producerEpochWatermarkForRestore(state *BrokerFSMState) (uint64, error) {
	if state == nil {
		return 0, fmt.Errorf("transaction snapshot state is missing")
	}
	if state.NextProducerEpoch != nil {
		if state.Version != SnapshotVersionCurrent {
			return 0, fmt.Errorf("snapshot version %d cannot contain a producer epoch watermark", state.Version)
		}
		if err := transaction.ValidateProducerEpochWatermark(*state.NextProducerEpoch); err != nil {
			return 0, err
		}
		return *state.NextProducerEpoch, nil
	}
	if state.Version != SnapshotVersionLegacyEpoch {
		return 0, fmt.Errorf("snapshot version %d is missing producer epoch watermark", state.Version)
	}

	var next uint64
	observe := func(epoch int64) error {
		if epoch < 0 {
			return fmt.Errorf("invalid producer epoch %d", epoch)
		}
		next = max(next, uint64(epoch)+1)
		return transaction.ValidateProducerEpochWatermark(next)
	}
	for _, tx := range state.TransactionState {
		if err := observe(tx.Epoch); err != nil {
			return 0, err
		}
	}
	for _, partitions := range state.ProducerState {
		for _, producers := range partitions {
			for _, producer := range producers {
				if err := observe(producer.Epoch); err != nil {
					return 0, err
				}
			}
		}
	}
	for _, group := range state.GroupState {
		for _, reservation := range group.OffsetReservations {
			if err := observe(reservation.ProducerEpoch); err != nil {
				return 0, err
			}
		}
		for _, decision := range group.ReservationDecisions {
			if err := observe(decision.ProducerEpoch); err != nil {
				return 0, err
			}
		}
	}
	for _, entry := range state.Logs {
		if entry != nil && entry.Message.TransactionalID != "" {
			if err := observe(entry.Message.Epoch); err != nil {
				return 0, err
			}
		}
	}
	return next, nil
}
