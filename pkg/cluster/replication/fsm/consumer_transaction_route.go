package fsm

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
)

// ConsumerTransactionMetadataRoute preserves the transaction coordinator's
// original authority while allowing raw decision records on touched partitions.
func (f *BrokerFSM) ConsumerTransactionMetadataRoute(localID, id string, partition int) (*types.ConsumerMetadataRoute, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.txn == nil {
		return nil, fmt.Errorf("transaction state unavailable")
	}
	tx, ok := f.txn.Snapshot(id)
	if !ok {
		return nil, fmt.Errorf("transaction state unavailable")
	}
	owner := f.transactionCoordinatorShards[transaction.CoordinatorShardForCount(id, f.effectiveTransactionCoordinatorShardCountLocked())]
	if owner.Owner != localID || owner.Epoch <= 0 {
		return nil, fmt.Errorf("transaction metadata origin is not the current coordinator")
	}
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, partition)]
	if meta == nil {
		return nil, fmt.Errorf("consumer offsets topology unavailable")
	}
	route := &types.ConsumerMetadataRoute{Owner: meta.Leader, LeaderEpoch: meta.LeaderEpoch, LifecycleEpoch: meta.LifecycleEpoch, PartitionCount: meta.PartitionCount, Partition: partition,
		Transaction: &types.ConsumerTransactionRoute{ID: id, Owner: localID, CoordinatorEpoch: owner.Epoch, ProducerEpoch: tx.Epoch, Revision: tx.Revision}}
	if err := f.validateConsumerMetadataRouteLocked(config.ConsumerOffsetsTopicName, partition, route); err != nil {
		return nil, err
	}
	return route, nil
}

func (f *BrokerFSM) consumerTransactionSnapshotLocked(route *types.ConsumerMetadataRoute) (*transaction.Snapshot, error) {
	proof := route.Transaction
	fenced := fmt.Errorf("%w: transaction metadata authority changed", ErrPartitionCommitFenced)
	if proof == nil || proof.ID == "" || proof.Owner == "" || proof.CoordinatorEpoch <= 0 || proof.Revision == 0 || f.txn == nil {
		return nil, fenced
	}
	owner := f.transactionCoordinatorShards[transaction.CoordinatorShardForCount(proof.ID, f.effectiveTransactionCoordinatorShardCountLocked())]
	tx, ok := f.txn.Snapshot(proof.ID)
	if !ok || owner.Owner != proof.Owner || owner.Epoch != proof.CoordinatorEpoch || tx.Epoch != proof.ProducerEpoch || tx.Revision != proof.Revision ||
		(tx.Mode == transaction.ModeProcessingV1 && tx.CoordinatorEpoch != proof.CoordinatorEpoch) {
		return nil, fenced
	}
	for _, participant := range tx.Participants {
		if participant.Topic == config.ConsumerOffsetsTopicName && participant.Partition == route.Partition {
			return tx, nil
		}
	}
	for _, operation := range tx.Messages {
		if operation.Topic == config.ConsumerOffsetsTopicName && operation.Partition == route.Partition {
			return tx, nil
		}
	}
	return nil, fenced
}

func (f *BrokerFSM) validateConsumerTransactionMessageLocked(route *types.ConsumerMetadataRoute, msg types.Message) error {
	tx, err := f.consumerTransactionSnapshotLocked(route)
	if err != nil {
		return err
	}
	fenced := fmt.Errorf("%w: invalid transaction metadata record", ErrPartitionCommitFenced)
	if msg.TransactionalID != tx.ID || msg.Epoch != tx.Epoch {
		return fenced
	}
	if msg.TransactionMarker != "" {
		if f.consumerTransactionHistoryActivated && tx.Mode == transaction.ModeProcessingV1 && msg.ProducerID == fmt.Sprintf("txn-history:%s:%s", tx.ID, msg.TransactionMarker) && msg.SeqNum == uint64(tx.CoordinatorEpoch) {
			// Validate every other field using the existing processing marker contract.
			msg.ProducerID = fmt.Sprintf("txn-marker:%s:%d:%s", tx.ID, tx.CoordinatorEpoch, msg.TransactionMarker)
			msg.SeqNum = 1
		}
		return validateConsumerTransactionMarker(tx, msg)
	}
	if msg.ProducerID != tx.Producer || msg.TransactionState != types.TransactionStateOpen {
		return fenced
	}
	record, versioned, err := coordinator.DecodeConsumerMetadataRecord(msg.Payload)
	if err != nil || !versioned || record.Type != coordinator.ConsumerMetadataRecordTransactionalOffsetSnapshot ||
		record.TransactionalID != tx.ID || record.ProducerID != tx.Producer || record.ProducerEpoch != tx.Epoch || record.CoordinatorEpoch != tx.CoordinatorEpoch || record.Revision != tx.Revision {
		return fenced
	}
	if record.Version == coordinator.ConsumerMetadataRecordVersionTransactionHistory && (!f.consumerTransactionHistoryActivated || tx.Mode != transaction.ModeProcessingV1) {
		return fenced
	}
	expected := make(map[int]uint64)
	for _, offset := range tx.Offsets {
		if offset.Group == record.Group && offset.Topic == record.Topic && offset.RegistrationEpoch == record.Epoch {
			expected[offset.Partition] = offset.Offset
		}
	}
	actual := make(map[int]uint64)
	for _, offset := range record.Offsets {
		actual[offset.Partition] = offset.Offset
	}
	if len(expected) == 0 || len(actual) != len(record.Offsets) || !reflect.DeepEqual(expected, actual) {
		return fenced
	}
	_, expectedKey, err := coordinator.EncodeConsumerMetadataRecord(record)
	if err != nil || msg.Key != expectedKey {
		return fenced
	}
	return nil
}

func validateConsumerTransactionMarker(tx *transaction.Snapshot, msg types.Message) error {
	fenced := fmt.Errorf("%w: invalid transaction metadata marker", ErrPartitionCommitFenced)
	epoch := tx.Epoch
	producer := fmt.Sprintf("txn-marker:%s:%s", tx.ID, msg.TransactionMarker)
	if tx.Mode == transaction.ModeProcessingV1 {
		epoch = tx.CoordinatorEpoch
		producer = fmt.Sprintf("txn-marker:%s:%d:%s", tx.ID, epoch, msg.TransactionMarker)
	}
	key, value, err := types.TransactionMarkerControlBytes(msg.TransactionMarker, epoch)
	if err != nil || msg.ProducerID != producer || msg.SeqNum != 1 || msg.ControlBatchType != types.ControlBatchTransaction ||
		msg.ControlBatchVersion != types.ControlBatchVersionCursusV2 || msg.ControlBatchCoordinatorEpoch != epoch || !bytes.Equal(msg.ControlBatchKey, key) || !bytes.Equal(msg.ControlBatchValue, value) {
		return fenced
	}
	switch msg.TransactionMarker {
	case types.TransactionMarkerCommit:
		if (tx.State != transaction.StateCommitting && tx.State != transaction.StatePrepareCommit && tx.State != transaction.StateCommitted) || msg.TransactionState != types.TransactionStateCommitted {
			return fenced
		}
	case types.TransactionMarkerAbort:
		if (tx.State != transaction.StateOpen && tx.State != transaction.StatePrepareAbort && tx.State != transaction.StateAborted) || msg.TransactionState != types.TransactionStateAborted {
			return fenced
		}
	default:
		return fenced
	}
	return nil
}
