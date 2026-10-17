package fsm

import (
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerTransactionMarkerBindsDecisionAndControlBytes(t *testing.T) {
	for _, marker := range []string{types.TransactionMarkerCommit, types.TransactionMarkerAbort} {
		t.Run(marker, func(t *testing.T) {
			tx := &transaction.Snapshot{ID: "marker-transaction", Epoch: 3, CoordinatorEpoch: 7, Mode: transaction.ModeProcessingV1, State: transaction.StatePrepareCommit}
			state := types.TransactionStateCommitted
			if marker == types.TransactionMarkerAbort {
				tx.State, state = transaction.StatePrepareAbort, types.TransactionStateAborted
			}
			key, value, err := types.TransactionMarkerControlBytes(marker, tx.CoordinatorEpoch)
			require.NoError(t, err)
			message := types.Message{ProducerID: fmt.Sprintf("txn-marker:%s:%d:%s", tx.ID, tx.CoordinatorEpoch, marker), SeqNum: 1, TransactionState: state, TransactionMarker: marker, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: tx.CoordinatorEpoch, ControlBatchKey: key, ControlBatchValue: value}
			require.NoError(t, validateConsumerTransactionMarker(tx, message))
			for _, change := range []string{"producer", "sequence", "state", "type", "version", "epoch", "key", "value", "decision"} {
				t.Run(change, func(t *testing.T) {
					copyMessage, copyTx := message, *tx
					switch change {
					case "producer":
						copyMessage.ProducerID = "different"
					case "sequence":
						copyMessage.SeqNum++
					case "state":
						copyMessage.TransactionState = types.TransactionStateOpen
					case "type":
						copyMessage.ControlBatchType = "different"
					case "version":
						copyMessage.ControlBatchVersion++
					case "epoch":
						copyMessage.ControlBatchCoordinatorEpoch++
					case "key":
						copyMessage.ControlBatchKey = []byte{0}
					case "value":
						copyMessage.ControlBatchValue = []byte{0}
					case "decision":
						if marker == types.TransactionMarkerCommit {
							copyTx.State = transaction.StatePrepareAbort
						} else {
							copyTx.State = transaction.StatePrepareCommit
						}
					}
					require.ErrorIs(t, validateConsumerTransactionMarker(&copyTx, copyMessage), ErrPartitionCommitFenced)
				})
			}
		})
	}
}

func TestConsumerTransactionRouteRequiresOriginalAuthorityAndExactRecord(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerConsumerRoutingBroker(t, f, "a", "active", 2)
	f.SetTransactionManager(transaction.NewManager())
	const id = "offset-transaction"
	producer, epoch, err := f.txn.InitProducerWithMode(id, transaction.ModeProcessingV1)
	require.NoError(t, err)
	owner, ok := f.GetTransactionCoordinator(id)
	require.True(t, ok)
	require.NoError(t, f.txn.SetCoordinatorEpoch(id, owner.Epoch))
	require.NoError(t, f.txn.Begin(id, producer, epoch))
	require.NoError(t, f.txn.AddOffsets(id, producer, epoch, []transaction.OffsetOperation{{Group: "g", Topic: "orders", Member: "worker", Generation: 1, RegistrationEpoch: 1, Partition: 0, Offset: 9}}))
	require.NoError(t, f.txn.AddParticipant(id, producer, epoch, transaction.Participant{Topic: config.ConsumerOffsetsTopicName, Partition: 0}, time.Now().Add(time.Minute)))
	f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"] = &PartitionMetadata{Leader: "a", LeaderEpoch: 1, PartitionCount: 1, LifecycleEpoch: 1}
	route, err := f.ConsumerTransactionMetadataRoute("a", id, 0)
	require.NoError(t, err)
	_, err = f.ConsumerTransactionMetadataRoute("old-owner", id, 0)
	require.Error(t, err)
	tx, ok := f.txn.Snapshot(id)
	require.True(t, ok)
	record := coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersionTransactions, Type: coordinator.ConsumerMetadataRecordTransactionalOffsetSnapshot, Group: "g", Topic: "orders", Epoch: 1, Revision: tx.Revision, Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 9}}, TransactionalID: id, ProducerID: producer, ProducerEpoch: epoch, CoordinatorEpoch: owner.Epoch, Timestamp: tx.CreatedAt}
	payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	message := types.Message{Payload: string(payload), Key: key, ProducerID: producer, Epoch: epoch, TransactionalID: id, TransactionState: types.TransactionStateOpen}
	command := types.MessageCommand{Topic: config.ConsumerOffsetsTopicName, ConsumerRoute: route, Messages: []types.Message{message}}
	require.NoError(t, f.ValidateConsumerMetadataMessages(command))
	for _, mutation := range []string{"owner", "coordinator epoch", "producer epoch", "revision", "group", "offset", "ordinary"} {
		t.Run(mutation, func(t *testing.T) {
			copyRoute := *route
			proof := *route.Transaction
			copyRoute.Transaction = &proof
			copyRecord := record
			copyMessage := message
			switch mutation {
			case "owner":
				proof.Owner = "old"
			case "coordinator epoch":
				proof.CoordinatorEpoch++
			case "producer epoch":
				proof.ProducerEpoch++
			case "revision":
				proof.Revision++
			case "group":
				copyRecord.Group = "unrelated"
			case "offset":
				copyRecord.Offsets = []coordinator.OffsetItem{{Partition: 0, Offset: 999}}
			case "ordinary":
				copyMessage.Payload = `{"version":1,"type":"offset_snapshot","group":"g","topic":"orders","epoch":1,"revision":1,"offsets":[{"partition":0,"offset":999}]}`
			}
			if mutation == "group" || mutation == "offset" {
				encoded, key, err := coordinator.EncodeConsumerMetadataRecord(copyRecord)
				require.NoError(t, err)
				copyMessage.Payload, copyMessage.Key = string(encoded), key
			}
			require.ErrorIs(t, f.ValidateConsumerMetadataMessages(types.MessageCommand{Topic: config.ConsumerOffsetsTopicName, ConsumerRoute: &copyRoute, Messages: []types.Message{copyMessage}}), ErrPartitionCommitFenced)
		})
	}
}

func TestConsumerRouteBindsActualPayloadGroup(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	registerConsumerRoutingBroker(t, f, "a", "active", 2)
	f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"] = &PartitionMetadata{Leader: "a", LeaderEpoch: 1, PartitionCount: 1, LifecycleEpoch: 1}
	routes, _, err := f.ConsumerMetadataRoutes([]string{"expected"})
	require.NoError(t, err)
	route := routes["expected"]
	record := coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "other", Topic: "orders", PartitionCount: 1, Epoch: 1}
	payload, _, err := coordinator.EncodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	command := types.MessageCommand{Topic: config.ConsumerOffsetsTopicName, ConsumerRoute: &route, Messages: []types.Message{{Payload: string(payload)}}}
	require.ErrorIs(t, f.ValidateConsumerMetadataMessages(command), ErrPartitionCommitFenced)
	require.Equal(t, "ERROR", f.applyMessageBatch(&command).(types.AckResponse).Status)
}
