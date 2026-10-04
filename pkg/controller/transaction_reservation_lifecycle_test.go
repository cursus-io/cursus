package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func appendStandaloneReservationMetadata(tm *topic.TopicManager, record coordinator.ConsumerMetadataRecord) error {
	payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
	if err != nil {
		return err
	}
	return tm.PublishWithAck(config.ConsumerOffsetsTopicName, &types.Message{Key: key, Payload: string(payload)})
}

func stagedReservationWorkflow(t *testing.T) (*CommandHandler, *topic.TopicManager, *coordinator.Coordinator, string, int64, int) {
	t.Helper()
	ch, tm, cd, _ := newDiskBackedTransactionHandler(t)
	generation := prepareTransactionGroup(t, tm, cd, "input", "workers", "worker")
	require.NoError(t, tm.CreateTopic("output", 1, false, false))
	require.NoError(t, cd.CommitOffset("workers", "input", 0, 0))
	require.NoError(t, ch.ConfigureTransactionJournal(filepath.Join(ch.Config.LogDir, "reservation-txn.log")))
	ctx := NewClientContext("", 0)
	response := ch.HandleCommand("INIT_PRODUCER_ID transactional_id=tx", ctx)
	require.True(t, strings.HasPrefix(response, "OK "), response)
	fields := parseKeyValueArgs(strings.TrimPrefix(response, "OK "))
	producer := fields["producerId"]
	epoch, err := strconv.ParseInt(fields["epoch"], 10, 64)
	require.NoError(t, err)
	for _, command := range []string{
		fmt.Sprintf("BEGIN_TXN transactional_id=tx producerId=%s epoch=%d", producer, epoch),
		fmt.Sprintf("TXN_PUBLISH transactional_id=tx producerId=%s epoch=%d topic=output partition=0 seqNum=1 message=result", producer, epoch),
		fmt.Sprintf("SEND_OFFSETS_TO_TXN transactional_id=tx producerId=%s epoch=%d topic=input group=workers member=worker generation=%d offsets=P0:1", producer, epoch, generation),
	} {
		response = ch.HandleCommand(command, ctx)
		require.True(t, strings.HasPrefix(response, "OK "), response)
	}
	return ch, tm, cd, producer, epoch, generation
}

func TestPreparedReservationRecoversAfterMemberDepartureAndJournalReload(t *testing.T) {
	ch, tm, cd, producer, epoch, generation := stagedReservationWorkflow(t)
	ch.transactionStateSyncHook = func(id string) error {
		snapshot, _ := ch.TxnManager.Snapshot(id)
		if err := ch.txnJournal.Append(snapshot); err != nil {
			return err
		}
		if snapshot.State == transaction.StatePrepareCommit {
			require.NoError(t, cd.RemoveConsumerForGeneration("workers", "worker", generation))
			return errors.New("lost response after durable prepare")
		}
		return nil
	}
	response := ch.HandleCommand(fmt.Sprintf("END_TXN transactional_id=tx producerId=%s epoch=%d result=commit", producer, epoch), NewClientContext("", 0))
	require.Contains(t, response, "transaction_sync_failed")
	ch.transactionStateSyncHook = nil
	require.Empty(t, readCommittedPayloads(t, tm, "output"))
	_, err := cd.AddConsumer("workers", "replacement")
	require.NoError(t, err)
	require.Contains(t, ch.handleFetchOffset("FETCH_OFFSET topic=input partition=0 group=workers"), "unstable_offset_commit")
	restarted := NewCommandHandler(tm, ch.Config, cd, nil, nil)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	require.NoError(t, restarted.ConfigureTransactionJournal(filepath.Join(ch.Config.LogDir, "reservation-txn.log")))
	require.NoError(t, restarted.RecoverPreparedTransactions())
	require.Equal(t, []string{"result"}, readCommittedPayloads(t, tm, "output"))
	require.Equal(t, "OK offset=1", restarted.handleFetchOffset("FETCH_OFFSET topic=input partition=0 group=workers"))
	final, err := restarted.TxnManager.Status("tx")
	require.NoError(t, err)
	require.Equal(t, transaction.StateCommitted, final.State)
	require.True(t, final.OffsetsMaterialized)
	require.False(t, final.OffsetReservationsPending)
	require.NoError(t, restarted.RecoverPreparedTransactions())
	require.Equal(t, []string{"result"}, readCommittedPayloads(t, tm, "output"))
}

func TestUncertainReservationFreezesPayloadAndTimeoutReleasesInput(t *testing.T) {
	ch, tm, cd, producer, epoch, generation := stagedReservationWorkflow(t)
	loseAck := true
	cd.SetOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
		if err := appendStandaloneReservationMetadata(tm, record); err != nil {
			return err
		}
		if record.Type == coordinator.ConsumerMetadataRecordOffsetReservations && len(record.Reservations) > 0 && loseAck {
			loseAck = false
			return errors.New("reservation acknowledgement lost")
		}
		return nil
	})
	response := ch.HandleCommand(fmt.Sprintf("END_TXN transactional_id=tx producerId=%s epoch=%d result=commit", producer, epoch), NewClientContext("", 0))
	require.Contains(t, response, "acknowledgement lost")
	response = ch.HandleCommand(fmt.Sprintf("SEND_OFFSETS_TO_TXN transactional_id=tx producerId=%s epoch=%d topic=input group=workers member=worker generation=%d offsets=P0:2", producer, epoch, generation), NewClientContext("", 0))
	require.Contains(t, response, "frozen")
	require.NoError(t, ch.AbortTimedOutTransactions(time.Now().Add(time.Hour)))
	status, err := ch.TxnManager.Status("tx")
	require.NoError(t, err)
	require.Equal(t, transaction.StateAborted, status.State)
	require.False(t, status.OffsetReservationsPending)
	require.Empty(t, readCommittedPayloads(t, tm, "output"))
	require.Equal(t, "OK offset=0", ch.handleFetchOffset("FETCH_OFFSET topic=input partition=0 group=workers"))
}

func TestAbortedReservationRemainsRecoverableUntilReleaseSucceeds(t *testing.T) {
	ch, tm, cd, producer, epoch, _ := stagedReservationWorkflow(t)
	fail := true
	cd.SetOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
		if err := appendStandaloneReservationMetadata(tm, record); err != nil {
			return err
		}
		if record.Type == coordinator.ConsumerMetadataRecordOffsetReservations && fail {
			return errors.New("offset checkpoint unavailable")
		}
		return nil
	})
	commit := fmt.Sprintf("END_TXN transactional_id=tx producerId=%s epoch=%d result=commit", producer, epoch)
	require.Contains(t, ch.HandleCommand(commit, NewClientContext("", 0)), "checkpoint unavailable")
	abort := fmt.Sprintf("END_TXN transactional_id=tx producerId=%s epoch=%d result=abort", producer, epoch)
	require.Contains(t, ch.HandleCommand(abort, NewClientContext("", 0)), "checkpoint unavailable")
	status, err := ch.TxnManager.Status("tx")
	require.NoError(t, err)
	require.Equal(t, transaction.StateAborted, status.State)
	require.True(t, status.OffsetReservationsPending)
	require.Contains(t, ch.HandleCommand("INIT_PRODUCER_ID transactional_id=tx", NewClientContext("", 0)), "checkpoint unavailable")
	fail = false
	require.NoError(t, ch.RecoverPreparedTransactions())
	require.Equal(t, "OK offset=0", ch.handleFetchOffset("FETCH_OFFSET topic=input partition=0 group=workers"))
	require.Contains(t, ch.HandleCommand("INIT_PRODUCER_ID transactional_id=tx", NewClientContext("", 0)), "epoch=1")
}

func TestReservationRPCRejectsPublicCallerAndUncommittedDecision(t *testing.T) {
	ch, _, _, producer, epoch, _ := stagedReservationWorkflow(t)
	tx, err := ch.TxnManager.BeginOffsetReservations("tx", producer, epoch)
	require.NoError(t, err)
	request, err := reservationRequestFor(tx)
	require.NoError(t, err)
	require.ErrorContains(t, ch.applyTransactionReservation(request, "commit"), "matching final decision")
	require.ErrorContains(t, ch.applyTransactionReservation(request, "abort"), "matching final decision")
	response := ch.HandleCommand("BATCH_COMMIT group=workers reservation_action=abort reservation=e30", NewClientContext("", 0))
	require.Contains(t, response, "internal_command_unauthorized")
}

func TestReservationRPCWaitsForReplicatedActivationAndFinalDecision(t *testing.T) {
	ch, tm, cd, producer, epoch, _ := stagedReservationWorkflow(t)
	f := fsm.NewBrokerFSM(nil, nil)
	f.SetTransactionManager(ch.TxnManager)
	require.Nil(t, f.Apply(&raft.Log{Index: 1, Data: []byte(`REGISTER:{"id":"node-1","addr":"127.0.0.1:7000","status":"active","lifecycle_protocol":3}`)}))
	definition := topic.DefaultDefinition("input", ch.Config)
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Index: 2, Data: append([]byte("TOPIC:"), payload...)}))
	ch.Cluster = newCoordinatorRoutingHandler("node-1", f, cd).Cluster
	f.SetTransactionManager(ch.TxnManager)
	installRoutingOffsetsTopology(t, f, "node-1")
	ch.Config.EnabledDistribution = true
	cd.SetOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
		return appendStandaloneReservationMetadata(tm, record)
	})
	owner, ok := f.GetTransactionCoordinator("tx")
	require.True(t, ok)
	require.NoError(t, ch.TxnManager.SetCoordinatorEpoch("tx", owner.Epoch))
	tx, err := ch.TxnManager.BeginOffsetReservations("tx", producer, epoch)
	require.NoError(t, err)
	request, err := reservationRequestFor(tx)
	require.NoError(t, err)
	request.CoordinatorOwner = owner.Owner
	require.ErrorIs(t, ch.applyTransactionReservation(request, "prepare"), errReservationStateNotApplied)
	apply := func(snapshot *transaction.Snapshot, index uint64) {
		t.Helper()
		payload, err := json.Marshal(map[string]interface{}{"transaction": snapshot, "coordinator_owner": owner.Owner, "coordinator_epoch": owner.Epoch})
		require.NoError(t, err)
		require.Nil(t, f.Apply(&raft.Log{Index: index, Data: append([]byte("TXN_SYNC:"), payload...)}))
	}
	waiting := make(chan error, 1)
	go func() { waiting <- ch.applyTransactionReservationWithWait(request, "prepare") }()
	select {
	case err := <-waiting:
		t.Fatalf("reservation returned before activation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	snapshot, ok := ch.TxnManager.Snapshot("tx")
	require.True(t, ok)
	apply(snapshot, 3)
	select {
	case err := <-waiting:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reservation did not observe activation")
	}
	_, err = ch.TxnManager.PrepareCommit("tx", producer, epoch)
	require.NoError(t, err)
	decision, err := ch.TxnManager.BuildCommittedSnapshot("tx")
	require.NoError(t, err)
	request.TransactionRevision = decision.Revision
	require.ErrorIs(t, ch.applyTransactionReservation(request, "commit"), errReservationStateNotApplied)
	go func() { waiting <- ch.applyTransactionReservationWithWait(request, "commit") }()
	select {
	case err := <-waiting:
		t.Fatalf("reservation returned before final decision: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	apply(decision, 4)
	select {
	case err := <-waiting:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reservation did not observe final decision")
	}
	offset, found, err := cd.GetStableOffset("workers", "input", 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(1), offset)
	require.ErrorContains(t, ch.applyTransactionReservationWithWait(request, "abort"), "matching final decision")
	request.CoordinatorEpoch--
	require.ErrorContains(t, ch.applyTransactionReservationWithWait(request, "commit"), "coordinator fenced")
}

func TestTerminalReservationCleanupTransfersOwnershipWithoutChangingDecisionEpoch(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			ch, tm, cd, producer, epoch, _ := stagedReservationWorkflow(t)
			f := fsm.NewBrokerFSM(nil, nil)
			f.SetTransactionManager(ch.TxnManager)
			require.Nil(t, f.Apply(&raft.Log{Index: 1, Data: []byte(`REGISTER:{"id":"node-1","addr":"127.0.0.1:7000","status":"active","lifecycle_protocol":3}`)}))
			definition := topic.DefaultDefinition("input", ch.Config)
			payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
			require.NoError(t, err)
			require.Nil(t, f.Apply(&raft.Log{Index: 2, Data: append([]byte("TOPIC:"), payload...)}))
			owner, ok := f.GetTransactionCoordinator("tx")
			require.True(t, ok)
			require.NoError(t, ch.TxnManager.SetCoordinatorEpoch("tx", owner.Epoch))
			tx, err := ch.TxnManager.BeginOffsetReservations("tx", producer, epoch)
			require.NoError(t, err)
			apply := func(snapshot *transaction.Snapshot, owner string, epoch int64) interface{} {
				payload, err := json.Marshal(map[string]interface{}{"transaction": snapshot, "coordinator_owner": owner, "coordinator_epoch": epoch})
				require.NoError(t, err)
				return f.Apply(&raft.Log{Data: append([]byte("TXN_SYNC:"), payload...)})
			}
			snapshot, ok := ch.TxnManager.Snapshot("tx")
			require.True(t, ok)
			require.Nil(t, apply(snapshot, owner.Owner, owner.Epoch))
			request, err := reservationRequestFor(tx)
			require.NoError(t, err)
			require.NoError(t, cd.PrepareOffsetReservation(request.Group, request.RegistrationEpoch, request.Reservation))
			var decision *transaction.Snapshot
			if committed {
				_, err = ch.TxnManager.PrepareCommit("tx", producer, epoch)
				require.NoError(t, err)
				decision, err = ch.TxnManager.BuildCommittedSnapshot("tx")
			} else {
				decision, err = ch.TxnManager.BuildAbortedSnapshot("tx", producer, epoch)
			}
			require.NoError(t, err)
			require.Nil(t, apply(decision, owner.Owner, owner.Epoch))
			require.Nil(t, f.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node-2","addr":"127.0.0.1:7001","status":"active","lifecycle_protocol":3}`)}))
			require.Nil(t, f.Apply(&raft.Log{Data: []byte(`DEREGISTER:{"id":"node-1"}`)}))
			current, ok := f.GetTransactionCoordinator("tx")
			require.True(t, ok)
			require.Equal(t, "node-2", current.Owner)
			require.Greater(t, current.Epoch, owner.Epoch)
			ch.Cluster = newCoordinatorRoutingHandler("node-2", f, cd).Cluster
			f.SetTransactionManager(ch.TxnManager)
			rm := ch.Cluster.RaftManager.(*coordinatorRoutingRaftManager)
			rm.isLeader, rm.state = true, f
			installRoutingOffsetsTopology(t, f, "node-2")
			ch.Config.EnabledDistribution = true
			cd.SetOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
				return appendStandaloneReservationMetadata(tm, record)
			})
			tx, err = ch.TxnManager.Status("tx")
			require.NoError(t, err)
			require.Equal(t, owner.Epoch, tx.CoordinatorEpoch)
			checkpoint, err := ch.TxnManager.BuildOffsetReservationsResolvedSnapshot("tx")
			require.NoError(t, err)
			// New ownership does not authorize rewriting the earlier decision,
			// staged output, request assignments, or its marker epoch.
			for _, mutate := range []func(*transaction.Snapshot){
				func(s *transaction.Snapshot) { s.State = transaction.StateOpen },
				func(s *transaction.Snapshot) { s.CoordinatorEpoch++ },
				func(s *transaction.Snapshot) { s.Revision++ },
				func(s *transaction.Snapshot) { s.Messages = []transaction.MessageOperation{{Topic: "input"}} },
				func(s *transaction.Snapshot) { s.SequenceByPartition = map[string]uint64{"forged": 1} },
				func(s *transaction.Snapshot) {
					s.RequestAssignments = map[string]transaction.RequestAssignment{"forged": {Topic: "input"}}
				},
			} {
				forged := *checkpoint
				mutate(&forged)
				require.False(t, ch.TxnManager.IsOffsetReservationCleanupSnapshot(&forged))
				require.NotNil(t, apply(&forged, current.Owner, current.Epoch))
			}
			require.NotNil(t, apply(checkpoint, owner.Owner, owner.Epoch), "previous owner stays fenced")
			request.CoordinatorOwner = owner.Owner
			request.TransactionRevision = decision.Revision
			action := "abort"
			if committed {
				action = "commit"
			}
			require.ErrorContains(t, ch.applyTransactionReservation(request, action), "coordinator fenced")
			require.NoError(t, ch.resolveAndCheckpointTransactionReservations(tx))
			final, err := ch.TxnManager.Status("tx")
			require.NoError(t, err)
			require.Equal(t, decision.State, final.State)
			require.Equal(t, owner.Epoch, final.CoordinatorEpoch)
			require.False(t, final.OffsetReservationsPending)
			require.Equal(t, committed, final.OffsetsMaterialized)
			require.Nil(t, apply(checkpoint, current.Owner, current.Epoch), "checkpoint retry remains idempotent")
			offset, _, err := cd.GetStableOffset("workers", "input", 0)
			require.NoError(t, err)
			want := uint64(0)
			if committed {
				want = 1
			}
			require.Equal(t, want, offset)
		})
	}
}
