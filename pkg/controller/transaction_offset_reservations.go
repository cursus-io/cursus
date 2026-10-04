package controller

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
)

func (ch *CommandHandler) transactionGroupRegistrationEpoch(group, topic string, partition int) (uint64, error) {
	if ch.Cluster == nil || ch.Cluster.Router == nil {
		return 0, fmt.Errorf("group coordinator unavailable")
	}
	cmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s include_found=true include_epoch=true", topic, partition, group)
	response, err := ch.Cluster.Router.ForwardToCoordinator(group, cmd)
	if err != nil {
		return 0, err
	}
	if !strings.HasPrefix(response, "OK ") {
		return 0, fmt.Errorf("%s", response)
	}
	fields := parseKeyValueArgs(strings.TrimPrefix(response, "OK "))
	epoch, err := strconv.ParseUint(fields["registration_epoch"], 10, 64)
	if err != nil || epoch == 0 {
		return 0, fmt.Errorf("authoritative group epoch unavailable")
	}
	return epoch, nil
}

func (ch *CommandHandler) requireOffsetReservationProtocol() error {
	if !ch.isDistributed() {
		return nil
	}
	if ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil {
		return fmt.Errorf("offset reservation protocol metadata unavailable")
	}
	brokers := ch.Cluster.RaftManager.GetFSM().GetBrokers()
	if len(brokers) == 0 {
		return fmt.Errorf("offset reservation broker registry unavailable")
	}
	for _, broker := range brokers {
		if broker.LifecycleProtocol < fsm.OffsetReservationsProtocolVersion {
			return fmt.Errorf("offset reservations require broker protocol %d on every registered broker", fsm.OffsetReservationsProtocolVersion)
		}
	}
	return nil
}

type transactionReservationRequest struct {
	Group                    string                                   `json:"group"`
	RegistrationEpoch        uint64                                   `json:"registration_epoch"`
	Reservation              coordinator.TransactionOffsetReservation `json:"reservation"`
	CoordinatorOwner         string                                   `json:"coordinator_owner"`
	CoordinatorEpoch         int64                                    `json:"coordinator_epoch"`
	DecisionCoordinatorEpoch int64                                    `json:"decision_coordinator_epoch"`
	TransactionRevision      uint64                                   `json:"transaction_revision"`
}

func reservationRequestFor(tx *transaction.Transaction) (transactionReservationRequest, error) {
	if tx == nil || !tx.OffsetReservationsPending || len(tx.Offsets) == 0 {
		return transactionReservationRequest{}, fmt.Errorf("transaction has no pending offset reservation")
	}
	scope := tx.Offsets[0]
	if scope.RegistrationEpoch == 0 {
		return transactionReservationRequest{}, fmt.Errorf("transaction offset reservation requires a group epoch")
	}
	request := transactionReservationRequest{Group: scope.Group, RegistrationEpoch: scope.RegistrationEpoch,
		CoordinatorEpoch: tx.CoordinatorEpoch, DecisionCoordinatorEpoch: tx.CoordinatorEpoch, TransactionRevision: tx.Revision,
		Reservation: coordinator.TransactionOffsetReservation{TransactionalID: tx.ID, ProducerID: tx.Producer, ProducerEpoch: tx.Epoch, MemberID: scope.Member, Generation: scope.Generation}}
	for _, op := range tx.Offsets {
		if op.Group != scope.Group || op.RegistrationEpoch != scope.RegistrationEpoch || op.Member != scope.Member || op.Generation != scope.Generation {
			return transactionReservationRequest{}, fmt.Errorf("transaction offset reservation scope mismatch")
		}
		request.Reservation.Offsets = append(request.Reservation.Offsets, coordinator.ReservedTransactionOffset{Topic: op.Topic, Partition: op.Partition, Offset: op.Offset})
	}
	return request, nil
}

func (ch *CommandHandler) routeTransactionReservation(tx *transaction.Transaction, action string) error {
	request, err := reservationRequestFor(tx)
	if err != nil {
		return err
	}
	if !ch.isDistributed() {
		return ch.applyTransactionReservationWithWait(request, action)
	}
	if ch.Cluster.Router == nil {
		return fmt.Errorf("transaction reservation router unavailable")
	}
	owner, _, epoch, err := ch.Cluster.Router.FindTransactionCoordinator(tx.ID)
	if err != nil {
		return err
	}
	terminal := tx.State == transaction.StateCommitted || tx.State == transaction.StateAborted
	if owner != ch.Cluster.Router.BrokerID() || (!terminal && epoch != tx.CoordinatorEpoch) {
		return fmt.Errorf("transaction reservation coordinator fenced")
	}
	request.CoordinatorOwner = owner
	request.CoordinatorEpoch = epoch
	groupOwner, _, err := ch.Cluster.Router.FindCoordinator(request.Group)
	if err != nil {
		return err
	}
	if groupOwner == owner {
		if _, local, err := ch.checkCoordinator(request.Group); err != nil || !local {
			return fmt.Errorf("group coordinator unavailable")
		}
		return ch.applyTransactionReservationWithWait(request, action)
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf("BATCH_COMMIT group=%s reservation_action=%s reservation=%s", request.Group, action, base64.RawURLEncoding.EncodeToString(payload))
	response, err := ch.Cluster.Router.ForwardToCoordinator(request.Group, cmd)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(response, "OK") {
		return fmt.Errorf("%s", response)
	}
	return nil
}

func (ch *CommandHandler) handleTransactionReservationCommand(cmd string, ctx *ClientContext) string {
	input := decodeCommandInput(cmd)
	if response := ch.authorizeInternalCommand("BATCH_COMMIT", input, ctx); response != "" {
		return response
	}
	encoded, err := base64.RawURLEncoding.DecodeString(input.Args["reservation"])
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_offset_prepare_failed reason=%q", "invalid reservation encoding")
	}
	var request transactionReservationRequest
	if err := json.Unmarshal(encoded, &request); err != nil || request.Group == "" || request.Group != input.Args["group"] {
		return fmt.Sprintf("ERROR: transaction_offset_prepare_failed reason=%q", "invalid reservation request")
	}
	if ch.isDistributed() {
		address, local, err := ch.checkCoordinator(request.Group)
		if err != nil {
			return coordinatorUnavailableResponse
		}
		if !local {
			return notCoordinatorResponse(address)
		}
	}
	if err := ch.applyTransactionReservationWithWait(request, input.Args["reservation_action"]); err != nil {
		return fmt.Sprintf("ERROR: transaction_offset_prepare_failed reason=%q", err.Error())
	}
	return "OK"
}

var errReservationStateNotApplied = errors.New("transaction reservation state not yet applied")

// A leader's Raft acknowledgement may precede the group owner's local apply.
// Retry only that transient condition; each attempt revalidates ownership and
// the durable payload. Invalid identities and conflicting decisions fail now.
func (ch *CommandHandler) applyTransactionReservationWithWait(request transactionReservationRequest, action string) error {
	deadline := time.Now().Add(DefaultFSMApplyTimeout)
	for {
		err := ch.applyTransactionReservation(request, action)
		if !errors.Is(err, errReservationStateNotApplied) || !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Both remote and local requests must agree with this broker's replicated
// transaction decision. A lagging replica returns an error for retry instead
// of trusting an unverified commit/abort flag supplied by the request.
func (ch *CommandHandler) applyTransactionReservation(request transactionReservationRequest, action string) error {
	if ch.Coordinator == nil || ch.TxnManager == nil {
		return fmt.Errorf("transaction reservation coordinator unavailable")
	}
	if ch.isDistributed() {
		if ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil || ch.Cluster.Router == nil {
			return fmt.Errorf("offset reservation protocol metadata unavailable")
		}
		owner, _, epoch, err := ch.Cluster.Router.FindTransactionCoordinator(request.Reservation.TransactionalID)
		if err != nil {
			return err
		}
		if request.CoordinatorOwner != owner || request.CoordinatorEpoch != epoch {
			return fmt.Errorf("transaction reservation coordinator fenced")
		}
		if !ch.Cluster.RaftManager.GetFSM().OffsetReservationsEnabled() {
			return fmt.Errorf("%w: protocol activation", errReservationStateNotApplied)
		}
		// Ownership can move while waiting for transaction replication. A
		// receiver must still own and recover this group before writing its
		// reservation checkpoint on every attempt.
		if _, local, err := ch.checkCoordinator(request.Group); err != nil || !local {
			return fmt.Errorf("transaction reservation group coordinator unavailable or fenced")
		}
	}
	tx, err := ch.TxnManager.Status(request.Reservation.TransactionalID)
	if err != nil {
		if ch.isDistributed() {
			return fmt.Errorf("%w: transaction unavailable", errReservationStateNotApplied)
		}
		return err
	}
	if tx.CoordinatorEpoch > request.DecisionCoordinatorEpoch {
		return fmt.Errorf("transaction reservation coordinator fenced")
	}
	if tx.Revision < request.TransactionRevision || tx.CoordinatorEpoch < request.DecisionCoordinatorEpoch {
		return fmt.Errorf("%w: decision revision", errReservationStateNotApplied)
	}
	if !tx.OffsetReservationsPending && tx.Producer == request.Reservation.ProducerID && tx.Epoch == request.Reservation.ProducerEpoch && tx.Revision > request.TransactionRevision &&
		((action == "commit" && tx.State == transaction.StateCommitted && tx.OffsetsMaterialized) || (action == "abort" && tx.State == transaction.StateAborted)) {
		return nil // A duplicate cleanup may arrive after its checkpoint.
	}
	expected, err := reservationRequestFor(tx)
	if err != nil {
		return err
	}
	if request.Group != expected.Group || request.RegistrationEpoch != expected.RegistrationEpoch || !reflect.DeepEqual(request.Reservation, expected.Reservation) {
		return fmt.Errorf("transaction reservation does not match durable transaction")
	}
	switch action {
	case "prepare":
		if tx.State != transaction.StateOpen && tx.State != transaction.StatePrepareCommit {
			return fmt.Errorf("transaction is not preparing a commit")
		}
		return ch.Coordinator.PrepareOffsetReservation(request.Group, request.RegistrationEpoch, request.Reservation)
	case "commit", "abort":
		committed := action == "commit"
		if (committed && tx.State != transaction.StateCommitted) || (!committed && tx.State != transaction.StateAborted) {
			return fmt.Errorf("transaction reservation lacks matching final decision")
		}
		return ch.Coordinator.ResolveOffsetReservation(request.Group, request.RegistrationEpoch, tx.ID, tx.Producer, tx.Epoch, committed)
	default:
		return fmt.Errorf("invalid transaction reservation action")
	}
}

func (ch *CommandHandler) resolveAndCheckpointTransactionReservations(tx *transaction.Transaction) error {
	if !tx.OffsetReservationsPending {
		return nil
	}
	action := "abort"
	if tx.State == transaction.StateCommitted {
		action = "commit"
	} else if tx.State != transaction.StateAborted {
		return fmt.Errorf("transaction reservation has no final decision")
	}
	if err := ch.routeTransactionReservation(tx, action); err != nil {
		return err
	}
	checkpoint, err := ch.TxnManager.BuildOffsetReservationsResolvedSnapshot(tx.ID)
	if err != nil {
		return err
	}
	return ch.persistFinalTransactionDecision(checkpoint)
}
