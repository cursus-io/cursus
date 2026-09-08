package transaction

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

const DefaultCoordinatorShardCount = 50

// CoordinatorShard maps a transactional ID to a stable logical coordinator
// shard. Shard ownership may move between brokers, but the mapping itself does
// not change with cluster membership.
func CoordinatorShard(transactionalID string) int {
	return CoordinatorShardForCount(transactionalID, DefaultCoordinatorShardCount)
}

// CoordinatorShardForCount maps a transactional ID using the cluster's
// immutable logical coordinator shard count.
func CoordinatorShardForCount(transactionalID string, shardCount int) int {
	if shardCount <= 0 {
		shardCount = DefaultCoordinatorShardCount
	}
	digest := sha256.Sum256([]byte(transactionalID))
	shard := binary.BigEndian.Uint64(digest[:8]) % uint64(shardCount)
	return int(shard) // #nosec G115 -- modulo guarantees shard is less than the positive int shardCount.
}

type State string

type Mode string

const (
	StateOpen          State = "open"
	StateCommitting    State = "committing"
	StatePrepareCommit State = "prepare_commit"
	StatePrepareAbort  State = "prepare_abort"
	StateCommitted     State = "committed"
	StateAborted       State = "aborted"

	ModeLegacy       Mode = "legacy"
	ModeProcessingV1 Mode = "transactional_processing_v1"
)

var ErrProducerReinitializationRequired = errors.New("producer reinitialization required")

type MessageOperation struct {
	Topic     string
	Partition int
	Message   types.Message
}

type OffsetOperation struct {
	Topic             string
	Group             string
	Member            string
	Generation        int
	Partition         int
	Offset            uint64
	RegistrationEpoch uint64 `json:"registration_epoch,omitempty"`
}

type Participant struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

type Transaction struct {
	ID                  string
	Mode                Mode
	Producer            string
	Epoch               int64
	CoordinatorEpoch    int64
	Revision            uint64
	Ready               bool
	Expired             bool
	OffsetsMaterialized bool
	State               State
	Messages            []MessageOperation
	Offsets             []OffsetOperation
	Participants        []Participant
	Deadline            time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type Snapshot struct {
	ID                  string             `json:"id"`
	Mode                Mode               `json:"mode,omitempty"`
	Producer            string             `json:"producer"`
	Epoch               int64              `json:"epoch"`
	CoordinatorEpoch    int64              `json:"coordinator_epoch,omitempty"`
	Revision            uint64             `json:"revision,omitempty"`
	Ready               bool               `json:"ready,omitempty"`
	Expired             bool               `json:"expired,omitempty"`
	OffsetsMaterialized bool               `json:"offsets_materialized,omitempty"`
	State               State              `json:"state"`
	Messages            []MessageOperation `json:"messages,omitempty"`
	Offsets             []OffsetOperation  `json:"offsets,omitempty"`
	Participants        []Participant      `json:"participants,omitempty"`
	Deadline            time.Time          `json:"deadline,omitempty"`
	CreatedAt           time.Time          `json:"created_at"`
	UpdatedAt           time.Time          `json:"updated_at"`
}

type Manager struct {
	shards              []managerShard
	expiration          time.Duration
	committedOffsetMu   sync.RWMutex
	committedOffsets    map[committedOffsetKey]uint64
	recoveryScanMu      sync.Mutex
	preparedShardCursor int
	timeoutShardCursor  int
}

type committedOffsetKey struct {
	group             string
	topic             string
	partition         int
	registrationEpoch uint64
}

func NewManager() *Manager {
	return NewManagerWithExpiration(7 * 24 * time.Hour)
}

func NewManagerWithExpiration(expiration time.Duration) *Manager {
	return NewManagerWithExpirationAndShards(expiration, DefaultCoordinatorShardCount)
}

func NewManagerWithExpirationAndShards(expiration time.Duration, shardCount int) *Manager {
	if expiration <= 0 {
		expiration = 7 * 24 * time.Hour
	}
	if shardCount <= 0 {
		shardCount = DefaultCoordinatorShardCount
	}
	m := &Manager{shards: make([]managerShard, shardCount), expiration: expiration, committedOffsets: make(map[committedOffsetKey]uint64)}
	for i := range m.shards {
		m.shards[i] = newManagerShard(expiration)
	}
	return m
}

func (m *Manager) PruneExpired(now time.Time) int {
	if m.expiration <= 0 {
		return 0
	}
	removed := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for len(s.expirations) > 0 && s.expirations[0].deadline.Before(now) {
			id := s.expirations[0].id
			tx := s.txns[id]
			if tx == nil || tx.Expired {
				s.remove(id)
				removed++
				continue
			}
			if expireTransactionLocked(tx, now.Add(-m.expiration), now) {
				s.reindex(tx)
				removed++
				continue
			}
			s.reindex(tx)
		}
		s.mu.Unlock()
	}
	return removed
}

func expireTransactionLocked(tx *Transaction, cutoff, now time.Time) bool {
	if tx == nil || tx.Expired || !tx.UpdatedAt.Before(cutoff) {
		return false
	}
	if tx.State != StateCommitted && tx.State != StateAborted {
		return false
	}
	tx.Messages = nil
	tx.Offsets = nil
	tx.Ready = false
	tx.Expired = true
	tx.Revision++
	tx.UpdatedAt = now
	return true
}

func (m *Manager) InitProducer(id string) (string, int64, error) {
	return m.InitProducerWithMode(id, ModeLegacy)
}

func (m *Manager) InitProducerWithMode(id string, mode Mode) (string, int64, error) {
	if id == "" {
		return "", 0, fmt.Errorf("missing transaction id")
	}

	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if mode == "" {
		mode = ModeLegacy
	}
	previous := s.txns[id]
	expireTransactionLocked(previous, now.Add(-m.expiration), now)

	producer := producerIDForTransactionalID(id)
	epoch := int64(0)
	revision := uint64(1)
	if tx := previous; tx != nil {
		if tx.State == StateCommitting || tx.State == StatePrepareCommit || tx.State == StatePrepareAbort {
			return "", 0, fmt.Errorf("transaction %s is committing; retry END_TXN before reinitializing producer", id)
		}
		if tx.Producer != "" {
			producer = tx.Producer
		}
		epoch = tx.Epoch + 1
		revision = tx.Revision + 1
	}

	s.put(&Transaction{
		ID:               id,
		Mode:             mode,
		Producer:         producer,
		Epoch:            epoch,
		CoordinatorEpoch: epoch,
		Revision:         revision,
		Ready:            true,
		State:            StateAborted,
		CreatedAt:        now,
		UpdatedAt:        now,
	})
	return producer, epoch, nil
}

// SetCoordinatorEpoch attaches the durable coordinator-shard epoch to a newly
// initialized v1 transaction before it is replicated.
func (m *Manager) SetCoordinatorEpoch(id string, epoch int64) error {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, ok := s.txns[id]
	if !ok {
		return fmt.Errorf("transaction %s not found", id)
	}
	if tx.Mode != ModeProcessingV1 {
		return nil
	}
	if epoch <= 0 {
		return fmt.Errorf("invalid coordinator epoch %d", epoch)
	}
	tx.CoordinatorEpoch = epoch
	return nil
}

// ReconcileCoordinatorEpochs fences local v1 state after ownership of a shard
// changes in the replicated cluster registry. It deliberately leaves producer
// epochs and transaction revisions unchanged.
func (m *Manager) ReconcileCoordinatorEpochs(epochs map[int]int64, shardCount int) {
	for shardID, epoch := range epochs {
		if shardID < 0 || shardID >= len(m.shards) || epoch <= 0 {
			continue
		}
		s := &m.shards[shardID]
		s.mu.Lock()
		for id := range s.nonTerminal {
			if tx := s.txns[id]; tx != nil && tx.Mode == ModeProcessingV1 {
				tx.CoordinatorEpoch = epoch
			}
		}
		s.mu.Unlock()
	}
}
func (m *Manager) Begin(id, producer string, epoch int64) error {
	return m.BeginWithDeadline(id, producer, epoch, time.Time{})
}

func (m *Manager) BeginWithDeadline(id, producer string, epoch int64, deadline time.Time) error {
	if id == "" {
		return fmt.Errorf("missing transaction id")
	}
	if producer == "" {
		return fmt.Errorf("missing transactional producer")
	}

	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok || tx.Expired {
		return fmt.Errorf("transaction %s is not initialized; call INIT_PRODUCER_ID first", id)
	}
	if tx.Producer != "" && tx.Producer != producer {
		return fmt.Errorf("transaction owner mismatch transactional_id=%s producer=%s requested=%s", id, tx.Producer, producer)
	}
	if epoch < tx.Epoch {
		return fmt.Errorf("producer fenced transactional_id=%s current_epoch=%d requested_epoch=%d", id, tx.Epoch, epoch)
	}
	if epoch > tx.Epoch {
		return fmt.Errorf("producer epoch not initialized transactional_id=%s current_epoch=%d requested_epoch=%d", id, tx.Epoch, epoch)
	}
	if !tx.Ready {
		return fmt.Errorf("%w: transactional_id=%s epoch=%d", ErrProducerReinitializationRequired, id, epoch)
	}
	if tx.State != StateCommitted && tx.State != StateAborted {
		return fmt.Errorf("transaction %s is already active", id)
	}

	now := time.Now()
	s.put(&Transaction{
		ID:               id,
		Mode:             tx.Mode,
		Producer:         producer,
		Epoch:            epoch,
		CoordinatorEpoch: tx.CoordinatorEpoch,
		Revision:         tx.Revision + 1,
		Ready:            false,
		State:            StateOpen,
		Deadline:         deadline,
		CreatedAt:        now,
		UpdatedAt:        now,
	})
	return nil
}

func (m *Manager) AddParticipant(id, producer string, epoch int64, participant Participant, deadline time.Time) error {
	if participant.Topic == "" || participant.Partition < 0 {
		return fmt.Errorf("invalid transaction participant")
	}
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := activeLocked(s, id)
	if err != nil {
		return err
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	if tx.Mode != ModeProcessingV1 {
		return fmt.Errorf("transaction %s does not use transactional processing v1", id)
	}
	for _, existing := range tx.Participants {
		if existing == participant {
			return nil
		}
	}
	tx.Participants = append(tx.Participants, participant)
	if tx.Deadline.IsZero() && !deadline.IsZero() {
		tx.Deadline = deadline
	}
	tx.Revision++
	tx.UpdatedAt = time.Now()
	s.reindex(tx)
	return nil
}

func (m *Manager) AddMessage(id, producer string, epoch int64, op MessageOperation) error {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := activeLocked(s, id)
	if err != nil {
		return err
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	tx.Messages = append(tx.Messages, op)
	tx.Revision++
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) AddOffsets(id, producer string, epoch int64, offsets []OffsetOperation) error {
	if len(offsets) == 0 {
		return fmt.Errorf("no offsets supplied")
	}

	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := activeLocked(s, id)
	if err != nil {
		return err
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	scope := offsets[0]
	if len(tx.Offsets) > 0 {
		scope = tx.Offsets[0]
	}
	for _, op := range offsets {
		topicMismatch := op.Topic != scope.Topic && tx.Mode != ModeProcessingV1
		if topicMismatch || op.Group != scope.Group || op.Member != scope.Member || op.Generation != scope.Generation || op.RegistrationEpoch != scope.RegistrationEpoch {
			return fmt.Errorf(
				"transaction offset scope mismatch: expected topic=%s group=%s member=%s generation=%d registration_epoch=%d",
				scope.Topic, scope.Group, scope.Member, scope.Generation, scope.RegistrationEpoch,
			)
		}
	}

	for _, op := range offsets {
		updated := false
		for i := range tx.Offsets {
			current := &tx.Offsets[i]
			if current.Topic != op.Topic || current.Group != op.Group || current.Partition != op.Partition {
				continue
			}
			if op.Offset < current.Offset {
				return fmt.Errorf(
					"transaction offset regression topic=%s group=%s partition=%d current=%d attempted=%d",
					op.Topic, op.Group, op.Partition, current.Offset, op.Offset,
				)
			}
			*current = op
			updated = true
			break
		}
		if !updated {
			tx.Offsets = append(tx.Offsets, op)
		}
	}
	tx.Revision++
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) PrepareCommit(id, producer string, epoch int64) (*Transaction, error) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	switch tx.State {
	case StateOpen:
		if tx.Mode == ModeProcessingV1 {
			tx.State = StatePrepareCommit
		} else {
			tx.State = StateCommitting
		}
		tx.Revision++
		tx.UpdatedAt = time.Now()
		s.reindex(tx)
		return clone(tx), nil
	case StateCommitting, StatePrepareCommit:
		return clone(tx), nil
	case StateCommitted:
		return clone(tx), nil
	case StateAborted:
		return nil, fmt.Errorf("transaction %s is aborted", id)
	default:
		return nil, fmt.Errorf("transaction %s is %s", id, tx.State)
	}
}

func (m *Manager) Commit(id string) error {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok {
		return fmt.Errorf("transaction %s not found", id)
	}
	if tx.State != StateCommitting && tx.State != StatePrepareCommit {
		return fmt.Errorf("transaction %s is not prepared for commit", id)
	}
	tx.State = StateCommitted
	tx.Revision++
	tx.UpdatedAt = time.Now()
	s.reindex(tx)
	m.indexCommittedOffsets(tx.Offsets)
	return nil
}

// MarkOffsetsMaterialized records that __consumer_offsets now contains the
// ordinary recovery snapshot, allowing staged offset payloads to be retired.
func (m *Manager) MarkOffsetsMaterialized(id string) error {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := s.txns[id]
	if tx == nil || tx.State != StateCommitted {
		return fmt.Errorf("transaction %s is not committed", id)
	}
	if tx.OffsetsMaterialized {
		return nil
	}
	tx.OffsetsMaterialized = true
	tx.Offsets = nil
	tx.Revision++
	tx.UpdatedAt = time.Now()
	s.reindex(tx)
	return nil
}

func (m *Manager) PrepareAbort(id, producer string, epoch int64) (*Transaction, error) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, ok := s.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	switch tx.State {
	case StateOpen:
		tx.State = StatePrepareAbort
		tx.Revision++
		tx.UpdatedAt = time.Now()
		s.reindex(tx)
		return clone(tx), nil
	case StatePrepareAbort, StateAborted:
		return clone(tx), nil
	case StatePrepareCommit, StateCommitting, StateCommitted:
		return nil, fmt.Errorf("transaction %s cannot be aborted from state %s", id, tx.State)
	default:
		return nil, fmt.Errorf("transaction %s cannot be aborted from state %s", id, tx.State)
	}
}

func (m *Manager) Abort(id, producer string, epoch int64) error {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok {
		return fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	if tx.State == StateCommitted {
		return fmt.Errorf("transaction %s is already committed", id)
	}
	if tx.State == StateAborted {
		return nil
	}
	if tx.State == StateCommitting || tx.State == StatePrepareCommit || tx.State == StatePrepareAbort {
		return fmt.Errorf("transaction %s cannot be aborted from state %s", id, tx.State)
	}
	tx.State = StateAborted
	tx.Messages = nil
	tx.Offsets = nil
	tx.Revision++
	tx.UpdatedAt = time.Now()
	s.reindex(tx)
	return nil
}

func (m *Manager) ValidateOwner(id, producer string, epoch int64) (*Transaction, error) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	return clone(tx), nil
}
func (m *Manager) Status(id string) (*Transaction, error) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok || tx.Expired {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	return clone(tx), nil
}

func (m *Manager) Snapshot(id string) (*Snapshot, bool) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, ok := s.txns[id]
	if !ok {
		return nil, false
	}
	return snapshot(tx), true
}

func (m *Manager) TransactionDecision(id string, epoch int64) (string, bool) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok || tx.Expired || tx.Epoch != epoch {
		return "", false
	}
	return string(tx.State), true
}

func (m *Manager) TransactionDecisionWithCoordinatorEpoch(id string, epoch int64) (string, int64, bool) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok || tx.Expired || tx.Epoch != epoch {
		return "", 0, false
	}
	if tx.Mode != ModeProcessingV1 {
		return string(tx.State), 0, true
	}
	return string(tx.State), tx.CoordinatorEpoch, true
}

func (m *Manager) BuildCommittedSnapshot(id string) (*Snapshot, error) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	switch tx.State {
	case StateCommitted:
		return snapshot(tx), nil
	case StateCommitting, StatePrepareCommit:
	default:
		return nil, fmt.Errorf("transaction %s is not prepared for commit", id)
	}
	committed := snapshot(tx)
	committed.State = StateCommitted
	committed.Revision++
	committed.UpdatedAt = time.Now()
	return committed, nil
}
func (m *Manager) BuildAbortedSnapshot(id, producer string, epoch int64) (*Snapshot, error) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	switch tx.State {
	case StateCommitted:
		return nil, fmt.Errorf("transaction %s is already committed", id)
	case StateAborted:
		return snapshot(tx), nil
	case StateCommitting, StatePrepareCommit:
		return nil, fmt.Errorf("transaction %s cannot be aborted from state %s", id, tx.State)
	case StateOpen, StatePrepareAbort:
		aborted := snapshot(tx)
		aborted.State = StateAborted
		aborted.Revision++
		aborted.UpdatedAt = time.Now()
		return aborted, nil
	default:
		return nil, fmt.Errorf("transaction %s cannot be aborted from state %s", id, tx.State)
	}
}

func (m *Manager) TransactionsByState(states ...State) []*Transaction {
	wanted := make(map[State]struct{}, len(states))
	for _, state := range states {
		wanted[state] = struct{}{}
	}

	out := make([]*Transaction, 0)
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for _, tx := range s.txns {
			if tx.Expired {
				continue
			}
			if _, ok := wanted[tx.State]; ok {
				out = append(out, clone(tx))
			}
		}
		s.mu.Unlock()
	}
	return out
}
func (m *Manager) ExportState() map[string]*Snapshot {
	m.lockAllShards()
	defer m.unlockAllShards()
	return m.exportStateLocked()
}

func (m *Manager) lockAllShards() {
	for i := range m.shards {
		m.shards[i].mu.Lock()
	}
}

func (m *Manager) unlockAllShards() {
	for i := len(m.shards) - 1; i >= 0; i-- {
		m.shards[i].mu.Unlock()
	}
}

func (m *Manager) exportStateLocked() map[string]*Snapshot {
	out := make(map[string]*Snapshot)
	for i := range m.shards {
		for id, tx := range m.shards[i].txns {
			out[id] = snapshot(tx)
		}
	}
	return out
}

func (m *Manager) ImportState(state map[string]*Snapshot) error {
	if err := ValidateImportState(state); err != nil {
		return err
	}
	m.lockAllShards()
	defer m.unlockAllShards()
	m.replaceStateLocked(state)
	return nil
}

func (m *Manager) replaceStateLocked(state map[string]*Snapshot) {
	for i := range m.shards {
		m.shards[i].txns = make(map[string]*Transaction)
		m.shards[i].prepared = make(map[string]struct{})
		m.shards[i].nonTerminal = make(map[string]struct{})
		m.shards[i].deadlines = nil
		m.shards[i].deadlineByID = make(map[string]*deadlineItem)
		m.shards[i].expirations = nil
		m.shards[i].expiryByID = make(map[string]*deadlineItem)
	}
	committed := make(map[committedOffsetKey]uint64)
	for id, snap := range state {
		if snap == nil {
			continue
		}
		tx := transactionFromSnapshot(snap)
		m.shards[CoordinatorShardForCount(id, len(m.shards))].put(tx)
		if tx.State == StateCommitted {
			for _, op := range tx.Offsets {
				key := committedOffsetKey{group: op.Group, topic: op.Topic, partition: op.Partition, registrationEpoch: op.RegistrationEpoch}
				if current, ok := committed[key]; !ok || op.Offset > current {
					committed[key] = op.Offset
				}
			}
		}
	}
	m.committedOffsetMu.Lock()
	m.committedOffsets = committed
	m.committedOffsetMu.Unlock()
}

func (m *Manager) ApplySnapshot(snap *Snapshot) {
	if snap == nil || snap.ID == "" {
		return
	}
	s := m.shardForID(snap.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.put(transactionFromSnapshot(snap))
	if snap.State == StateCommitted {
		m.indexCommittedOffsets(snap.Offsets)
	}
}

func (m *Manager) ApplyReplicatedSnapshot(snap *Snapshot) error {
	if err := validateSnapshot(snap); err != nil {
		return err
	}
	s := m.shardForID(snap.ID)
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.txns[snap.ID]
	if !ok || snapshotIsNewer(current, snap) {
		s.put(transactionFromSnapshot(snap))
		if snap.State == StateCommitted {
			m.indexCommittedOffsets(snap.Offsets)
		}
		return nil
	}
	if snapshotsEqual(current, snap) {
		return nil
	}
	// A retried coordinator must re-propose its durable committing snapshot
	// before applying records. If this replica has already applied the exact
	// successor committed decision, the predecessor is an idempotent no-op;
	// it must never regress the terminal state.
	if committedSnapshotSucceeds(current, snap) {
		return nil
	}
	return fmt.Errorf("stale transaction snapshot transactional_id=%s current_epoch=%d current_revision=%d incoming_epoch=%d incoming_revision=%d", snap.ID, current.Epoch, current.Revision, snap.Epoch, snap.Revision)
}

func (m *Manager) indexCommittedOffsets(offsets []OffsetOperation) {
	m.committedOffsetMu.Lock()
	defer m.committedOffsetMu.Unlock()
	for _, op := range offsets {
		key := committedOffsetKey{group: op.Group, topic: op.Topic, partition: op.Partition, registrationEpoch: op.RegistrationEpoch}
		if current, ok := m.committedOffsets[key]; !ok || op.Offset > current {
			m.committedOffsets[key] = op.Offset
		}
	}
}

// CommittedOffset is an O(1) decision-gated view used by the consumer group
// coordinator. Prepared and aborted transactions are never indexed.
func (m *Manager) CommittedOffset(group, topic string, partition int, registrationEpoch uint64) (uint64, bool) {
	m.committedOffsetMu.RLock()
	defer m.committedOffsetMu.RUnlock()
	offset, ok := m.committedOffsets[committedOffsetKey{group: group, topic: topic, partition: partition, registrationEpoch: registrationEpoch}]
	return offset, ok
}

func committedSnapshotSucceeds(current *Transaction, incoming *Snapshot) bool {
	return current != nil && incoming != nil &&
		current.ID == incoming.ID &&
		current.Mode == normalizeMode(incoming.Mode) &&
		current.Producer == incoming.Producer &&
		current.Epoch == incoming.Epoch &&
		current.CoordinatorEpoch == incoming.CoordinatorEpoch &&
		current.State == StateCommitted &&
		(incoming.State == StateCommitting || incoming.State == StatePrepareCommit) &&
		current.Revision == incoming.Revision+1 &&
		current.Ready == incoming.Ready &&
		current.Expired == incoming.Expired &&
		current.OffsetsMaterialized == incoming.OffsetsMaterialized &&
		current.CreatedAt.Equal(incoming.CreatedAt) &&
		reflect.DeepEqual(current.Messages, incoming.Messages) &&
		reflect.DeepEqual(current.Offsets, incoming.Offsets) &&
		reflect.DeepEqual(current.Participants, incoming.Participants) &&
		current.Deadline.Equal(incoming.Deadline)
}

func snapshotIsNewer(current *Transaction, incoming *Snapshot) bool {
	if incoming.Epoch != current.Epoch {
		return incoming.Epoch > current.Epoch
	}
	if incoming.Revision != current.Revision {
		return incoming.Revision > current.Revision
	}
	return false
}

// ValidateImportState rejects incomplete transaction state before a journal
// or Raft snapshot can mutate the live manager.
func ValidateImportState(state map[string]*Snapshot) error {
	for id, snap := range state {
		if snap == nil {
			return fmt.Errorf("transaction snapshot %q is nil", id)
		}
		if snap.ID != id {
			return fmt.Errorf("transaction snapshot key %q does not match id %q", id, snap.ID)
		}
		if err := validateSnapshot(snap); err != nil {
			return fmt.Errorf("transaction snapshot %q: %w", id, err)
		}
	}
	return nil
}

func validateSnapshot(snap *Snapshot) error {
	if snap == nil || snap.ID == "" || snap.Producer == "" {
		return fmt.Errorf("invalid transaction snapshot identity")
	}
	if snap.Epoch < 0 {
		return fmt.Errorf("transaction snapshot has negative epoch %d", snap.Epoch)
	}
	if snap.Revision == 0 {
		return fmt.Errorf("transaction snapshot is missing revision; clean bootstrap required")
	}
	switch snap.State {
	case StateOpen, StateCommitting, StatePrepareCommit, StatePrepareAbort, StateCommitted, StateAborted:
	default:
		return fmt.Errorf("transaction snapshot has invalid state %q", snap.State)
	}
	if snap.CreatedAt.IsZero() || snap.UpdatedAt.IsZero() || snap.UpdatedAt.Before(snap.CreatedAt) {
		return fmt.Errorf("transaction snapshot has invalid timestamps")
	}
	return nil
}

func snapshotsEqual(current *Transaction, incoming *Snapshot) bool {
	return current.ID == incoming.ID &&
		current.Mode == normalizeMode(incoming.Mode) &&
		current.Producer == incoming.Producer &&
		current.Epoch == incoming.Epoch &&
		current.CoordinatorEpoch == incoming.CoordinatorEpoch &&
		current.Revision == incoming.Revision &&
		current.Ready == incoming.Ready &&
		current.Expired == incoming.Expired &&
		current.OffsetsMaterialized == incoming.OffsetsMaterialized &&
		current.State == incoming.State &&
		current.CreatedAt.Equal(incoming.CreatedAt) &&
		current.UpdatedAt.Equal(incoming.UpdatedAt) &&
		reflect.DeepEqual(current.Messages, incoming.Messages) &&
		reflect.DeepEqual(current.Offsets, incoming.Offsets) &&
		reflect.DeepEqual(current.Participants, incoming.Participants) &&
		current.Deadline.Equal(incoming.Deadline)
}

func (m *Manager) Delete(id string) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remove(id)
}

func activeLocked(s *managerShard, id string) (*Transaction, error) {
	tx, ok := s.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if tx.State != StateOpen {
		return nil, fmt.Errorf("transaction %s is %s", id, tx.State)
	}
	return tx, nil
}

func validateOwner(tx *Transaction, producer string, epoch int64) error {
	if tx.Expired {
		return fmt.Errorf("transaction %s is expired; call INIT_PRODUCER_ID first", tx.ID)
	}
	if producer == "" {
		return fmt.Errorf("missing transactional producer")
	}
	if tx.Producer != producer {
		return fmt.Errorf("transaction owner mismatch transactional_id=%s producer=%s requested=%s", tx.ID, tx.Producer, producer)
	}
	if epoch != tx.Epoch {
		return fmt.Errorf("producer fenced transactional_id=%s current_epoch=%d requested_epoch=%d", tx.ID, tx.Epoch, epoch)
	}
	return nil
}

func clone(tx *Transaction) *Transaction {
	if tx == nil {
		return nil
	}
	out := *tx
	out.Messages = append([]MessageOperation(nil), tx.Messages...)
	out.Offsets = append([]OffsetOperation(nil), tx.Offsets...)
	out.Participants = append([]Participant(nil), tx.Participants...)
	return &out
}

func snapshot(tx *Transaction) *Snapshot {
	if tx == nil {
		return nil
	}
	return &Snapshot{
		ID:                  tx.ID,
		Mode:                tx.Mode,
		Producer:            tx.Producer,
		Epoch:               tx.Epoch,
		CoordinatorEpoch:    tx.CoordinatorEpoch,
		Revision:            tx.Revision,
		Ready:               tx.Ready,
		Expired:             tx.Expired,
		OffsetsMaterialized: tx.OffsetsMaterialized,
		State:               tx.State,
		Messages:            append([]MessageOperation(nil), tx.Messages...),
		Offsets:             append([]OffsetOperation(nil), tx.Offsets...),
		Participants:        append([]Participant(nil), tx.Participants...),
		Deadline:            tx.Deadline,
		CreatedAt:           tx.CreatedAt,
		UpdatedAt:           tx.UpdatedAt,
	}
}

func transactionFromSnapshot(snap *Snapshot) *Transaction {
	return &Transaction{
		ID:                  snap.ID,
		Mode:                normalizeMode(snap.Mode),
		Producer:            snap.Producer,
		Epoch:               snap.Epoch,
		CoordinatorEpoch:    snap.CoordinatorEpoch,
		Revision:            snap.Revision,
		Ready:               snap.Ready,
		Expired:             snap.Expired,
		OffsetsMaterialized: snap.OffsetsMaterialized,
		State:               snap.State,
		Messages:            append([]MessageOperation(nil), snap.Messages...),
		Offsets:             append([]OffsetOperation(nil), snap.Offsets...),
		Participants:        append([]Participant(nil), snap.Participants...),
		Deadline:            snap.Deadline,
		CreatedAt:           snap.CreatedAt,
		UpdatedAt:           snap.UpdatedAt,
	}
}

func normalizeMode(mode Mode) Mode {
	if mode == "" {
		return ModeLegacy
	}
	return mode
}

func producerIDForTransactionalID(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "txn-" + hex.EncodeToString(sum[:8])
}
