package transaction

import (
	"fmt"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
)

type State string

const (
	StateOpen       State = "open"
	StateCommitting State = "committing"
	StateCommitted  State = "committed"
	StateAborted    State = "aborted"
)

type MessageOperation struct {
	Topic     string
	Partition int
	Message   types.Message
}

type OffsetOperation struct {
	Topic      string
	Group      string
	Member     string
	Generation int
	Partition  int
	Offset     uint64
}

type Transaction struct {
	ID        string
	Producer  string
	Epoch     int64
	State     State
	Messages  []MessageOperation
	Offsets   []OffsetOperation
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Snapshot struct {
	ID        string             `json:"id"`
	Producer  string             `json:"producer"`
	Epoch     int64              `json:"epoch"`
	State     State              `json:"state"`
	Messages  []MessageOperation `json:"messages,omitempty"`
	Offsets   []OffsetOperation  `json:"offsets,omitempty"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type Manager struct {
	mu   sync.Mutex
	txns map[string]*Transaction
}

func NewManager() *Manager {
	return &Manager{txns: make(map[string]*Transaction)}
}

func (m *Manager) Begin(id, producer string, epoch int64) error {
	if id == "" {
		return fmt.Errorf("missing transaction id")
	}
	if producer == "" {
		return fmt.Errorf("missing transactional producer")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if tx, ok := m.txns[id]; ok {
		if epoch < tx.Epoch {
			return fmt.Errorf("producer fenced transactional_id=%s current_epoch=%d requested_epoch=%d", id, tx.Epoch, epoch)
		}
		if tx.State != StateCommitted && tx.State != StateAborted {
			return fmt.Errorf("transaction %s is already active", id)
		}
	}

	now := time.Now()
	m.txns[id] = &Transaction{
		ID:        id,
		Producer:  producer,
		Epoch:     epoch,
		State:     StateOpen,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return nil
}

func (m *Manager) AddMessage(id, producer string, epoch int64, op MessageOperation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.activeLocked(id)
	if err != nil {
		return err
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	tx.Messages = append(tx.Messages, op)
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) AddOffsets(id, producer string, epoch int64, offsets []OffsetOperation) error {
	if len(offsets) == 0 {
		return fmt.Errorf("no offsets supplied")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.activeLocked(id)
	if err != nil {
		return err
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	tx.Offsets = append(tx.Offsets, offsets...)
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) PrepareCommit(id, producer string, epoch int64) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	switch tx.State {
	case StateOpen:
		tx.State = StateCommitting
		tx.UpdatedAt = time.Now()
		return clone(tx), nil
	case StateCommitting:
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
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return fmt.Errorf("transaction %s not found", id)
	}
	if tx.State != StateCommitting {
		return fmt.Errorf("transaction %s is not prepared for commit", id)
	}
	tx.State = StateCommitted
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) Abort(id, producer string, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
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
	tx.State = StateAborted
	tx.Messages = nil
	tx.Offsets = nil
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) ValidateOwner(id, producer string, epoch int64) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	return clone(tx), nil
}
func (m *Manager) Status(id string) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	return clone(tx), nil
}

func (m *Manager) TransactionsByState(states ...State) []*Transaction {
	wanted := make(map[State]struct{}, len(states))
	for _, state := range states {
		wanted[state] = struct{}{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]*Transaction, 0)
	for _, tx := range m.txns {
		if _, ok := wanted[tx.State]; ok {
			out = append(out, clone(tx))
		}
	}
	return out
}
func (m *Manager) ExportState() map[string]*Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make(map[string]*Snapshot, len(m.txns))
	for id, tx := range m.txns {
		out[id] = snapshot(tx)
	}
	return out
}

func (m *Manager) ImportState(state map[string]*Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.txns = make(map[string]*Transaction, len(state))
	for id, snap := range state {
		if snap == nil {
			continue
		}
		m.txns[id] = transactionFromSnapshot(snap)
	}
}

func (m *Manager) ApplySnapshot(snap *Snapshot) {
	if snap == nil || snap.ID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txns[snap.ID] = transactionFromSnapshot(snap)
}

func (m *Manager) activeLocked(id string) (*Transaction, error) {
	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if tx.State != StateOpen {
		return nil, fmt.Errorf("transaction %s is %s", id, tx.State)
	}
	return tx, nil
}

func validateOwner(tx *Transaction, producer string, epoch int64) error {
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
	return &out
}

func snapshot(tx *Transaction) *Snapshot {
	if tx == nil {
		return nil
	}
	return &Snapshot{
		ID:        tx.ID,
		Producer:  tx.Producer,
		Epoch:     tx.Epoch,
		State:     tx.State,
		Messages:  append([]MessageOperation(nil), tx.Messages...),
		Offsets:   append([]OffsetOperation(nil), tx.Offsets...),
		CreatedAt: tx.CreatedAt,
		UpdatedAt: tx.UpdatedAt,
	}
}

func transactionFromSnapshot(snap *Snapshot) *Transaction {
	return &Transaction{
		ID:        snap.ID,
		Producer:  snap.Producer,
		Epoch:     snap.Epoch,
		State:     snap.State,
		Messages:  append([]MessageOperation(nil), snap.Messages...),
		Offsets:   append([]OffsetOperation(nil), snap.Offsets...),
		CreatedAt: snap.CreatedAt,
		UpdatedAt: snap.UpdatedAt,
	}
}

func ToCoordinatorOffsets(ops []OffsetOperation) []coordinator.OffsetItem {
	out := make([]coordinator.OffsetItem, 0, len(ops))
	for _, op := range ops {
		out = append(out, coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
	}
	return out
}
