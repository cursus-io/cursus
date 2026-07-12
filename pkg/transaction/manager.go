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
	Topic     string
	Group     string
	Partition int
	Offset    uint64
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

	if tx, ok := m.txns[id]; ok && tx.State != StateCommitted && tx.State != StateAborted {
		return fmt.Errorf("transaction %s is already active", id)
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

func (m *Manager) AddMessage(id string, op MessageOperation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.activeLocked(id)
	if err != nil {
		return err
	}
	tx.Messages = append(tx.Messages, op)
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) AddOffsets(id string, offsets []OffsetOperation) error {
	if len(offsets) == 0 {
		return fmt.Errorf("no offsets supplied")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.activeLocked(id)
	if err != nil {
		return err
	}
	tx.Offsets = append(tx.Offsets, offsets...)
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) PrepareCommit(id string) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.activeLocked(id)
	if err != nil {
		return nil, err
	}
	tx.State = StateCommitting
	tx.UpdatedAt = time.Now()
	return clone(tx), nil
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

func (m *Manager) Abort(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return fmt.Errorf("transaction %s not found", id)
	}
	if tx.State == StateCommitted {
		return fmt.Errorf("transaction %s is already committed", id)
	}
	tx.State = StateAborted
	tx.Messages = nil
	tx.Offsets = nil
	tx.UpdatedAt = time.Now()
	return nil
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

func clone(tx *Transaction) *Transaction {
	if tx == nil {
		return nil
	}
	out := *tx
	out.Messages = append([]MessageOperation(nil), tx.Messages...)
	out.Offsets = append([]OffsetOperation(nil), tx.Offsets...)
	return &out
}

func ToCoordinatorOffsets(ops []OffsetOperation) []coordinator.OffsetItem {
	out := make([]coordinator.OffsetItem, 0, len(ops))
	for _, op := range ops {
		out = append(out, coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
	}
	return out
}
