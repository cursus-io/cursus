package topic

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
)

// ValidateReconciliationRecord checks the exact retained bytes and excludes
// genuine storage recovery failures. Callers must hold replication ownership.
func (p *Partition) ValidateReconciliationRecord(marker error, message types.Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	if p.closed || p.snapshotRecovery || p.recoveryErr != nil || p.reconciliationErr != marker {
		return fmt.Errorf("stream reconciliation recovery state changed or unavailable")
	}
	messages, err := p.dh.ReadMessages(message.Offset, 1)
	if err != nil {
		return err
	}
	if len(messages) != 1 || !sameReplicatedMessage(messages[0], message) {
		return fmt.Errorf("stream reconciliation retained record conflicts with original append")
	}
	return nil
}

// CompleteReconciliation clears only a caller-validated outcome after its exact
// retained tail is committed. It never truncates, rewrites, or advances HWM.
func (p *Partition) CompleteReconciliation(marker error, hwm uint64) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	if marker == nil || p.reconciliationErr != marker || p.recoveryErr != nil || p.snapshotRecovery || p.closed ||
		!p.hwmAuthoritative || p.HWM != hwm || p.LEO.Load() != hwm {
		return fmt.Errorf("stream reconciliation is not committed at the verified boundary")
	}
	p.reconciliationErr = nil
	return nil
}
