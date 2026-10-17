package coordinator

import "fmt"

// ConsumerRecoveryRequirement is detached runtime state, not snapshot authority.
// Reading it never waits for lifecycle, group, FSM, or storage locks.
type ConsumerRecoveryRequirement struct {
	RequiredGeneration, CompletedGeneration uint64
	StartupPending, Installing              bool
	Failure                                 error
}

func (r ConsumerRecoveryRequirement) Pending() bool {
	return r.StartupPending || r.Installing || r.Failure != nil || r.RequiredGeneration != r.CompletedGeneration
}

func (c *Coordinator) ConsumerRecoveryRequirement() ConsumerRecoveryRequirement {
	if c == nil || c.standalone {
		return ConsumerRecoveryRequirement{}
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	return ConsumerRecoveryRequirement{g.requiredGeneration, g.completedGeneration, g.startupPending, g.restoreInstalling, g.restoreError}
}

// ValidateConsumerRestoreOrigin is FSM-specific: ordinary import APIs keep their
// historical semantics. Only the original startup can accept unknown authority.
func (c *Coordinator) ValidateConsumerRestoreOrigin(envelope *ConsumerRecoveryEnvelope) error {
	if c == nil || c.standalone {
		return nil
	}
	if envelope != nil && envelope.KnownComplete {
		return nil
	}
	if !c.ConsumerRecoveryRequirement().StartupPending {
		return ErrConsumerMetadataBootstrapUnknown
	}
	return nil
}

// BeginConsumerRestore invalidates preparation before the first FSM mutation.
// It deliberately does not drain admission or invoke any callback.
func (c *Coordinator) BeginConsumerRestore() uint64 {
	if c == nil || c.standalone {
		return 0
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requiredGeneration++
	g.restoreInstalling = true
	g.restoreError = nil
	g.notifyLocked()
	return g.requiredGeneration
}

func (c *Coordinator) FinishConsumerRestore(generation uint64, err error) {
	if c == nil || c.standalone {
		return
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if generation != g.requiredGeneration {
		return
	}
	g.restoreInstalling = false
	g.restoreError = err
	g.notifyLocked()
}

// CompleteConsumerRecovery requires a full strict install under this lease.
// Scoped bootstrap and ordinary reload never satisfy a runtime requirement.
func (c *Coordinator) CompleteConsumerRecovery(generation uint64) error {
	if c == nil || c.standalone {
		return ErrConsumerMetadataRecoveryStandalone
	}
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.pendingLogRepairs) != 0 || !g.owned || generation != g.requiredGeneration || g.installedGeneration != generation || g.installedLease != g.lease || g.installedRevision != c.authorityRevision || g.restoreInstalling || g.restoreError != nil {
		return fmt.Errorf("consumer metadata recovery generation is stale or incomplete")
	}
	g.completedGeneration = generation
	g.startupPending = false
	g.notifyLocked()
	return nil
}
