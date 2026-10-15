package coordinator

import "fmt"

// BeginConsumerLogRepair invalidates the complete disk overlay immediately
// before a possible log rewrite. The caller owns the exclusive admission lease
// and final metadata/partition fences. Live repair never uses the unknown
// original-startup compatibility exception.
func (c *Coordinator) BeginConsumerLogRepair(partition int) (uint64, error) {
	if c == nil || c.standalone {
		return 0, ErrConsumerMetadataRecoveryStandalone
	}
	if err := c.ValidateConsumerMetadataBootstrapEligibility(partition); err != nil {
		return 0, err
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.owned || g.restoreInstalling || g.restoreError != nil {
		return 0, fmt.Errorf("consumer log repair requires an available exclusive recovery lease")
	}
	g.requiredGeneration++
	if g.pendingLogRepairs == nil {
		g.pendingLogRepairs = make(map[int]uint64)
	}
	g.pendingLogRepairs[partition] = g.requiredGeneration
	g.restoreInstalling = true
	g.logRepairGeneration, g.logRepairLease = g.requiredGeneration, g.lease
	g.notifyLocked()
	return g.requiredGeneration, nil
}

// PendingConsumerLogRepair is a local admission requirement, never snapshot or
// Raft authority. Only validated full replay may discharge it.
func (c *Coordinator) PendingConsumerLogRepair(partition int) uint64 {
	if c == nil {
		return 0
	}
	c.consumerAdmission.mu.Lock()
	defer c.consumerAdmission.mu.Unlock()
	return c.consumerAdmission.pendingLogRepairs[partition]
}

// ConsumerLogRepairValidator binds replay to actual receiver verification.
type ConsumerLogRepairValidator interface {
	ValidateConsumerLogRepair(partition int, repairGeneration uint64) error
}

type ConsumerLogRepairPublisher interface {
	AcquireConsumerLogRepairPublication(map[int]uint64) (ConsumerLogRepairPublication, error)
}

// ConsumerLogRepairPublication owns partition verification guards acquired
// before coordinator locks. Publish changes only already-locked scalar/map state.
type ConsumerLogRepairPublication interface {
	Publish(func()) error
	Close()
}

func (c *Coordinator) pendingConsumerLogRepairs() map[int]uint64 {
	c.consumerAdmission.mu.Lock()
	defer c.consumerAdmission.mu.Unlock()
	repairs := make(map[int]uint64, len(c.consumerAdmission.pendingLogRepairs))
	for p, g := range c.consumerAdmission.pendingLogRepairs {
		repairs[p] = g
	}
	return repairs
}

func validateConsumerLogRepairs(validator ConsumerLogRepairValidator, repairs map[int]uint64) error {
	if len(repairs) != 0 && validator == nil {
		return fmt.Errorf("consumer log repair requires verified local authority")
	}
	for p, g := range repairs {
		if err := validator.ValidateConsumerLogRepair(p, g); err != nil {
			return err
		}
	}
	return nil
}

// FinishConsumerLogRepair leaves full strict replay required after lease release.
// Pass a failure only for uncertain/partial mutation; a clean stale rejection
// leaves a retryable replay requirement. Neither unrelated attempts nor release
// may clear a persistent failure. A failed physical repair requires recovery of
// the unavailable partition before another runtime generation can be completed.
func (c *Coordinator) FinishConsumerLogRepair(generation uint64, failure error) {
	if c == nil || c.standalone {
		return
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.owned || generation == 0 || generation != g.requiredGeneration || generation != g.logRepairGeneration || g.lease != g.logRepairLease {
		return
	}
	g.restoreInstalling = false
	g.restoreError = failure
	g.logRepairGeneration, g.logRepairLease = 0, 0
	g.notifyLocked()
}
