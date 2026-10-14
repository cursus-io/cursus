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
	g.restoreInstalling = true
	g.logRepairGeneration, g.logRepairLease = g.requiredGeneration, g.lease
	g.notifyLocked()
	return g.requiredGeneration, nil
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
