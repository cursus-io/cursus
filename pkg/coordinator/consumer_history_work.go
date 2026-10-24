package coordinator

import "fmt"

// ConsumerHistoryWork owns only a detached compatibility baseline, never live
// group/member state. Capture without changing admission; scan the independent
// baseline and validate under a publication lease. Work is single-threaded and
// process-local.
type ConsumerHistoryWork struct {
	owner *Coordinator
	stamp consumerMetadataReplayStamp
	proof *Coordinator
}

// TryCaptureConsumerHistoryWork never closes live admission. Nonblocking lock
// acquisition avoids waiting for a writer that needs Raft while a caller is
// observing topology. Only the independent compatibility baseline is copied;
// every authority/restore change is rechecked by the final publication lease.
func (c *Coordinator) TryCaptureConsumerHistoryWork() (*ConsumerHistoryWork, error) {
	if c == nil || c.standalone {
		return nil, ErrConsumerMetadataRecoveryStandalone
	}
	if !c.authorityMu.TryLock() {
		return nil, ErrConsumerSnapshotBusy
	}
	defer c.authorityMu.Unlock()
	if !c.mu.TryRLock() {
		return nil, ErrConsumerSnapshotBusy
	}
	defer c.mu.RUnlock()
	if !c.provenanceMu.TryLock() {
		return nil, ErrConsumerSnapshotBusy
	}
	defer c.provenanceMu.Unlock()
	g := &c.consumerAdmission
	if !g.mu.TryLock() {
		return nil, ErrConsumerSnapshotBusy
	}
	if g.unavailableLocked() {
		g.mu.Unlock()
		return nil, ErrConsumerMetadataRecovering
	}
	stamp := consumerMetadataReplayStamp{generation: c.stateImportGeneration, revision: c.authorityRevision, required: g.requiredGeneration}
	g.mu.Unlock()
	return c.copyConsumerHistoryWorkLocked(stamp, c.offsetTopicPartitionCount, c.offsetTopic)
}

func (c *Coordinator) CaptureConsumerHistoryWork() (*ConsumerHistoryWork, error) {
	stamp, err := c.captureConsumerMetadataReplayStamp()
	if err != nil {
		return nil, err
	}
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	c.mu.RLock()
	count, name := c.offsetTopicPartitionCount, c.offsetTopic
	c.mu.RUnlock()
	c.provenanceMu.Lock()
	defer c.provenanceMu.Unlock()
	return c.copyConsumerHistoryWorkLocked(stamp, count, name)
}

func (c *Coordinator) copyConsumerHistoryWorkLocked(stamp consumerMetadataReplayStamp, count int, name string) (*ConsumerHistoryWork, error) {
	if !c.raftCompatibilityKnown || !c.metadataSourcesKnown || c.raftCompatibility == nil {
		return nil, ErrConsumerMetadataBootstrapUnknown
	}
	baseline := newCompatibilityCoordinator()
	if err := baseline.importStateValidated(c.raftCompatibility.ExportState()); err != nil {
		return nil, err
	}
	for _, partitions := range c.metadataSources {
		for p := range partitions {
			count = max(count, p+1)
		}
	}
	proof := newCompatibilityCoordinator()
	proof.offsetTopic, proof.offsetTopicPartitionCount = name, count
	proof.raftCompatibility, proof.raftCompatibilityKnown, proof.metadataSourcesKnown = baseline, true, true
	// This private replay-only object has no workers, writer or public admission.
	proof.consumerAdmission.owned, proof.consumerAdmission.lease = true, 1
	return &ConsumerHistoryWork{owner: c, stamp: stamp, proof: proof}, nil
}

func (w *ConsumerHistoryWork) Prepare(source ConsumerMetadataIndexReader, partition int, first, end uint64) (*ConsumerHistoryCompactionPlan, error) {
	if w == nil || w.proof == nil {
		return nil, fmt.Errorf("missing consumer history work")
	}
	plan, err := w.proof.PrepareConsumerHistoryCompaction(source, partition, first, end)
	if err != nil {
		return nil, err
	}
	plan.owner, plan.stamp, plan.detached = w.owner, w.stamp, true
	return plan, nil
}
