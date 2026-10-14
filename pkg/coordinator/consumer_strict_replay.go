package coordinator

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

// ConsumerMetadataBounds describes a fixed, local read generation. Ends are
// exclusive and include compacted holes. Implementations must remain valid for
// the entire call (in particular, call preparation inside read-view callbacks).
type ConsumerMetadataBounds interface {
	EarliestTopicOffset(topic string, partition int) (uint64, error)
	EndTopicOffset(topic string, partition int) (uint64, error)
}

// ConsumerMetadataCommittedReader must expose ALL current offsets partitions,
// filter uncommitted/aborted records through each fixed end (not its LSO), and
// return detached pages in increasing offset order. It must not invoke network
// or Raft. A short page is not required to signify the end of the range.
// Transaction decisions must also be bounded by the captured end; truncating
// pages read against a later HWM does not satisfy this contract.
type ConsumerMetadataCommittedReader interface {
	ConsumerMetadataBounds
	ExistingPartitionCount(topic string) (int, error)
	ReadCommittedConsumerMetadata(topic string, partition int, offset uint64, max int) ([]types.Message, error)
}

// ConsumerMetadataRawReader includes divergent and uncommitted tails. The bound
// for the target partition must describe the physical generation being replaced.
type ConsumerMetadataRawReader interface {
	ConsumerMetadataBounds
	OffsetLogReader
}

type ConsumerMetadataRoute func(group string) (int, error)

// ConsumerMetadataScope is a conservative name set, reusable across retries and
// imports on its original coordinator and partition. It never carries live state.
type ConsumerMetadataScope struct {
	owner     *Coordinator
	partition int
	names     map[string]struct{}
}

// PreparedConsumerMetadataReplay is single-use, detached, and bound to the
// exclusive recovery lease and authority generation that prepared it.
type PreparedConsumerMetadataReplay struct {
	owner   *Coordinator
	stamp   consumerMetadataReplayStamp
	scope   *ConsumerMetadataScope
	scratch *Coordinator
}

// A stamp belongs to one operation, not to a reusable conservative scope.
type consumerMetadataReplayStamp struct{ lease, generation, revision uint64 }

type metadataReadRange struct{ first, end uint64 }
type boundedMetadataReader struct {
	bounds map[int]metadataReadRange
	read   func(string, int, uint64, int) ([]types.Message, error)
}

func (r *boundedMetadataReader) EarliestTopicOffset(_ string, p int) (uint64, error) {
	return r.bounds[p].first, nil
}
func (r *boundedMetadataReader) ReadTopicPartition(topic string, p int, offset uint64, max int) ([]types.Message, error) {
	bounds, ok := r.bounds[p]
	if !ok || offset < bounds.first || offset > bounds.end {
		return nil, fmt.Errorf("invalid metadata read range partition=%d offset=%d", p, offset)
	}
	if offset == bounds.end {
		return nil, nil
	}
	messages, err := r.read(topic, p, offset, max)
	if err != nil {
		return nil, err
	}
	if len(messages) > max {
		return nil, fmt.Errorf("metadata reader exceeded page limit")
	}
	next := offset
	for _, m := range messages {
		if m.Offset < next || m.Offset >= bounds.end || m.Offset == math.MaxUint64 {
			return nil, fmt.Errorf("metadata reader returned invalid offset %d in [%d,%d)", m.Offset, next, bounds.end)
		}
		next = m.Offset + 1
	}
	return messages, nil
}
func metadataBounds(reader ConsumerMetadataBounds, topic string, partition int) (metadataReadRange, error) {
	first, err := reader.EarliestTopicOffset(topic, partition)
	if err != nil {
		return metadataReadRange{}, err
	}
	end, err := reader.EndTopicOffset(topic, partition)
	if err != nil {
		return metadataReadRange{}, err
	}
	if first > end {
		return metadataReadRange{}, fmt.Errorf("invalid metadata bounds partition=%d start=%d end=%d", partition, first, end)
	}
	return metadataReadRange{first: first, end: end}, nil
}

func (c *Coordinator) captureConsumerMetadataReplayStamp() (consumerMetadataReplayStamp, error) {
	if c == nil || c.standalone {
		return consumerMetadataReplayStamp{}, ErrConsumerMetadataRecoveryStandalone
	}
	// Admission is acquired last and never waits for another lock or callback.
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.provenanceMu.Lock()
	defer c.provenanceMu.Unlock()
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.owned {
		return consumerMetadataReplayStamp{}, fmt.Errorf("consumer metadata replay requires an active exclusive lease")
	}
	if !c.raftCompatibilityKnown || !c.metadataSourcesKnown || c.raftCompatibility == nil {
		return consumerMetadataReplayStamp{}, ErrConsumerMetadataBootstrapUnknown
	}
	return consumerMetadataReplayStamp{lease: g.lease, generation: c.stateImportGeneration, revision: c.authorityRevision}, nil
}

func (c *Coordinator) validateConsumerMetadataReplayStamp(stamp consumerMetadataReplayStamp) error {
	current, err := c.captureConsumerMetadataReplayStamp()
	if err != nil {
		return err
	}
	if current != stamp {
		return fmt.Errorf("consumer metadata replay operation is stale")
	}
	return nil
}

// CaptureConsumerMetadataScope must run immediately before destructive disk
// installation, under the caller's FSM transition exclusion and exclusive lease.
// Keep the returned scope even on error: valid observations only expand physical
// provenance. The route must be the actual controller offsets-partition route.
func (c *Coordinator) CaptureConsumerMetadataScope(partition int, raw ConsumerMetadataRawReader, route ConsumerMetadataRoute) (*ConsumerMetadataScope, error) {
	scope := &ConsumerMetadataScope{owner: c, partition: partition, names: make(map[string]struct{})}
	stamp, err := c.captureConsumerMetadataReplayStamp()
	if err != nil {
		return scope, err
	}
	if partition < 0 {
		return scope, fmt.Errorf("invalid consumer metadata partition %d", partition)
	}
	if err := c.extendConsumerMetadataScope(scope, raw, route); err != nil {
		return scope, err
	}
	return scope, c.validateConsumerMetadataReplayStamp(stamp)
}

func (c *Coordinator) extendConsumerMetadataScope(scope *ConsumerMetadataScope, raw ConsumerMetadataRawReader, route ConsumerMetadataRoute) error {
	if raw == nil || route == nil {
		return fmt.Errorf("raw metadata reader and actual route are required")
	}
	bounds, err := metadataBounds(raw, c.offsetTopic, scope.partition)
	if err != nil {
		return err
	}
	reader := &boundedMetadataReader{bounds: map[int]metadataReadRange{scope.partition: bounds}, read: raw.ReadTopicPartition}
	for next := bounds.first; next < bounds.end; {
		messages, err := reader.ReadTopicPartition(c.offsetTopic, scope.partition, next, 1024)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			break
		}
		for _, m := range messages {
			next = m.Offset + 1
			if m.TransactionMarker != types.TransactionMarkerNone {
				continue
			}
			record, versioned, err := decodeConsumerMetadataRecord(m.Payload)
			if err != nil {
				return fmt.Errorf("decode raw consumer metadata: %w", err)
			}
			if strings.HasPrefix(m.Key, "cursus.consumer.offset-reservations.") && (!versioned || record.Type != ConsumerMetadataRecordOffsetReservations) {
				return fmt.Errorf("invalid raw reservation record type")
			}
			name := record.Group
			if versioned {
				if m.Key != consumerMetadataRecordKey(record) {
					return fmt.Errorf("raw consumer metadata key mismatch")
				}
			} else {
				var offsets []OffsetItem
				name, _, offsets, err = parseOffsetLogPayload(m.Payload)
				if err != nil {
					return fmt.Errorf("decode raw legacy consumer metadata: %w", err)
				}
				for _, offset := range offsets {
					if offset.Partition < 0 {
						return fmt.Errorf("raw legacy metadata contains negative partition")
					}
				}
			}
			scope.names[name] = struct{}{}
			c.RecordConsumerMetadataSource(name, scope.partition)
		}
	}
	// Detach names under locks; routing may call back into the coordinator.
	names := make(map[string]struct{})
	c.authorityMu.Lock()
	if c.raftCompatibility != nil {
		for name := range c.raftCompatibility.groups {
			names[name] = struct{}{}
		}
		for name := range c.raftCompatibility.groupEpochs {
			names[name] = struct{}{}
		}
	}
	c.mu.RLock()
	for name := range c.groups {
		names[name] = struct{}{}
	}
	for name := range c.groupEpochs {
		names[name] = struct{}{}
	}
	c.mu.RUnlock()
	c.authorityMu.Unlock()
	c.provenanceMu.Lock()
	for name, partitions := range c.metadataSources {
		if _, ok := partitions[scope.partition]; ok {
			scope.names[name] = struct{}{}
		}
	}
	c.provenanceMu.Unlock()
	for name := range names {
		partition, err := route(name)
		if err != nil {
			return err
		}
		if partition < 0 {
			return fmt.Errorf("invalid consumer metadata route for %q", name)
		}
		if partition == scope.partition {
			scope.names[name] = struct{}{}
		}
	}
	return nil
}

// PrepareFullConsumerMetadataReplay reconstructs only proven Raft compatibility
// plus committed logs. It never copies the current live disk overlay.
// Callers must hold FSM transition exclusion throughout prepare and apply.
func (c *Coordinator) PrepareFullConsumerMetadataReplay(reader ConsumerMetadataCommittedReader) (*PreparedConsumerMetadataReplay, error) {
	stamp, err := c.captureConsumerMetadataReplayStamp()
	if err != nil {
		return nil, err
	}
	return c.prepareConsumerMetadataReplay(reader, nil, stamp)
}

func (c *Coordinator) PrepareScopedConsumerMetadataReplay(reader ConsumerMetadataCommittedReader, replacementRaw ConsumerMetadataRawReader, old *ConsumerMetadataScope, route ConsumerMetadataRoute) (*PreparedConsumerMetadataReplay, error) {
	stamp, err := c.captureConsumerMetadataReplayStamp()
	if err != nil {
		return nil, err
	}
	if old == nil || old.owner != c || old.partition < 0 {
		return nil, fmt.Errorf("consumer metadata scope identity mismatch")
	}
	scope := &ConsumerMetadataScope{owner: c, partition: old.partition, names: make(map[string]struct{}, len(old.names))}
	for name := range old.names {
		scope.names[name] = struct{}{}
		// A paired import may have replaced the persisted provenance since the
		// capture. Retained names remain conservative physical history.
		c.RecordConsumerMetadataSource(name, old.partition)
	}
	if err := c.extendConsumerMetadataScope(scope, replacementRaw, route); err != nil {
		return nil, err
	}
	return c.prepareConsumerMetadataReplay(reader, scope, stamp)
}

func (c *Coordinator) prepareConsumerMetadataReplay(reader ConsumerMetadataCommittedReader, scope *ConsumerMetadataScope, stamp consumerMetadataReplayStamp) (*PreparedConsumerMetadataReplay, error) {
	if err := c.validateConsumerMetadataReplayStamp(stamp); err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, fmt.Errorf("committed metadata reader is required")
	}
	scratch := newCompatibilityCoordinator()
	c.authorityMu.Lock()
	c.mu.RLock()
	minimumCount := c.offsetTopicPartitionCount
	c.mu.RUnlock()
	c.provenanceMu.Lock()
	known := c.raftCompatibilityKnown && c.metadataSourcesKnown && c.raftCompatibility != nil
	for _, partitions := range c.metadataSources {
		for p := range partitions {
			if p >= minimumCount {
				minimumCount = p + 1
			}
		}
	}
	c.provenanceMu.Unlock()
	if !known {
		c.authorityMu.Unlock()
		return nil, ErrConsumerMetadataBootstrapUnknown
	}
	state := c.raftCompatibility.ExportState()
	c.authorityMu.Unlock()
	if err := validateCompatibilityState(state); err != nil {
		return nil, err
	}
	if err := scratch.importStateValidated(state); err != nil {
		return nil, err
	}
	count, err := reader.ExistingPartitionCount(c.offsetTopic)
	if err != nil {
		return nil, err
	}
	if count <= 0 || count < minimumCount {
		return nil, fmt.Errorf("consumer metadata reader omits known partitions: got %d need %d", count, minimumCount)
	}
	bounded := &boundedMetadataReader{bounds: make(map[int]metadataReadRange, count), read: reader.ReadCommittedConsumerMetadata}
	for p := 0; p < count; p++ {
		bounds, err := metadataBounds(reader, c.offsetTopic, p)
		if err != nil {
			return nil, err
		}
		bounded.bounds[p] = bounds
	}
	scratch.offsetTopic = c.offsetTopic
	scratch.offsetTopicPartitionCount = count
	if _, err := scratch.recoverConsumerMetadataWithOptions(bounded, true); err != nil {
		return nil, err
	}
	if err := validateStrictReplayState(scratch); err != nil {
		return nil, err
	}
	prepared := &PreparedConsumerMetadataReplay{owner: c, stamp: stamp, scope: scope, scratch: scratch}
	// Fail preparation too if historical authority moved while local reads ran.
	if err := c.validateConsumerMetadataReplayStamp(stamp); err != nil {
		return nil, err
	}
	return prepared, nil
}

// Materializer validation deliberately accepts historical epoch-zero shells and
// registration-only records with zero activity. Public snapshot import validation
// is stronger and is not appropriate for this trusted-baseline replay result.
func validateStrictReplayState(c *Coordinator) error {
	for name, g := range c.groups {
		if name == "" || g == nil || g.Generation < 0 || g.RegistrationEpoch != c.groupEpochs[name] {
			return fmt.Errorf("invalid reconstructed consumer group %q", name)
		}
		for topic, offsets := range g.Offsets {
			if !groupAcceptsTopic(g, topic) {
				return fmt.Errorf("reconstructed consumer offset topic mismatch")
			}
			for p := range offsets {
				if p < 0 {
					return fmt.Errorf("negative reconstructed consumer offset partition")
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) validatePreparedReplayLocked(p *PreparedConsumerMetadataReplay) error {
	if p == nil || p.owner != c || p.scratch == nil || !c.consumerAdmission.owned || p.stamp.lease != c.consumerAdmission.lease || p.stamp.generation != c.stateImportGeneration || p.stamp.revision != c.authorityRevision {
		return fmt.Errorf("consumer metadata prepared replay is stale or belongs to another coordinator")
	}
	return nil
}
func (c *Coordinator) ApplyFullConsumerMetadataReplay(p *PreparedConsumerMetadataReplay) error {
	return c.applyConsumerMetadataReplay(p, false)
}
func (c *Coordinator) ApplyScopedConsumerMetadataReplay(p *PreparedConsumerMetadataReplay) error {
	return c.applyConsumerMetadataReplay(p, true)
}

func (c *Coordinator) applyConsumerMetadataReplay(p *PreparedConsumerMetadataReplay, scoped bool) error {
	if c == nil || c.standalone {
		return ErrConsumerMetadataRecoveryStandalone
	}
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumerAdmission.mu.Lock()
	if err := c.validatePreparedReplayLocked(p); err != nil {
		c.consumerAdmission.mu.Unlock()
		return err
	}
	if (p.scope != nil) != scoped {
		c.consumerAdmission.mu.Unlock()
		return fmt.Errorf("consumer metadata prepared replay scope mismatch")
	}
	if scoped {
		for name := range p.scope.names {
			delete(c.groups, name)
			delete(c.groupEpochs, name)
			delete(c.ownershipSince, name)
			if g := p.scratch.groups[name]; g != nil {
				c.groups[name] = g
			}
			if epoch, ok := p.scratch.groupEpochs[name]; ok {
				c.groupEpochs[name] = epoch
			}
		}
	} else {
		c.groups = p.scratch.groups
		c.groupEpochs = p.scratch.groupEpochs
		c.ownershipSince = make(map[string]time.Time)
	}
	sourcesByGroup := p.scratch.metadataSources
	c.authorityRevision++
	// Drop every prepared reference to installed mutable state.
	p.scratch = nil
	c.consumerAdmission.mu.Unlock()
	// Admission never waits for provenance or state locks. Authority and mu
	// still exclude another install/import until provenance joins the commit.
	for name, sources := range sourcesByGroup {
		for partition := range sources {
			c.RecordConsumerMetadataSource(name, partition)
		}
	}
	return nil
}
