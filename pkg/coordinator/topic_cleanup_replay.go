package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const (
	topicCleanupDuration = 10 * time.Millisecond
	topicCleanupRecords  = 128
	topicCleanupBytes    = 1 << 20
	topicCleanupGroups   = 64
	topicCleanupTopics   = 64
)

// ConsumerCleanupPartition identifies an immutable committed prefix. Identity
// must be comparable and change when the underlying partition is replaced.
type ConsumerCleanupPartition struct {
	TopicInstance     string
	TopicEpoch        uint64
	Identity          any
	DurableGeneration string
	Generation        uint64
	Start             uint64
	End               uint64
}

// ConsumerCleanupPage counts all storage work, including filtered records.
type ConsumerCleanupPage = types.ReplayPage

// ConsumerCleanupView is valid only inside WithConsumerCleanupView. The reader
// fences partition replacement/rollback until the callback returns.
type ConsumerCleanupView interface {
	Partitions() []ConsumerCleanupPartition
	Read(partition int, offset uint64, cursor types.ReplayCursor, maxRecords, maxBytes int) (ConsumerCleanupPage, error)
}

type ConsumerCleanupReader interface {
	WithConsumerCleanupView(func(ConsumerCleanupView) error) error
}

type consumerCleanupReader = ConsumerCleanupReader

// SetConsumerCleanupProvider installs optional detached preparation. Preparation
// may perform network I/O; view callbacks and proof checks must be local-only.
// It must not import coordinator state or renew member/ownership clocks.
func (c *Coordinator) SetConsumerCleanupProvider(provider func(context.Context) (ConsumerCleanupReader, func(), error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupProvider = provider
}

// TopicCleanupPending is structured progress for a committed topic intent that
// is waiting for local consumer metadata. It never contains record payloads.
type TopicCleanupPending struct {
	Topic     string `json:"topic"`
	Group     string `json:"group,omitempty"`
	Epoch     uint64 `json:"epoch,omitempty"`
	Partition int    `json:"partition"`
	Next      uint64 `json:"next"`
	End       uint64 `json:"end"`
	Records   uint64 `json:"records"`
	Bytes     uint64 `json:"bytes"`
	ReadBytes uint64 `json:"read_bytes"`
	Reads     uint64 `json:"reads"`
	Segment   uint64 `json:"segment"`
	Position  uint64 `json:"position"`
	Reason    string `json:"reason"`
	State     string `json:"state,omitempty"`
	cause     error
}

const (
	CleanupContinue = "continue"
	CleanupWait     = "wait"
	CleanupError    = "error"
)

var errCleanupBudget = errors.New("metadata scan budget exhausted")

// Reserve the complete read/decode/validation lifetime, not just the disk
// buffer. One process may host more than one Coordinator. Try acquisition is
// required because callers already pin lifecycle and partition read views.
var cleanupReplaySlot = make(chan struct{}, 1)

func (p *TopicCleanupPending) Unwrap() error { return p.cause }

func (p *TopicCleanupPending) Error() string {
	return fmt.Sprintf("consumer cleanup pending topic=%q group=%q epoch=%d partition=%d next=%d end=%d records=%d bytes=%d reads=%d reason=%s",
		p.Topic, p.Group, p.Epoch, p.Partition, p.Next, p.End, p.Records, p.Bytes, p.Reads, p.Reason)
}

type cleanupEvidence struct {
	beforeLive        uint64
	deletedEpoch      uint64
	registrationAfter bool
	epoch             uint64
	liveEpoch         uint64
	kind              string
	topic             string
	digest            [32]byte
}

type cleanupTarget struct {
	singleTopic         bool
	group               *GroupMetadata
	epoch               uint64
	generation          int
	cooperativeRevision uint64
	evidence            cleanupEvidence
}

type topicCleanupScan struct {
	completed           map[string]*cleanupTarget
	completedPartitions []ConsumerCleanupPartition
	validated           bool
	checkProof          func([]ConsumerCleanupPartition) error
	boundary            *TopicCleanupBoundary
	current             func() bool
	targets             map[string]*cleanupTarget
	partitions          []ConsumerCleanupPartition
	next                []uint64
	cursors             []types.ReplayCursor
	status              TopicCleanupPending
	checkpointLoaded    bool
	checkpointRecords   uint64
	checkpointBytes     uint64
}

// ReconcileTopicGroups preserves standalone cleanup and makes distributed
// materialization replay committed tombstones before consulting stale members.
func (c *Coordinator) ReconcileTopicGroups(topicName string) ([]string, error) {
	return c.ReconcileTopicGroupsIf(topicName, func() bool { return true })
}

// CheckTopicGroupCleanup is the nonblocking Raft-Apply path. Both replay and
// new writes run in reconciliation: even waiting for a coordinator lock here
// could block a writer whose partition commit needs the same Raft apply loop.
func (c *Coordinator) CheckTopicGroupCleanup(topicName string, boundaries ...*TopicCleanupBoundary) ([]string, error) {
	if c.standalone {
		return c.DeleteInactiveGroupsForTopic(topicName)
	}
	pending := &TopicCleanupPending{Topic: topicName, State: CleanupWait, Reason: "consumer cleanup scheduled outside Raft apply"}
	// Snapshot installation can temporarily empty the live overlay. Absence
	// is meaningful only while consumer recovery admission remains open.
	gate := &c.consumerAdmission
	if !gate.mu.TryLock() {
		return nil, pending
	}
	defer gate.mu.Unlock()
	if gate.unavailableLocked() {
		return nil, pending
	}
	if !c.mu.TryLock() {
		return nil, pending
	}
	defer c.mu.Unlock()
	var boundary *TopicCleanupBoundary
	if len(boundaries) > 0 {
		boundary = boundaries[0]
	}
	// The final check runs from Raft apply and cannot perform disk I/O. A
	// completed proof is reusable only for the same operation and group epoch.
	var scan *topicCleanupScan
	if boundary != nil && c.cleanupMu.TryLock() {
		defer c.cleanupMu.Unlock()
		candidate := c.cleanupProgress[topicName]
		if candidate != nil && candidate.validated && reflect.DeepEqual(candidate.boundary, boundary) && candidate.matches(candidate.partitions) {
			check := candidate.checkProof
			if validator, ok := c.topicHandler.(interface {
				CheckConsumerCleanupProof([]ConsumerCleanupPartition) error
			}); ok && check == nil {
				check = validator.CheckConsumerCleanupProof
			}
			if check != nil && check(candidate.partitions) == nil {
				scan = candidate
			}
		}
	}
	for name, group := range c.groups {
		if group == nil {
			continue
		}
		if !group.mu.TryRLock() {
			return nil, pending
		}
		_, found := topicGroupReferenceLocked(name, group, topicName)
		epoch := group.RegistrationEpoch
		if scan != nil {
			target := scan.preservedTarget(name)
			if target != nil && target.group == group && target.epoch == epoch && !c.lifecyclePending[name] && scan.preserves(target) {
				found = false
			}
		}
		group.mu.RUnlock()
		if found {
			pending.Group, pending.Epoch = name, epoch
			return nil, pending
		}
	}
	return nil, nil
}

// ReconcileTopicGroupsIf validates the caller's topic intent after capturing
// group targets under lifecycleMu. The callback must not mutate coordinator
// state and may be retained to reclaim superseded scans on cache admission.
// New registrations cannot replace those targets until cleanup returns.
func (c *Coordinator) ReconcileTopicGroupsIf(topicName string, current func() bool) ([]string, error) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return c.ReconcileTopicGroupsContext(ctx, topicName, current)
}

func (c *Coordinator) ReconcileTopicGroupsContext(ctx context.Context, topicName string, current func() bool, boundaries ...*TopicCleanupBoundary) ([]string, error) {
	var boundary *TopicCleanupBoundary
	if len(boundaries) > 0 {
		boundary = boundaries[0].Clone()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if c.standalone {
		if !current() {
			return nil, fmt.Errorf("topic cleanup intent changed")
		}
		return c.DeleteInactiveGroupsForTopic(topicName)
	}
	if !c.cleanupMu.TryLock() {
		return nil, &TopicCleanupPending{Topic: topicName, State: CleanupWait, Reason: "cleanup worker busy"}
	}
	defer c.cleanupMu.Unlock()

	// Ownership recovery may itself take lifecycleMu. Never invoke it while
	// holding lifecycleMu or the metadata read view.
	c.mu.RLock()
	checkOwner := c.groupOwnerChecker
	provider := c.cleanupProvider
	c.mu.RUnlock()
	owners := make(map[string]bool)
	references := c.TopicGroupReferences(topicName)
	previous := c.cleanupProgress[topicName]
	if previous != nil && !reflect.DeepEqual(previous.boundary, boundary) {
		previous = nil
	}
	references = c.pendingCleanupReferences(references, previous, boundary)
	for i, reference := range references {
		if i == topicCleanupGroups {
			break
		}
		if checkOwner != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			owners[reference.Name] = checkOwner(reference.Name)
		}
	}

	reader, ok := c.topicHandler.(consumerCleanupReader)
	if provider != nil && len(references) > 0 {
		prepared, closePrepared, err := provider(ctx)
		if closePrepared != nil {
			defer closePrepared()
		}
		if err != nil {
			return nil, err
		}
		reader, ok = prepared, prepared != nil
	}
	if !c.lifecycleMu.TryLock() {
		return nil, &TopicCleanupPending{Topic: topicName, State: CleanupWait, Reason: "group lifecycle busy"}
	}
	defer c.lifecycleMu.Unlock()
	references = c.TopicGroupReferences(topicName)
	if len(references) == 0 {
		delete(c.cleanupProgress, topicName)
		c.removeCleanupCheckpoint(topicName)
		return nil, nil
	}
	if !ok {
		return nil, &TopicCleanupPending{Topic: topicName, State: CleanupWait, Reason: "committed metadata view unavailable"}
	}
	references = c.pendingCleanupReferences(references, previous, boundary)
	for _, reference := range references {
		// Stale members may be covered by a committed deletion. The remaining
		// scope protections cannot be bypassed by follower reconciliation.
		reference.MemberCount = 0
		if err := reference.validateCleanup(topicName); err != nil && (boundary == nil || reference.Shared || reference.Pattern) {
			return nil, err
		}
	}
	if c.cleanupProgress == nil {
		c.cleanupProgress = make(map[string]*topicCleanupScan)
	}
	scan := c.cleanupProgress[topicName]
	if !c.cleanupTargetsCurrent(scan, references) || !reflect.DeepEqual(scan.boundary, boundary) {
		if scan == nil && len(c.cleanupProgress) >= topicCleanupTopics {
			// Preserve unfinished scans so a large pending-topic set cannot
			// continually evict every cursor before it reaches the log tail.
			for name, candidate := range c.cleanupProgress {
				if (candidate.current != nil && !candidate.current()) ||
					(len(candidate.partitions) > 0 && candidate.matches(candidate.partitions)) {
					delete(c.cleanupProgress, name)
					c.removeCleanupCheckpoint(name)
					break
				}
			}
			if len(c.cleanupProgress) >= topicCleanupTopics {
				return nil, &TopicCleanupPending{Topic: topicName, State: CleanupWait, Reason: "waiting for cleanup scan capacity"}
			}
		}
		scan = c.newTopicCleanupScan(topicName, references)
		scan.boundary = boundary
		if previous != nil {
			scan.completed, scan.completedPartitions = previous.completed, previous.completedPartitions
			if len(references) == 0 {
				scan.partitions, scan.next, scan.cursors = previous.partitions, previous.next, previous.cursors
			}
		}
		c.cleanupProgress[topicName] = scan
	}
	if !current() {
		delete(c.cleanupProgress, topicName)
		c.removeCleanupCheckpoint(topicName)
		return nil, fmt.Errorf("topic cleanup intent changed")
	}
	scan.current = current
	scan.validated = false
	var deleted []string
	scan.status.State, scan.status.cause = CleanupWait, nil
	err := withCleanupView(ctx, reader, func(view ConsumerCleanupView) error {
		if err := boundary.validateView(view.Partitions()); err != nil {
			return err
		}
		c.loadCleanupCheckpoint(scan, view.Partitions())
		err := scan.readContext(ctx, view, time.Now)
		if err == nil || errors.Is(err, errCleanupBudget) {
			if saveErr := c.saveCleanupCheckpoint(scan, view); saveErr != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				if err != nil {
					return saveErr
				}
				// The completed in-memory proof is still checked against the
				// current view below. A disposable cache must not block apply.
				util.Warn("Consumer cleanup checkpoint save failed for topic %s: %v", topicName, saveErr)
			}
		}
		return err
	})
	if err == nil {
		// CommitOffset holds c.mu while consulting the topic manager. Acquire
		// locks in that same order when applying proof; never take c.mu from a
		// read view. Disk I/O above does not hold the global coordinator lock.
		c.mu.Lock()
		err = withCleanupView(ctx, reader, func(view ConsumerCleanupView) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !current() {
				return fmt.Errorf("topic cleanup intent changed")
			}
			if !scan.matches(view.Partitions()) {
				return fmt.Errorf("committed metadata view changed before cleanup apply")
			}
			if err := boundary.validateView(view.Partitions()); err != nil {
				return err
			}
			for name, target := range scan.completed {
				group := c.groups[name]
				if group == nil || group != target.group {
					delete(scan.completed, name)
					return fmt.Errorf("preserved group replaced during cleanup")
				}
				group.mu.RLock()
				valid := group.RegistrationEpoch == target.epoch && !c.lifecyclePending[name]
				group.mu.RUnlock()
				if !valid {
					delete(scan.completed, name)
					return fmt.Errorf("preserved group changed during cleanup")
				}
			}
			for _, reference := range references {
				target := scan.targets[reference.Name]
				if target == nil {
					continue
				}
				group := c.groups[reference.Name]
				if group != target.group || group == nil {
					return fmt.Errorf("local group replaced during cleanup")
				}
				group.mu.Lock()
				if group.RegistrationEpoch == target.epoch && !c.lifecyclePending[reference.Name] && scan.preserves(target) {
					group.mu.Unlock()
					continue
				}
				evidence := target.evidence
				deletionEpoch := evidence.epoch
				deletionProven := evidence.kind == ConsumerMetadataRecordTombstone && evidence.epoch > evidence.liveEpoch
				// A later committed registration cannot revive an older local
				// incarnation already covered by a tombstone. Remove only that
				// stale cache; the newer registration/members/offsets stay in the
				// log and are installed by normal ownership recovery.
				if boundary != nil && len(boundary.Ends) != 0 && evidence.registrationAfter &&
					evidence.kind == ConsumerMetadataRecordRegistration && target.epoch <= evidence.beforeLive &&
					evidence.deletedEpoch > target.epoch && evidence.deletedEpoch < evidence.epoch {
					deletionEpoch, deletionProven = evidence.deletedEpoch, true
				}
				ref, found := topicGroupReferenceLocked(reference.Name, group, topicName)
				ref.MemberCount = 0
				valid := found && ref.validateCleanup(topicName) == nil &&
					group.RegistrationEpoch == target.epoch && group.Generation == target.generation && cooperativeRevision(group) == target.cooperativeRevision &&
					!c.lifecyclePending[reference.Name] && deletionProven &&
					evidence.topic == topicName && deletionEpoch > group.RegistrationEpoch &&
					deletionEpoch > c.groupEpochs[reference.Name]
				group.mu.Unlock()
				if !valid {
					continue
				}
				delete(c.groups, reference.Name)
				c.groupEpochs[reference.Name] = deletionEpoch
				delete(c.ownershipSince, reference.Name)
				deleted = append(deleted, reference.Name)
			}
			return nil
		})
		c.mu.Unlock()
	}
	if len(deleted) > 0 {
		c.updateOffsetPartitionCount()
		if boundary == nil {
			delete(c.cleanupProgress, topicName)
			c.removeCleanupCheckpoint(topicName)
		}
	}
	if err != nil {
		scan.status.State, scan.status.cause = CleanupError, err
		var waiting *TopicCleanupPending
		if errors.As(err, &waiting) && waiting.State == CleanupWait {
			scan.status.State = CleanupWait
		}
		if errors.Is(err, errCleanupBudget) {
			scan.status.State = CleanupContinue
		}
		scan.status.Reason = err.Error()
		status := scan.status
		return deleted, &status
	}
	scan.validated = true
	scan.checkProof = nil
	if validator, ok := reader.(interface {
		CheckConsumerCleanupProof([]ConsumerCleanupPartition) error
	}); ok {
		scan.checkProof = validator.CheckConsumerCleanupProof
	}
	if boundary != nil {
		if scan.completed == nil {
			scan.completed = make(map[string]*cleanupTarget)
		}
		for name, target := range scan.targets {
			if scan.preserves(target) {
				scan.completed[name] = target
			}
		}
		scan.completedPartitions = append([]ConsumerCleanupPartition(nil), scan.partitions...)
	}
	// Only a recovered owner can originate a new deletion. The configured
	// durable writer must additionally fence ownership at append time.
	for _, reference := range c.TopicGroupReferences(topicName) {
		if owners[reference.Name] {
			target := scan.targets[reference.Name]
			if !scan.mayDelete(target) {
				continue
			}
			if target == nil || target.evidence.epoch > target.epoch || target.evidence.liveEpoch > target.epoch {
				continue
			}
			c.offsetRecordWriterMu.RLock()
			writerAvailable := c.offsetRecordWriter != nil
			c.offsetRecordWriterMu.RUnlock()
			if !writerAvailable {
				return deleted, &TopicCleanupPending{Topic: topicName, Group: reference.Name, State: CleanupWait, Reason: "durable group writer unavailable"}
			}
			if err := c.deleteGroupWithFenceLocked(reference.Name, topicName, target.group); err != nil {
				scan.status.Group = reference.Name
				scan.status.State, scan.status.cause = CleanupError, err
				if errors.Is(err, errCleanupBudget) {
					scan.status.State = CleanupContinue
				}
				scan.status.Reason = err.Error()
				status := scan.status
				return deleted, &status
			}
			deleted = append(deleted, reference.Name)
		}
	}
	remaining := c.TopicGroupReferences(topicName)
	if boundary != nil {
		filtered := remaining[:0]
		for _, ref := range remaining {
			if !scan.preserves(scan.preservedTarget(ref.Name)) {
				filtered = append(filtered, ref)
			}
		}
		remaining = filtered
	}
	if len(remaining) == 0 {
		if boundary == nil {
			delete(c.cleanupProgress, topicName)
			c.removeCleanupCheckpoint(topicName)
		}
		return deleted, nil
	}
	scan.status.Group = remaining[0].Name
	if target := scan.targets[remaining[0].Name]; target != nil {
		scan.status.Epoch = target.epoch
	}
	scan.status.Reason = "waiting for a newer committed group tombstone"
	if boundary != nil && len(boundary.Ends) == 0 {
		scan.status.Reason = "truncate ordering evidence unavailable"
	}
	if len(deleted) > 0 || (len(scan.completed) > 0 && scan.targets[remaining[0].Name] == nil) {
		scan.status.State = CleanupContinue
		scan.status.Reason = "more cleanup targets remain"
	}
	status := scan.status
	return deleted, &status
}

func (scan *topicCleanupScan) matches(partitions []ConsumerCleanupPartition) bool {
	if len(partitions) != len(scan.partitions) {
		return false
	}
	for i, partition := range partitions {
		if partition != scan.partitions[i] || scan.next[i] != partition.End {
			return false
		}
	}
	return true
}

func (c *Coordinator) cleanupTargetsCurrent(scan *topicCleanupScan, refs []TopicGroupReference) bool {
	if scan == nil || len(scan.targets) != min(len(refs), topicCleanupGroups) {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range refs[:min(len(refs), topicCleanupGroups)] {
		target := scan.targets[ref.Name]
		group := c.groups[ref.Name]
		if target == nil || group == nil || target.group != group {
			return false
		}
		group.mu.RLock()
		// The deletion apply check fences membership as well as registration.
		// Refresh a changed target instead of retaining a scan that can never
		// satisfy that check. Completed preserved registrations are filtered
		// separately and remain independent of membership changes.
		same := group.RegistrationEpoch == target.epoch && group.Generation == target.generation && cooperativeRevision(group) == target.cooperativeRevision
		group.mu.RUnlock()
		if !same {
			return false
		}
	}
	return true
}

func (c *Coordinator) newTopicCleanupScan(topicName string, refs []TopicGroupReference) *topicCleanupScan {
	scan := &topicCleanupScan{targets: make(map[string]*cleanupTarget), status: TopicCleanupPending{Topic: topicName}}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range refs[:min(len(refs), topicCleanupGroups)] {
		group := c.groups[ref.Name]
		if group == nil {
			continue
		}
		group.mu.RLock()
		scan.targets[ref.Name] = &cleanupTarget{singleTopic: !ref.Shared && !ref.Pattern, group: group, epoch: group.RegistrationEpoch, generation: group.Generation, cooperativeRevision: cooperativeRevision(group)}
		group.mu.RUnlock()
	}
	return scan
}

func withCleanupView(ctx context.Context, reader consumerCleanupReader, fn func(ConsumerCleanupView) error) error {
	if r, ok := reader.(interface {
		WithConsumerCleanupViewContext(context.Context, func(ConsumerCleanupView) error) error
	}); ok {
		return r.WithConsumerCleanupViewContext(ctx, fn)
	}
	return reader.WithConsumerCleanupView(fn)
}

func (scan *topicCleanupScan) read(view ConsumerCleanupView) error {
	return scan.readContext(context.Background(), view, time.Now)
}

func (scan *topicCleanupScan) readContext(ctx context.Context, view ConsumerCleanupView, now func() time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case cleanupReplaySlot <- struct{}{}:
		defer func() { <-cleanupReplaySlot }()
	default:
		return &TopicCleanupPending{State: CleanupWait, Reason: "cleanup replay memory reservation busy"}
	}
	started := now()
	progressed := false

	partitions := view.Partitions()
	if len(scan.completed) > 0 {
		valid := len(partitions) == len(scan.completedPartitions)
		for i, p := range partitions {
			if !valid {
				break
			}
			old := scan.completedPartitions[i]
			valid = p.Identity == old.Identity && p.Generation == old.Generation && p.DurableGeneration == old.DurableGeneration && p.Start == old.Start && p.End >= old.End
		}
		if !valid {
			scan.completed = nil
			return fmt.Errorf("completed cleanup evidence view changed")
		}
	}
	if len(partitions) == 0 {
		return fmt.Errorf("consumer metadata partitions unavailable")
	}
	reset := len(partitions) != len(scan.partitions)
	for i, partition := range partitions {
		if reset {
			break
		}
		old := scan.partitions[i]
		reset = partition.Identity != old.Identity || partition.Generation != old.Generation ||
			partition.Start != old.Start || partition.End < old.End || partition.DurableGeneration != old.DurableGeneration
	}
	if reset {
		metrics.ConsumerCleanupReplayInvalidations.Inc()
		scan.next = make([]uint64, len(partitions))
		scan.cursors = make([]types.ReplayCursor, len(partitions))
		for i, partition := range partitions {
			scan.next[i] = partition.Start
		}
		for _, target := range scan.targets {
			target.evidence = cleanupEvidence{}
		}
	}
	scan.partitions = append(scan.partitions[:0], partitions...)
	records, bytes, pages := topicCleanupRecords, topicCleanupBytes, topicCleanupRecords
	for partition, bounds := range partitions {
		scan.status.Partition, scan.status.Next, scan.status.End = partition, scan.next[partition], bounds.End
		scan.status.Segment, scan.status.Position = scan.cursors[partition].Segment, scan.cursors[partition].Position
		for scan.next[partition] < bounds.End {
			if err := ctx.Err(); err != nil {
				return err
			}
			if records == 0 || bytes <= 0 || pages == 0 || (progressed && now().Sub(started) >= topicCleanupDuration) {
				return errCleanupBudget
			}
			read := func(recordLimit, byteLimit int) (ConsumerCleanupPage, error) {
				page, err := view.Read(partition, scan.next[partition], scan.cursors[partition], recordLimit, byteLimit)
				pages--
				scan.status.Reads++
				metrics.ConsumerCleanupReplayReads.Inc()
				metrics.ConsumerCleanupReplayRecords.Add(float64(max(0, page.Records)))
				metrics.ConsumerCleanupReplayBytes.Add(float64(max(0, page.Bytes)))
				scan.status.Records += uint64(max(0, page.Records))
				scan.status.Bytes += uint64(max(0, page.Bytes))
				scan.status.ReadBytes += uint64(max(0, page.ReadBytes))
				return page, err
			}
			page, err := read(records, bytes)
			limitRecords, limitBytes := records, bytes
			if err != nil {
				var budget *types.ReplayByteBudget
				if errors.As(err, &budget) && budget.Size > bytes && budget.Size <= util.MaxMessageSize &&
					page.Records == 0 && page.Bytes == 0 && len(page.Messages) == 0 {
					if progressed || pages == 0 {
						return errCleanupBudget
					}
					if err := ctx.Err(); err != nil {
						return err
					}
					// The reserved slot admits one full storage record, including
					// its envelope, through validation. Retry the ORIGINAL cursor;
					// a length-only probe cannot advance a validated checkpoint.
					limitRecords, limitBytes = 1, budget.Size
					page, err = read(limitRecords, limitBytes)
				}
				if err != nil {
					return err
				}
			}
			if page.Records < 0 || page.Records > limitRecords || page.Bytes < 0 || page.Bytes > limitBytes ||
				page.Next < scan.next[partition] || page.Next > bounds.End ||
				(page.Next == scan.next[partition] && page.Cursor == scan.cursors[partition]) {
				return fmt.Errorf("invalid bounded metadata page")
			}
			for _, message := range page.Messages {
				if err := ctx.Err(); err != nil {
					return err
				}
				if message.Offset < scan.next[partition] || message.Offset >= page.Next {
					return fmt.Errorf("metadata record outside committed page")
				}
				if err := scan.observeAt(message, partition); err != nil {
					return err
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			progressed = true
			records -= page.Records
			bytes -= page.Bytes
			scan.next[partition] = page.Next
			scan.cursors[partition] = page.Cursor
			scan.status.Next = page.Next
			scan.status.Segment, scan.status.Position = page.Cursor.Segment, page.Cursor.Position
		}
	}
	return nil
}

func (scan *topicCleanupScan) observeAt(message types.Message, partition int) error {
	// The physical committed prefix includes non-JSON transaction controls.
	// Validate their complete envelope before skipping them: a marker flag
	// must never hide a keyed lifecycle record or supply deletion authority.
	if message.TransactionMarker != "" || message.ControlBatchType != "" ||
		message.ControlBatchVersion != 0 || message.ControlBatchCoordinatorEpoch != 0 ||
		len(message.ControlBatchKey) != 0 || len(message.ControlBatchValue) != 0 {
		return validateCleanupTransactionControl(message)
	}
	var identity struct {
		Group   string          `json:"group"`
		Version json.RawMessage `json:"version"`
	}
	if err := json.Unmarshal([]byte(message.Payload), &identity); err != nil {
		return fmt.Errorf("malformed consumer metadata")
	}
	target := scan.targets[identity.Group]
	if target == nil {
		// A forged group name must not hide a record under a tracked key.
		for name := range scan.targets {
			key := consumerMetadataRecordKey(ConsumerMetadataRecord{Type: ConsumerMetadataRecordTombstone, Group: name})
			if message.Key == key {
				return fmt.Errorf("consumer metadata key/group mismatch")
			}
		}
		// Size and unrelated identity never exempt a versioned record from
		// schema/key validation. Legacy offset objects retain their existing
		// syntax-only treatment and cannot supply lifecycle evidence.
		if len(identity.Version) != 0 {
			record, versioned, err := decodeConsumerMetadataRecord(message.Payload)
			if err != nil || !versioned || message.Key != consumerMetadataRecordKey(record) {
				return fmt.Errorf("invalid consumer metadata for unrelated group")
			}
		}
		return nil
	}
	scan.status.Group, scan.status.Epoch = identity.Group, target.epoch
	record, versioned, err := decodeConsumerMetadataRecord(message.Payload)
	if err != nil || !versioned || message.Key != consumerMetadataRecordKey(record) {
		return fmt.Errorf("invalid consumer metadata for group %q", identity.Group)
	}
	if record.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot {
		// Raw transaction intents do not materialize group state and cannot
		// authorize cleanup. Validate their envelope before ignoring them;
		// current reservations remain protected by lifecycle admission.
		if message.TransactionalID != record.TransactionalID || message.ProducerID != record.ProducerID || message.Epoch != record.ProducerEpoch ||
			(message.TransactionState != types.TransactionStateOpen && message.TransactionState != types.TransactionStateCommitted) {
			return fmt.Errorf("invalid transactional cleanup evidence for group %q", identity.Group)
		}
		return nil
	}
	// Group lifecycle records are nontransactional. A transaction (even an
	// aborted/open one) can never supply deletion authority.
	if message.TransactionalID != "" || message.TransactionState != "" || message.TransactionMarker != "" ||
		message.ControlBatchType != "" || message.ControlBatchVersion != 0 || message.ControlBatchCoordinatorEpoch != 0 ||
		len(message.ControlBatchKey) != 0 || len(message.ControlBatchValue) != 0 {
		return fmt.Errorf("transactional consumer lifecycle evidence for group %q", identity.Group)
	}
	evidence := &target.evidence
	if scan.boundary != nil && len(scan.boundary.Ends) > partition && record.Type != ConsumerMetadataRecordTombstone && message.Offset < scan.boundary.Ends[partition] {
		evidence.beforeLive = max(evidence.beforeLive, record.Epoch)
	}
	if record.Type != ConsumerMetadataRecordTombstone {
		evidence.liveEpoch = max(evidence.liveEpoch, record.Epoch)
	}
	if record.Type != ConsumerMetadataRecordRegistration && record.Type != ConsumerMetadataRecordTombstone {
		return nil
	}
	if record.Epoch < target.epoch && scan.boundary == nil {
		return nil
	}
	if record.Type == ConsumerMetadataRecordTombstone &&
		(record.Version != ConsumerMetadataRecordVersion || record.Topic != scan.status.Topic ||
			len(record.Topics) != 0 || record.TopicPattern != "" || len(record.TopicPartitions) != 0 ||
			record.Lifecycle != nil || len(record.Reservations) != 0 || len(record.ReservationDecisions) != 0) {
		return fmt.Errorf("invalid tombstone scope for group %q", identity.Group)
	}
	if record.Type == ConsumerMetadataRecordTombstone {
		evidence.deletedEpoch = max(evidence.deletedEpoch, record.Epoch)
	}
	// Timestamp is not lifecycle identity (acknowledgement retries may differ).
	record.Timestamp = ConsumerMetadataRecord{}.Timestamp
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode lifecycle identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	if record.Epoch == evidence.epoch && digest != evidence.digest {
		return fmt.Errorf("conflicting lifecycle records for group %q epoch=%d", record.Group, record.Epoch)
	}
	if record.Epoch > evidence.epoch {
		evidence.epoch, evidence.kind, evidence.topic, evidence.digest = record.Epoch, record.Type, record.Topic, digest
		evidence.registrationAfter = scan.boundary != nil && len(scan.boundary.Ends) > partition && message.Offset >= scan.boundary.Ends[partition] && len(record.InitialOffsets) == 0
	}
	return nil
}

func validateCleanupTransactionControl(message types.Message) error {
	invalid := fmt.Errorf("invalid consumer metadata transaction control")
	if message.TransactionalID == "" || message.Epoch < 0 || message.Payload != types.TransactionControlMarkerPayload ||
		message.Key != "" || message.EventType != "" || message.SchemaVersion != 0 || message.AggregateVersion != 0 ||
		message.Metadata != "" || message.EventID != "" || message.PayloadDigest != "" ||
		message.ControlBatchType != types.ControlBatchTransaction ||
		message.ControlBatchVersion != types.ControlBatchVersionCursusV2 {
		return invalid
	}
	key, value, err := types.TransactionMarkerControlBytes(message.TransactionMarker, message.ControlBatchCoordinatorEpoch)
	if err != nil || !bytes.Equal(message.ControlBatchKey, key) || !bytes.Equal(message.ControlBatchValue, value) {
		return invalid
	}
	state := types.TransactionStateCommitted
	if message.TransactionMarker == types.TransactionMarkerAbort {
		state = types.TransactionStateAborted
	}
	if message.TransactionState != state {
		return invalid
	}
	// Legacy transactions use the producer epoch; processing transactions
	// use a separately fenced coordinator epoch in the marker producer ID.
	legacy := fmt.Sprintf("txn-marker:%s:%s", message.TransactionalID, message.TransactionMarker)
	processing := fmt.Sprintf("txn-marker:%s:%d:%s", message.TransactionalID, message.ControlBatchCoordinatorEpoch, message.TransactionMarker)
	history := fmt.Sprintf("txn-history:%s:%s", message.TransactionalID, message.TransactionMarker)
	if message.ProducerID == history && message.ControlBatchCoordinatorEpoch > 0 && message.SeqNum == uint64(message.ControlBatchCoordinatorEpoch) {
		return nil
	}
	if message.SeqNum != 1 {
		return invalid
	}
	if message.ProducerID != processing &&
		(message.ProducerID != legacy || message.Epoch != message.ControlBatchCoordinatorEpoch) {
		return invalid
	}
	return nil
}
