package coordinator

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// Coordinator manages consumer groups, membership, heartbeats, and partition assignment.
type Coordinator struct {
	groups           map[string]*GroupMetadata // All consumer groups
	mu               sync.RWMutex              // Global lock for coordinator state
	lifecycleMu      sync.Mutex                // Serializes durable group lifecycle transitions
	lifecyclePending map[string]bool           // Groups whose durable lifecycle write is in progress
	cfg              *config.Config            // Configuration reference
	ctx              context.Context
	cancel           context.CancelFunc

	topicHandler              TopicHandler
	offsetTopic               string
	offsetTopicPartitionCount int
	standalone                bool
	groupEpochs               map[string]uint64
	offsetRecordWriter        func(ConsumerMetadataRecord) error
	transactionalOffsets      TransactionalOffsetResolver

	recoveryMu sync.RWMutex
	recovery   ConsumerMetadataRecoveryStatus

	// Session expiration is decided by the broker that owns each group. In a
	// cluster, the expiration callback serializes removals through the metadata
	// log so every broker observes the same generation and assignments.
	groupOwnerChecker func(groupName string) bool
	expirationHandler func(groupName string, generation int, memberIDs []string) error
	ownershipSince    map[string]time.Time
	observationOwner  func(groupName string) (bool, error)
	observationOwners func(groupNames []string) (map[string]bool, error)
}

type TopicHandler interface {
	Publish(topic string, msg *types.Message) error
	CreateTopic(topic string, partitionCount int, idempotent bool, eventSourcing bool) error
}

type OffsetLogReader interface {
	ReadTopicPartition(topic string, partitionID int, offset uint64, max int) ([]types.Message, error)
}

type offsetTopicPartitionProvider interface {
	ExistingPartitionCount(topic string) (int, error)
}

type offsetLogStartProvider interface {
	EarliestTopicOffset(topic string, partition int) (uint64, error)
}

type syncPublisher interface {
	PublishWithAck(topic string, msg *types.Message) error
}

// TransactionalOffsetResolver exposes only offsets whose transaction has a
// final committed decision. The registration epoch prevents an old
// transaction from leaking into a re-created consumer group.
type TransactionalOffsetResolver interface {
	CommittedOffset(group, topic string, partition int, registrationEpoch uint64) (uint64, bool)
}

// SetOffsetRecordWriter installs the cluster-aware __consumer_offsets writer.
// Standalone coordinators continue to publish through their TopicHandler.
func (c *Coordinator) SetOffsetRecordWriter(writer func(ConsumerMetadataRecord) error) {
	c.mu.Lock()
	c.offsetRecordWriter = writer
	c.mu.Unlock()
}

func (c *Coordinator) SetTransactionalOffsetResolver(resolver TransactionalOffsetResolver) {
	c.mu.Lock()
	c.transactionalOffsets = resolver
	c.mu.Unlock()
}

// GroupMetadata holds metadata for a single consumer group.
type GroupMetadata struct {
	mu                sync.RWMutex               // Per-group lock for offset operations
	TopicName         string                     // Topic this group consumes
	Topics            []string                   // Explicit v1 subscription topics
	TopicPattern      string                     // Optional v1 subscription pattern
	TopicPartitions   []TopicPartition           // Assignable v1 topic-partitions
	Members           map[string]*MemberMetadata // Active members
	Generation        int                        // Current membership generation
	Partitions        []int                      // All partitions of the topic
	LastRebalance     time.Time                  // Timestamp of last rebalance
	LastActivity      time.Time                  // Timestamp of last heartbeat or lifecycle activity
	Offsets           map[string]map[int]uint64  // topic -> partition -> next offset
	RegistrationEpoch uint64                     // durable lifecycle epoch
	OffsetRevisions   map[string]uint64          // topic -> durable snapshot revision
}

// MemberMetadata holds state for a single consumer instance.
type MemberMetadata struct {
	ID               string           // Unique consumer ID
	LastHeartbeat    time.Time        // Last heartbeat timestamp
	Assignments      []int            // Legacy single-topic assignments
	TopicAssignments []TopicPartition // v1 topic-partition assignments
}

type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// GroupStateSnapshot is a serializable snapshot of a consumer group's state.
type GroupStateSnapshot struct {
	TopicName         string                      `json:"topic"`
	Topics            []string                    `json:"topics,omitempty"`
	TopicPattern      string                      `json:"topic_pattern,omitempty"`
	TopicPartitions   []TopicPartition            `json:"topic_partitions,omitempty"`
	Generation        int                         `json:"generation"`
	Members           map[string][]int            `json:"members"`
	TopicAssignments  map[string][]TopicPartition `json:"topic_assignments,omitempty"`
	Partitions        []int                       `json:"partitions,omitempty"`
	LastRebalance     time.Time                   `json:"last_rebalance,omitempty"`
	LastActivity      time.Time                   `json:"last_activity,omitempty"`
	Offsets           map[string]map[int]uint64   `json:"offsets"`
	RegistrationEpoch uint64                      `json:"registration_epoch,omitempty"`
	OffsetRevisions   map[string]uint64           `json:"offset_revisions,omitempty"`
	Deleted           bool                        `json:"deleted,omitempty"`
}

// GroupStatus represents the status of a consumer group
type GroupStatus struct {
	Status         string       `json:"status,omitempty"`
	GroupName      string       `json:"group_name"`
	TopicName      string       `json:"topic_name"`
	Topics         []string     `json:"topics,omitempty"`
	TopicPattern   string       `json:"topic_pattern,omitempty"`
	State          string       `json:"state"` // "Stable", "Rebalancing", "Dead"
	Generation     int          `json:"generation"`
	MemberCount    int          `json:"member_count"`
	PartitionCount int          `json:"partition_count"`
	Members        []MemberInfo `json:"members"`
	LastRebalance  time.Time    `json:"last_rebalance"`
}

type MemberInfo struct {
	MemberID         string           `json:"member_id"`
	LastHeartbeat    time.Time        `json:"last_heartbeat"`
	Assignments      []int            `json:"assignments"`
	TopicAssignments []TopicPartition `json:"topic_assignments,omitempty"`
}

const (
	ConsumerGroupStateStable = "stable"
	ConsumerGroupStateEmpty  = "empty"

	ObservationFailureCoordinatorLookup = "coordinator_lookup"
	ObservationFailureGroupLookup       = "group_lookup"
	ObservationFailureTopicLookup       = "topic_lookup"
)

// ConsumerGroupObservation is the bounded-cardinality lifecycle view used by
// the broker exporter. It intentionally excludes member and broker identity.
type ConsumerGroupObservation struct {
	TopicName        string
	GroupName        string
	MemberCount      int
	State            string
	LastActivity     time.Time
	LastRebalance    time.Time
	CoordinatorUp    bool
	ObservationError string
}

type consumerGroupObservationRef struct {
	topic string
	group string
}

type OffsetItem struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// NewCoordinator creates a new Coordinator instance.
// The provided ctx controls the lifetime of background goroutines (e.g., heartbeat monitor).
func NewCoordinator(ctx context.Context, cfg *config.Config, handler TopicHandler) *Coordinator {
	coordinator, err := NewCoordinatorWithRecovery(ctx, cfg, handler)
	if err != nil {
		util.Error("Coordinator recovery failed: %v", err)
	}
	return coordinator
}

// NewCoordinatorWithRecovery initializes the internal metadata topic and
// completes consumer metadata replay before returning success.
func NewCoordinatorWithRecovery(ctx context.Context, cfg *config.Config, handler TopicHandler) (*Coordinator, error) {
	if handler == nil {
		return nil, fmt.Errorf("coordinator requires a non-nil topic handler")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	childCtx, cancel := context.WithCancel(ctx)
	standalone := cfg == nil || !cfg.EnabledDistribution
	c := &Coordinator{
		groups:                    make(map[string]*GroupMetadata),
		lifecyclePending:          make(map[string]bool),
		cfg:                       cfg,
		ctx:                       childCtx,
		cancel:                    cancel,
		topicHandler:              handler,
		offsetTopic:               config.ConsumerOffsetsTopicName,
		offsetTopicPartitionCount: 4,
		standalone:                standalone,
		groupEpochs:               make(map[string]uint64),
		ownershipSince:            make(map[string]time.Time),
		recovery: ConsumerMetadataRecoveryStatus{
			Phase: "internal_topic_validation",
		},
	}
	recoveryComplete := false
	defer func() {
		if !recoveryComplete {
			cancel()
		}
	}()

	if provider, ok := handler.(offsetTopicPartitionProvider); ok {
		partitionCount, err := provider.ExistingPartitionCount(c.offsetTopic)
		if err != nil {
			recoveryErr := fmt.Errorf("discover internal consumer metadata partitions: %w", err)
			c.setRecoveryFailure(recoveryErr)
			return c, recoveryErr
		}
		if partitionCount > c.offsetTopicPartitionCount {
			c.offsetTopicPartitionCount = partitionCount
		}
	}

	if err := handler.CreateTopic(c.offsetTopic, c.offsetTopicPartitionCount, false, false); err != nil {
		recoveryErr := fmt.Errorf("validate internal consumer metadata topic %q: %w", c.offsetTopic, err)
		c.setRecoveryFailure(recoveryErr)
		return c, recoveryErr
	}
	if c.standalone {
		if reader, ok := handler.(OffsetLogReader); ok {
			if recoveryErr := c.LoadOffsetsFromLog(reader); recoveryErr != nil {
				wrapped := fmt.Errorf("replay internal consumer metadata from %q: %w", c.offsetTopic, recoveryErr)
				c.setRecoveryFailure(wrapped)
				return c, wrapped
			}
		} else {
			c.markRecoveryComplete(ConsumerMetadataRecoveryStatus{})
		}
	} else {
		// Distributed consumer metadata is restored exclusively through the
		// versioned Raft snapshot and log. The local internal topic is not an
		// independent recovery authority.
		c.markRecoveryComplete(ConsumerMetadataRecoveryStatus{})
	}
	recoveryComplete = true
	return c, nil
}

func (c *Coordinator) SetGroupSessionCallbacks(
	ownerChecker func(groupName string) bool,
	expirationHandler func(groupName string, generation int, memberIDs []string) error,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groupOwnerChecker = ownerChecker
	c.expirationHandler = expirationHandler
	c.ownershipSince = make(map[string]time.Time)
}

// SetGroupObservationResolver configures distributed exporter ownership.
// A failed lookup is treated as non-authoritative and reported with a bounded
// reason; raw resolver errors never enter metric labels.
func (c *Coordinator) SetGroupObservationResolver(resolver func(groupName string) (bool, error)) {
	c.mu.Lock()
	c.observationOwner = resolver
	c.observationOwners = nil
	c.mu.Unlock()
}

// SetGroupObservationBatchResolver configures a scrape-scoped distributed
// exporter ownership lookup. The resolver receives every known group so the
// cluster membership and coordinator ring only need to be inspected once.
func (c *Coordinator) SetGroupObservationBatchResolver(resolver func(groupNames []string) (map[string]bool, error)) {
	c.mu.Lock()
	c.observationOwner = nil
	c.observationOwners = resolver
	c.mu.Unlock()
}

// ObserveConsumerGroups returns a sanitized, scrape-time lifecycle view. In
// distributed mode only the resolved local coordinator includes lifecycle
// values; every broker still returns an authority result for each known group.
func (c *Coordinator) ObserveConsumerGroups() []ConsumerGroupObservation {
	if c == nil {
		return nil
	}

	c.mu.RLock()
	refs := make([]consumerGroupObservationRef, 0, len(c.groups))
	for name, group := range c.groups {
		if group == nil {
			continue
		}
		refs = append(refs, consumerGroupObservationRef{topic: group.TopicName, group: name})
	}
	standalone := c.standalone
	resolver := c.observationOwner
	batchResolver := c.observationOwners
	c.mu.RUnlock()
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].topic != refs[j].topic {
			return refs[i].topic < refs[j].topic
		}
		return refs[i].group < refs[j].group
	})

	var (
		authoritativeByGroup map[string]bool
		batchErr             error
	)
	if !standalone && batchResolver != nil && len(refs) > 0 {
		groupNames := make([]string, len(refs))
		for i, ref := range refs {
			groupNames[i] = ref.group
		}
		authoritativeByGroup, batchErr = batchResolver(groupNames)
	}

	observations := make([]ConsumerGroupObservation, len(refs))
	for i, ref := range refs {
		observation := &observations[i]
		observation.TopicName = ref.topic
		observation.GroupName = ref.group
		authoritative := standalone
		if !standalone {
			if batchResolver != nil {
				var found bool
				authoritative, found = authoritativeByGroup[ref.group]
				if batchErr != nil || !found {
					observation.ObservationError = ObservationFailureCoordinatorLookup
					continue
				}
			} else if resolver == nil {
				observation.ObservationError = ObservationFailureCoordinatorLookup
				continue
			} else {
				var err error
				authoritative, err = resolver(ref.group)
				if err != nil {
					observation.ObservationError = ObservationFailureCoordinatorLookup
					continue
				}
			}
		}
		observation.CoordinatorUp = authoritative
	}

	c.mu.RLock()
	for i, ref := range refs {
		observation := &observations[i]
		if !observation.CoordinatorUp {
			continue
		}
		group := c.groups[ref.group]
		if group == nil || group.TopicName != ref.topic {
			observation.CoordinatorUp = false
			observation.ObservationError = ObservationFailureGroupLookup
			continue
		}
		observation.MemberCount = len(group.Members)
		observation.LastActivity = group.LastActivity
		observation.LastRebalance = group.LastRebalance

		observation.State = ConsumerGroupStateStable
		if observation.MemberCount == 0 {
			observation.State = ConsumerGroupStateEmpty
		}
	}
	c.mu.RUnlock()
	return observations
}

// Start launches background monitoring processes (e.g., heartbeat monitor).
func (c *Coordinator) Start() {
	go c.monitorHeartbeats()
}

// Stop cancels the coordinator context, shutting down all background goroutines.
func (c *Coordinator) Stop() {
	c.cancel()
}

// GetAssignments returns the current partition assignments for each group member.
func (c *Coordinator) GetAssignments(groupName string) map[string][]int {
	c.mu.RLock()
	group := c.groups[groupName]
	if group == nil || len(group.Members) == 0 {
		c.mu.RUnlock()
		return map[string][]int{}
	}

	result := make(map[string][]int, len(group.Members))
	for id, member := range group.Members {
		if len(member.Assignments) == 0 {
			result[id] = []int{}
			continue
		}
		cp := make([]int, len(member.Assignments))
		copy(cp, member.Assignments)
		result[id] = cp
	}
	c.mu.RUnlock()
	return result
}

// GetMemberAssignments returns the partition assignments for a specific member in a group.
func (c *Coordinator) GetMemberAssignments(groupName string, memberID string) []int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group := c.groups[groupName]
	if group == nil {
		return nil
	}

	member, exists := group.Members[memberID]
	if !exists || len(member.Assignments) == 0 {
		return []int{}
	}

	cp := make([]int, len(member.Assignments))
	copy(cp, member.Assignments)
	return cp
}

// GetMemberTopicAssignments returns assignments with their topic identity.
func (c *Coordinator) GetMemberTopicAssignments(groupName, memberID string) []TopicPartition {
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	if group == nil || group.Members[memberID] == nil {
		return nil
	}
	member := group.Members[memberID]
	if len(member.TopicAssignments) > 0 {
		return append([]TopicPartition(nil), member.TopicAssignments...)
	}
	result := make([]TopicPartition, 0, len(member.Assignments))
	for _, partition := range member.Assignments {
		result = append(result, TopicPartition{Topic: group.TopicName, Partition: partition})
	}
	return result
}

func (c *Coordinator) ListGroups() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	groups := make([]string, 0, len(c.groups))
	for name := range c.groups {
		groups = append(groups, name)
	}
	return groups
}

// GetGroupStatus returns the current status of a consumer group
func (c *Coordinator) GetGroupStatus(groupName string) (*GroupStatus, error) {
	c.mu.RLock()
	group := c.groups[groupName]
	if group == nil {
		c.mu.RUnlock()
		return nil, fmt.Errorf("group '%s' not found", groupName)
	}

	gName := groupName
	tName := group.TopicName
	topics := append([]string(nil), group.Topics...)
	topicPattern := group.TopicPattern
	gen := group.Generation
	lRebalance := group.LastRebalance
	mCount := len(group.Members)
	pCount := len(group.Partitions)
	if len(group.TopicPartitions) > 0 {
		pCount = len(group.TopicPartitions)
	}

	members := make([]MemberInfo, 0, mCount)
	for _, member := range group.Members {
		asgn := make([]int, len(member.Assignments))
		copy(asgn, member.Assignments)

		members = append(members, MemberInfo{
			MemberID:         member.ID,
			LastHeartbeat:    member.LastHeartbeat,
			Assignments:      asgn,
			TopicAssignments: append([]TopicPartition(nil), member.TopicAssignments...),
		})
	}
	c.mu.RUnlock()

	state := "Stable"
	if mCount == 0 {
		state = "Dead"
	}

	return &GroupStatus{
		GroupName:      gName,
		TopicName:      tName,
		Topics:         topics,
		TopicPattern:   topicPattern,
		State:          state,
		Generation:     gen,
		MemberCount:    mCount,
		PartitionCount: pCount,
		Members:        members,
		LastRebalance:  lRebalance,
	}, nil
}

func (c *Coordinator) GetGroup(groupName string) *GroupMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.groups[groupName]
}

func (c *Coordinator) IsSubscriptionGroup(groupName string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	return group != nil && len(group.TopicPartitions) > 0
}

func (c *Coordinator) GetGeneration(groupName string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if group := c.groups[groupName]; group != nil {
		return group.Generation
	}
	return 0
}

func (c *Coordinator) GetRegistrationEpoch(groupName string) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if group := c.groups[groupName]; group != nil {
		return group.RegistrationEpoch
	}
	return 0
}

// ValidateMemberGeneration returns a wire-ready error code when a member is not
// valid for the supplied group generation. Empty string means valid.
func (c *Coordinator) ValidateMemberGeneration(groupName, memberID string, generation int) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.validateMemberGenerationLocked(groupName, memberID, generation)
}

func (c *Coordinator) validateMemberGenerationLocked(groupName, memberID string, generation int) string {
	group := c.groups[groupName]
	if group == nil {
		return fmt.Sprintf("ERROR: group_not_found group=%s", groupName)
	}
	if memberID == "" {
		return "ERROR: missing_member"
	}
	if group.Members[memberID] == nil {
		return fmt.Sprintf("ERROR: member_not_found member=%s group=%s", memberID, groupName)
	}
	if generation >= 0 && group.Generation != generation {
		return fmt.Sprintf("ERROR: GEN_MISMATCH current=%d requested=%d group=%s member=%s", group.Generation, generation, groupName, memberID)
	}
	return ""
}

// ResumeConsumer refreshes a known member session without changing membership,
// generation, or assignments. It is used after a transient reconnect.
func (c *Coordinator) ResumeConsumer(groupName, memberID string, generation int) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return nil, fmt.Errorf("%s", errResp)
	}
	member := c.groups[groupName].Members[memberID]
	now := time.Now()
	member.LastHeartbeat = now
	c.groups[groupName].LastActivity = now
	return append([]int(nil), member.Assignments...), nil
}

func (c *Coordinator) ResumeConsumerTopicAssignments(groupName, memberID string, generation int) ([]TopicPartition, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return nil, fmt.Errorf("%s", errResp)
	}
	member := c.groups[groupName].Members[memberID]
	member.LastHeartbeat = time.Now()
	return append([]TopicPartition(nil), member.TopicAssignments...), nil
}

// ValidateOwnershipFailure returns a wire-ready error code when a member does
// not own a partition in the supplied generation. Empty string means valid.
func (c *Coordinator) ValidateOwnershipFailure(groupName, memberID string, generation int, partition int) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return errResp
	}

	group := c.groups[groupName]
	member := group.Members[memberID]
	if !contains(member.Assignments, partition) {
		return fmt.Sprintf("ERROR: NOT_OWNER partition=%d member=%s group=%s generation=%d", partition, memberID, groupName, generation)
	}
	return ""
}

func (c *Coordinator) ValidateTopicPartitionOwnershipFailure(groupName, memberID string, generation int, topic string, partition int) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return errResp
	}
	member := c.groups[groupName].Members[memberID]
	for _, assigned := range member.TopicAssignments {
		if assigned.Topic == topic && assigned.Partition == partition {
			return ""
		}
	}
	if len(member.TopicAssignments) == 0 && groupTopicMatches(c.groups[groupName].TopicName, topic) && contains(member.Assignments, partition) {
		return ""
	}
	return fmt.Sprintf("ERROR: NOT_OWNER topic=%s partition=%d member=%s group=%s generation=%d", topic, partition, memberID, groupName, generation)
}

func (c *Coordinator) WithTopicOwnershipFence(groupName, memberID string, generation int, partitions []TopicPartition, fn func() error) error {
	c.mu.RLock()
	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		c.mu.RUnlock()
		return fmt.Errorf("%s", errResp)
	}
	member := c.groups[groupName].Members[memberID]
	for _, requested := range partitions {
		owned := false
		for _, assigned := range member.TopicAssignments {
			if assigned == requested {
				owned = true
				break
			}
		}
		if !owned && len(member.TopicAssignments) == 0 && groupTopicMatches(c.groups[groupName].TopicName, requested.Topic) {
			owned = contains(member.Assignments, requested.Partition)
		}
		if !owned {
			c.mu.RUnlock()
			return fmt.Errorf("ERROR: NOT_OWNER topic=%s partition=%d member=%s group=%s generation=%d", requested.Topic, requested.Partition, memberID, groupName, generation)
		}
	}
	c.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn()
}
func (c *Coordinator) WithOwnershipFence(groupName, memberID string, generation int, partitions []int, fn func() error) error {
	c.mu.RLock()
	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		c.mu.RUnlock()
		return fmt.Errorf("%s", errResp)
	}
	group := c.groups[groupName]
	member := group.Members[memberID]
	for _, partition := range partitions {
		if !contains(member.Assignments, partition) {
			c.mu.RUnlock()
			return fmt.Errorf("ERROR: NOT_OWNER partition=%d member=%s group=%s generation=%d", partition, memberID, groupName, generation)
		}
	}
	c.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn()
}
func contains(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// getGroupSafe returns the GroupMetadata for the given name under the global read lock.
func (c *Coordinator) getGroupSafe(name string) *GroupMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.groups[name]
}

// getOffsetSafe reads an offset from the group's per-group offset map.
// GUARDED_BY(gm.mu) — caller must hold at least gm.mu.RLock.
func (gm *GroupMetadata) getOffsetSafe(topic string, partition int) (uint64, bool) {
	if partitions, ok := gm.Offsets[topic]; ok {
		if offset, ok := partitions[partition]; ok {
			return offset, true
		}
	}
	return 0, false
}

// storeOffset writes an offset into the group's per-group offset map.
// GUARDED_BY(gm.mu) - caller must hold gm.mu.Lock (exclusive).
func (gm *GroupMetadata) storeOffset(topic string, partition int, offset uint64) {
	if gm.Offsets == nil {
		gm.Offsets = make(map[string]map[int]uint64)
	}
	if _, ok := gm.Offsets[topic]; !ok {
		gm.Offsets[topic] = make(map[int]uint64)
	}
	gm.Offsets[topic][partition] = offset
}

// storeOffsetMonotonic writes an offset only if it does not move the committed
// position backwards. Equal offsets are idempotent and accepted.
// GUARDED_BY(gm.mu) — caller must hold gm.mu.Lock (exclusive).
func (gm *GroupMetadata) storeOffsetMonotonic(groupName, topic string, partition int, offset uint64) error {
	if current, ok := gm.getOffsetSafe(topic, partition); ok && offset < current {
		return fmt.Errorf("offset regression for group=%s topic=%s partition=%d: current=%d attempted=%d", groupName, topic, partition, current, offset)
	}
	gm.storeOffset(topic, partition, offset)
	return nil
}

// ExportState returns a serializable snapshot of all consumer groups.
func (c *Coordinator) ExportState() map[string]*GroupStateSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*GroupStateSnapshot, len(c.groupEpochs))
	for name, group := range c.groups {
		group.mu.RLock()
		snap := &GroupStateSnapshot{
			TopicName:         group.TopicName,
			Topics:            append([]string(nil), group.Topics...),
			TopicPattern:      group.TopicPattern,
			TopicPartitions:   append([]TopicPartition(nil), group.TopicPartitions...),
			Generation:        group.Generation,
			Members:           make(map[string][]int, len(group.Members)),
			TopicAssignments:  make(map[string][]TopicPartition, len(group.Members)),
			Partitions:        append([]int(nil), group.Partitions...),
			LastRebalance:     group.LastRebalance,
			LastActivity:      group.LastActivity,
			Offsets:           make(map[string]map[int]uint64),
			RegistrationEpoch: group.RegistrationEpoch,
			OffsetRevisions:   make(map[string]uint64, len(group.OffsetRevisions)),
		}
		for mid, member := range group.Members {
			assignments := make([]int, len(member.Assignments))
			copy(assignments, member.Assignments)
			snap.Members[mid] = assignments
			snap.TopicAssignments[mid] = append([]TopicPartition(nil), member.TopicAssignments...)
		}
		for topic, partitions := range group.Offsets {
			snap.Offsets[topic] = make(map[int]uint64, len(partitions))
			for pid, offset := range partitions {
				snap.Offsets[topic][pid] = offset
			}
		}
		for topic, revision := range group.OffsetRevisions {
			snap.OffsetRevisions[topic] = revision
		}
		group.mu.RUnlock()
		result[name] = snap
	}
	for name, epoch := range c.groupEpochs {
		if _, live := c.groups[name]; live {
			continue
		}
		result[name] = &GroupStateSnapshot{
			RegistrationEpoch: epoch,
			Deleted:           true,
		}
	}
	return result
}

// ImportState restores consumer group state from a current-version snapshot.
func (c *Coordinator) ImportState(state map[string]*GroupStateSnapshot) error {
	if err := ValidateImportState(state); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.groups = make(map[string]*GroupMetadata, len(state))
	c.groupEpochs = make(map[string]uint64, len(state))
	c.lifecyclePending = make(map[string]bool)
	c.ownershipSince = make(map[string]time.Time)
	for name, snap := range state {
		c.groupEpochs[name] = snap.RegistrationEpoch
		if snap.Deleted {
			continue
		}
		group := &GroupMetadata{
			TopicName:         snap.TopicName,
			Topics:            append([]string(nil), snap.Topics...),
			TopicPattern:      snap.TopicPattern,
			TopicPartitions:   append([]TopicPartition(nil), snap.TopicPartitions...),
			Generation:        snap.Generation,
			Members:           make(map[string]*MemberMetadata, len(snap.Members)),
			Partitions:        append([]int(nil), snap.Partitions...),
			LastRebalance:     snap.LastRebalance,
			LastActivity:      snap.LastActivity,
			Offsets:           make(map[string]map[int]uint64),
			RegistrationEpoch: snap.RegistrationEpoch,
			OffsetRevisions:   make(map[string]uint64, len(snap.OffsetRevisions)),
		}
		for mid, assignments := range snap.Members {
			group.Members[mid] = &MemberMetadata{
				ID:               mid,
				LastHeartbeat:    time.Now(),
				Assignments:      append([]int(nil), assignments...),
				TopicAssignments: append([]TopicPartition(nil), snap.TopicAssignments[mid]...),
			}
		}

		for topic, partitions := range snap.Offsets {
			group.Offsets[topic] = make(map[int]uint64, len(partitions))
			for pid, offset := range partitions {
				group.Offsets[topic][pid] = offset
			}
		}
		for topic, revision := range snap.OffsetRevisions {
			group.OffsetRevisions[topic] = revision
		}

		c.groups[name] = group
	}
	return nil
}

// ValidateImportState rejects incomplete or internally inconsistent group
// snapshots before any live coordinator state is replaced.
func ValidateImportState(state map[string]*GroupStateSnapshot) error {
	for name, snap := range state {
		if name == "" {
			return fmt.Errorf("consumer group snapshot has an empty group name")
		}
		if snap == nil {
			return fmt.Errorf("consumer group %q snapshot is nil", name)
		}
		if snap.RegistrationEpoch == 0 {
			return fmt.Errorf("consumer group %q snapshot is missing registration epoch; clean bootstrap is required", name)
		}
		if snap.Deleted {
			if snap.TopicName != "" || len(snap.Topics) != 0 || snap.TopicPattern != "" ||
				len(snap.TopicPartitions) != 0 || snap.Generation != 0 || len(snap.Members) != 0 ||
				len(snap.TopicAssignments) != 0 || len(snap.Partitions) != 0 || len(snap.Offsets) != 0 || len(snap.OffsetRevisions) != 0 {
				return fmt.Errorf("consumer group %q tombstone contains live state", name)
			}
			continue
		}
		if snap.Generation < 0 {
			return fmt.Errorf("consumer group %q snapshot has negative generation %d", name, snap.Generation)
		}
		if snap.LastActivity.IsZero() {
			return fmt.Errorf("consumer group %q snapshot is missing last activity; clean bootstrap is required", name)
		}

		isSubscription := len(snap.Topics) > 0 || snap.TopicPattern != "" || len(snap.TopicPartitions) > 0
		if isSubscription {
			if err := validateSubscriptionSnapshot(name, snap); err != nil {
				return err
			}
		} else if err := validateLegacyGroupSnapshot(name, snap); err != nil {
			return err
		}

		for topicName, offsets := range snap.Offsets {
			if !snapshotTopicMatches(snap, topicName) {
				return fmt.Errorf("consumer group %q snapshot offset topic %q does not match its subscription", name, topicName)
			}
			for partition := range offsets {
				if !snapshotPartitionDeclared(snap, topicName, partition) {
					return fmt.Errorf("consumer group %q offset references undeclared topic-partition %s:%d", name, topicName, partition)
				}
			}
		}
		for topicName, revision := range snap.OffsetRevisions {
			if !snapshotTopicMatches(snap, topicName) {
				return fmt.Errorf("consumer group %q snapshot revision topic %q does not match its subscription", name, topicName)
			}
			if revision == 0 {
				return fmt.Errorf("consumer group %q snapshot has zero offset revision for topic %q", name, topicName)
			}
		}
	}
	return nil
}

func validateLegacyGroupSnapshot(name string, snap *GroupStateSnapshot) error {
	if snap.TopicName == "" {
		return fmt.Errorf("consumer group %q snapshot is missing topic", name)
	}
	if len(snap.Partitions) == 0 {
		return fmt.Errorf("consumer group %q snapshot is missing declared partitions; clean bootstrap is required", name)
	}
	for memberID, assignments := range snap.TopicAssignments {
		if _, ok := snap.Members[memberID]; !ok || len(assignments) != 0 {
			return fmt.Errorf("consumer group %q legacy snapshot contains invalid topic assignments for member %q", name, memberID)
		}
	}
	declared := make(map[int]struct{}, len(snap.Partitions))
	for _, partition := range snap.Partitions {
		if partition < 0 {
			return fmt.Errorf("consumer group %q snapshot has negative partition %d", name, partition)
		}
		if _, duplicate := declared[partition]; duplicate {
			return fmt.Errorf("consumer group %q snapshot has duplicate partition %d", name, partition)
		}
		declared[partition] = struct{}{}
	}
	for partition := 0; partition < len(snap.Partitions); partition++ {
		if _, ok := declared[partition]; !ok {
			return fmt.Errorf("consumer group %q snapshot partitions must be contiguous from zero", name)
		}
	}

	assigned := make(map[int]string, len(declared))
	for memberID, assignments := range snap.Members {
		if memberID == "" {
			return fmt.Errorf("consumer group %q snapshot has an empty member id", name)
		}
		memberPartitions := make(map[int]struct{}, len(assignments))
		for _, partition := range assignments {
			if _, ok := declared[partition]; !ok {
				return fmt.Errorf("consumer group %q member %q references undeclared partition %d", name, memberID, partition)
			}
			if _, duplicate := memberPartitions[partition]; duplicate {
				return fmt.Errorf("consumer group %q member %q has duplicate partition %d", name, memberID, partition)
			}
			if owner, duplicate := assigned[partition]; duplicate {
				return fmt.Errorf("consumer group %q partition %d is assigned to both %q and %q", name, partition, owner, memberID)
			}
			memberPartitions[partition] = struct{}{}
			assigned[partition] = memberID
		}
	}

	return nil
}

func validateSubscriptionSnapshot(name string, snap *GroupStateSnapshot) error {
	if len(snap.Topics) == 0 || len(snap.TopicPartitions) == 0 {
		return fmt.Errorf("consumer group %q snapshot is missing subscription topics or partitions", name)
	}
	if snap.TopicName != subscriptionDisplayName(snap.Topics, snap.TopicPattern) {
		return fmt.Errorf("consumer group %q snapshot has inconsistent subscription display topic", name)
	}
	if len(snap.Partitions) != 0 {
		return fmt.Errorf("consumer group %q subscription snapshot contains legacy partitions", name)
	}
	topics := make(map[string]struct{}, len(snap.Topics))
	for i, topicName := range snap.Topics {
		if topicName == "" || (i > 0 && snap.Topics[i-1] >= topicName) {
			return fmt.Errorf("consumer group %q snapshot topics must be non-empty, unique, and sorted", name)
		}
		topics[topicName] = struct{}{}
	}
	declared := make(map[TopicPartition]struct{}, len(snap.TopicPartitions))
	counts := make(map[string]int, len(topics))
	for _, tp := range snap.TopicPartitions {
		if _, ok := topics[tp.Topic]; !ok || tp.Partition < 0 {
			return fmt.Errorf("consumer group %q snapshot has undeclared topic-partition %s:%d", name, tp.Topic, tp.Partition)
		}
		if _, duplicate := declared[tp]; duplicate {
			return fmt.Errorf("consumer group %q snapshot has duplicate topic-partition %s:%d", name, tp.Topic, tp.Partition)
		}
		declared[tp] = struct{}{}
		counts[tp.Topic]++
	}
	for topicName := range topics {
		if counts[topicName] == 0 {
			return fmt.Errorf("consumer group %q snapshot is missing partitions for topic %q", name, topicName)
		}
		for partition := 0; partition < counts[topicName]; partition++ {
			if _, ok := declared[TopicPartition{Topic: topicName, Partition: partition}]; !ok {
				return fmt.Errorf("consumer group %q snapshot partitions for topic %q must be contiguous from zero", name, topicName)
			}
		}
	}
	assigned := make(map[TopicPartition]string, len(declared))
	for memberID := range snap.Members {
		if memberID == "" {
			return fmt.Errorf("consumer group %q snapshot has an empty member id", name)
		}
		memberAssignments := make(map[TopicPartition]struct{})
		if len(snap.Members[memberID]) != 0 {
			return fmt.Errorf("consumer group %q subscription member %q contains legacy assignments", name, memberID)
		}
		for _, tp := range snap.TopicAssignments[memberID] {
			if _, ok := declared[tp]; !ok {
				return fmt.Errorf("consumer group %q member %q references undeclared topic-partition %s:%d", name, memberID, tp.Topic, tp.Partition)
			}
			if _, duplicate := memberAssignments[tp]; duplicate {
				return fmt.Errorf("consumer group %q member %q has duplicate topic-partition %s:%d", name, memberID, tp.Topic, tp.Partition)
			}
			if owner, duplicate := assigned[tp]; duplicate {
				return fmt.Errorf("consumer group %q topic-partition %s:%d is assigned to both %q and %q", name, tp.Topic, tp.Partition, owner, memberID)
			}
			memberAssignments[tp] = struct{}{}
			assigned[tp] = memberID
		}
	}
	for memberID := range snap.TopicAssignments {
		if _, ok := snap.Members[memberID]; !ok {
			return fmt.Errorf("consumer group %q snapshot has assignments for unknown member %q", name, memberID)
		}
	}
	return nil
}

func snapshotTopicMatches(snap *GroupStateSnapshot, topicName string) bool {
	if len(snap.Topics) == 0 {
		return groupTopicMatches(snap.TopicName, topicName)
	}
	for _, subscribed := range snap.Topics {
		if subscribed == topicName {
			return true
		}
	}
	return false
}

func snapshotPartitionDeclared(snap *GroupStateSnapshot, topicName string, partition int) bool {
	if len(snap.TopicPartitions) == 0 {
		return partition >= 0 && partition < len(snap.Partitions)
	}
	for _, declared := range snap.TopicPartitions {
		if declared.Topic == topicName && declared.Partition == partition {
			return true
		}
	}
	return false
}
