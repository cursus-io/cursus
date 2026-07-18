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
	groups map[string]*GroupMetadata // All consumer groups
	mu     sync.RWMutex              // Global lock for coordinator state
	cfg    *config.Config            // Configuration reference
	ctx    context.Context
	cancel context.CancelFunc

	topicHandler              TopicHandler
	offsetTopic               string
	offsetTopicPartitionCount int

	// Session expiration is decided by the broker that owns each group. In a
	// cluster, the expiration callback serializes removals through the metadata
	// log so every broker observes the same generation and assignments.
	groupOwnerChecker func(groupName string) bool
	expirationHandler func(groupName string, generation int, memberIDs []string) error
	ownershipSince    map[string]time.Time
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

type syncPublisher interface {
	PublishWithAck(topic string, msg *types.Message) error
}

// GroupMetadata holds metadata for a single consumer group.
type GroupMetadata struct {
	mu            sync.RWMutex               // Per-group lock for offset operations
	TopicName     string                     // Topic this group consumes
	Members       map[string]*MemberMetadata // Active members
	Generation    int                        // Current membership generation
	Partitions    []int                      // All partitions of the topic
	LastRebalance time.Time                  // Timestamp of last rebalance
	Offsets       map[string]map[int]uint64  // topic -> partition -> offset
}

// MemberMetadata holds state for a single consumer instance.
type MemberMetadata struct {
	ID            string    // Unique consumer ID
	LastHeartbeat time.Time // Last heartbeat timestamp
	Assignments   []int     // Partition assignments for this member
}

// GroupStateSnapshot is a serializable snapshot of a consumer group's state.
type GroupStateSnapshot struct {
	TopicName     string                    `json:"topic"`
	Generation    int                       `json:"generation"`
	Members       map[string][]int          `json:"members"`
	Partitions    []int                     `json:"partitions,omitempty"`
	LastRebalance time.Time                 `json:"last_rebalance,omitempty"`
	Offsets       map[string]map[int]uint64 `json:"offsets"`
}

// GroupStatus represents the status of a consumer group
type GroupStatus struct {
	Status         string       `json:"status,omitempty"`
	GroupName      string       `json:"group_name"`
	TopicName      string       `json:"topic_name"`
	State          string       `json:"state"` // "Stable", "Rebalancing", "Dead"
	Generation     int          `json:"generation"`
	MemberCount    int          `json:"member_count"`
	PartitionCount int          `json:"partition_count"`
	Members        []MemberInfo `json:"members"`
	LastRebalance  time.Time    `json:"last_rebalance"`
}

type MemberInfo struct {
	MemberID      string    `json:"member_id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Assignments   []int     `json:"assignments"`
}

type OffsetCommitMessage struct {
	Group     string    `json:"group"`
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    uint64    `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

type OffsetItem struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

type BulkOffsetMsg struct {
	Group     string       `json:"group"`
	Topic     string       `json:"topic"`
	Offsets   []OffsetItem `json:"offsets"`
	Timestamp time.Time    `json:"timestamp"`
}

// NewCoordinator creates a new Coordinator instance.
// The provided ctx controls the lifetime of background goroutines (e.g., heartbeat monitor).
func NewCoordinator(ctx context.Context, cfg *config.Config, handler TopicHandler) *Coordinator {
	if handler == nil {
		util.Fatal("Coordinator requires a non-nil TopicHandler")
	}

	childCtx, cancel := context.WithCancel(ctx)

	c := &Coordinator{
		groups:                    make(map[string]*GroupMetadata),
		cfg:                       cfg,
		ctx:                       childCtx,
		cancel:                    cancel,
		topicHandler:              handler,
		offsetTopic:               "__consumer_offsets",
		offsetTopicPartitionCount: 4, // init. dynamic
		ownershipSince:            make(map[string]time.Time),
	}

	if provider, ok := handler.(offsetTopicPartitionProvider); ok {
		partitionCount, err := provider.ExistingPartitionCount(c.offsetTopic)
		if err != nil {
			util.Warn("Coordinator: failed to discover offset topic partitions: %v", err)
		} else if partitionCount > c.offsetTopicPartitionCount {
			c.offsetTopicPartitionCount = partitionCount
		}
	}

	if err := handler.CreateTopic(c.offsetTopic, c.offsetTopicPartitionCount, false, false); err != nil {
		util.Error("Coordinator: failed to create offset topic '%s': %v", c.offsetTopic, err)
	}
	if reader, ok := handler.(OffsetLogReader); ok {
		if err := c.LoadOffsetsFromLog(reader); err != nil {
			util.Error("Coordinator: failed to load committed offsets from '%s': %v", c.offsetTopic, err)
		}
	}
	return c
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
	gen := group.Generation
	lRebalance := group.LastRebalance
	mCount := len(group.Members)
	pCount := len(group.Partitions)

	members := make([]MemberInfo, 0, mCount)
	for _, member := range group.Members {
		asgn := make([]int, len(member.Assignments))
		copy(asgn, member.Assignments)

		members = append(members, MemberInfo{
			MemberID:      member.ID,
			LastHeartbeat: member.LastHeartbeat,
			Assignments:   asgn,
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

func (c *Coordinator) GetGeneration(groupName string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if group := c.groups[groupName]; group != nil {
		return group.Generation
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
	member.LastHeartbeat = time.Now()
	return append([]int(nil), member.Assignments...), nil
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

	result := make(map[string]*GroupStateSnapshot, len(c.groups))
	for name, group := range c.groups {
		group.mu.RLock()
		snap := &GroupStateSnapshot{
			TopicName:     group.TopicName,
			Generation:    group.Generation,
			Members:       make(map[string][]int, len(group.Members)),
			Partitions:    append([]int(nil), group.Partitions...),
			LastRebalance: group.LastRebalance,
			Offsets:       make(map[string]map[int]uint64),
		}
		for mid, member := range group.Members {
			assignments := make([]int, len(member.Assignments))
			copy(assignments, member.Assignments)
			snap.Members[mid] = assignments
		}
		for topic, partitions := range group.Offsets {
			snap.Offsets[topic] = make(map[int]uint64, len(partitions))
			for pid, offset := range partitions {
				snap.Offsets[topic][pid] = offset
			}
		}
		group.mu.RUnlock()
		result[name] = snap
	}
	return result
}

// ImportState restores consumer group state from a snapshot.
func (c *Coordinator) ImportState(state map[string]*GroupStateSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.groups = make(map[string]*GroupMetadata, len(state))
	c.ownershipSince = make(map[string]time.Time)
	for name, snap := range state {
		group := &GroupMetadata{
			TopicName:     snap.TopicName,
			Generation:    snap.Generation,
			Members:       make(map[string]*MemberMetadata, len(snap.Members)),
			Partitions:    append([]int(nil), snap.Partitions...),
			LastRebalance: snap.LastRebalance,
			Offsets:       make(map[string]map[int]uint64),
		}

		for mid, assignments := range snap.Members {
			group.Members[mid] = &MemberMetadata{
				ID:            mid,
				LastHeartbeat: time.Now(),
				Assignments:   append([]int(nil), assignments...),
			}
		}

		for topic, partitions := range snap.Offsets {
			group.Offsets[topic] = make(map[int]uint64, len(partitions))
			for pid, offset := range partitions {
				group.Offsets[topic][pid] = offset
			}
		}

		if len(group.Partitions) == 0 {
			group.Partitions = inferSnapshotPartitions(snap)
		}

		c.groups[name] = group
	}
}

func inferSnapshotPartitions(snap *GroupStateSnapshot) []int {
	seen := make(map[int]struct{})
	for _, assignments := range snap.Members {
		for _, partition := range assignments {
			seen[partition] = struct{}{}
		}
	}
	if topicOffsets := snap.Offsets[snap.TopicName]; topicOffsets != nil {
		for partition := range topicOffsets {
			seen[partition] = struct{}{}
		}
	}
	partitions := make([]int, 0, len(seen))
	for partition := range seen {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)
	return partitions
}
