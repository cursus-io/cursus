package coordinator

import (
	"context"
	"fmt"
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

	// ensure only the active GroupCoordinator handles session expiration.
	leaderChecker func() bool
}

type TopicHandler interface {
	Publish(topic string, msg *types.Message) error
	CreateTopic(topic string, partitionCount int, idempotent bool, eventSourcing bool) error
}

// GroupMetadata holds metadata for a single consumer group.
type GroupMetadata struct {
	mu            sync.RWMutex               // Per-group lock for offset operations
	TopicName     string                     // Topic this group consumes
	Members       map[string]*MemberMetadata // Active members
	Generation    int                        // Current generation (unused but reserved)
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
	TopicName  string                    `json:"topic"`
	Generation int                       `json:"generation"`
	Members    map[string][]int          `json:"members"`
	Offsets    map[string]map[int]uint64 `json:"offsets"`
}

// GroupStatus represents the status of a consumer group
type GroupStatus struct {
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
	}

	if err := handler.CreateTopic(c.offsetTopic, c.offsetTopicPartitionCount, false, false); err != nil {
		util.Error("Coordinator: failed to create offset topic '%s': %v", c.offsetTopic, err)
	}
	return c
}

func (c *Coordinator) SetLeaderChecker(f func() bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaderChecker = f
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
// GUARDED_BY(gm.mu) — caller must hold gm.mu.Lock (exclusive).
func (gm *GroupMetadata) storeOffset(topic string, partition int, offset uint64) {
	if gm.Offsets == nil {
		gm.Offsets = make(map[string]map[int]uint64)
	}
	if _, ok := gm.Offsets[topic]; !ok {
		gm.Offsets[topic] = make(map[int]uint64)
	}
	gm.Offsets[topic][partition] = offset
}

// ExportState returns a serializable snapshot of all consumer groups.
func (c *Coordinator) ExportState() map[string]*GroupStateSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*GroupStateSnapshot, len(c.groups))
	for name, group := range c.groups {
		group.mu.RLock()
		snap := &GroupStateSnapshot{
			TopicName:  group.TopicName,
			Generation: group.Generation,
			Members:    make(map[string][]int, len(group.Members)),
			Offsets:    make(map[string]map[int]uint64),
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

	for name, snap := range state {
		group := &GroupMetadata{
			TopicName:  snap.TopicName,
			Generation: snap.Generation,
			Members:    make(map[string]*MemberMetadata, len(snap.Members)),
			Partitions: make([]int, 0),
			Offsets:    make(map[string]map[int]uint64),
		}

		for mid, assignments := range snap.Members {
			group.Members[mid] = &MemberMetadata{
				ID:            mid,
				LastHeartbeat: time.Now(),
				Assignments:   assignments,
			}
		}

		for topic, partitions := range snap.Offsets {
			group.Offsets[topic] = make(map[int]uint64, len(partitions))
			for pid, offset := range partitions {
				group.Offsets[topic][pid] = offset
			}
		}

		if topicOffsets, ok := snap.Offsets[snap.TopicName]; ok {
			for pid := range topicOffsets {
				group.Partitions = append(group.Partitions, pid)
			}
		}

		c.groups[name] = group
	}
}
