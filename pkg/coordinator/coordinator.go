package coordinator

import (
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
	stopCh chan struct{}

	topicHandler              TopicHandler
	offsetTopic               string
	offsetTopicPartitionCount int

	offsets map[string]map[string]map[int]uint64 // group -> topic -> partition -> offset
}

type TopicHandler interface {
	Publish(topic string, msg *types.Message) error
	CreateTopic(topic string, partitionCount int)
}

// GroupMetadata holds metadata for a single consumer group.
type GroupMetadata struct {
	TopicName     string                     // Topic this group consumes
	Members       map[string]*MemberMetadata // Active members
	Generation    int                        // Current generation (unused but reserved)
	Partitions    []int                      // All partitions of the topic
	LastRebalance time.Time                  // Timestamp of last rebalance
}

// MemberMetadata holds state for a single consumer instance.
type MemberMetadata struct {
	ID            string    // Unique consumer ID
	LastHeartbeat time.Time // Last heartbeat timestamp
	Assignments   []int     // Partition assignments for this member
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
func NewCoordinator(cfg *config.Config, handler TopicHandler) *Coordinator {
	if handler == nil {
		util.Fatal("Coordinator requires a non-nil TopicHandler")
	}

	c := &Coordinator{
		groups:                    make(map[string]*GroupMetadata),
		cfg:                       cfg,
		stopCh:                    make(chan struct{}),
		topicHandler:              handler,
		offsetTopic:               "__consumer_offsets",
		offsetTopicPartitionCount: 4, // init. dynamic
		offsets:                   make(map[string]map[string]map[int]uint64),
	}

	handler.CreateTopic(c.offsetTopic, c.offsetTopicPartitionCount)
	return c
}

// Start launches background monitoring processes (e.g., heartbeat monitor).
func (c *Coordinator) Start() {
	go c.monitorHeartbeats()
}

// Stop launches background monitoring processes (graceful shutdown)
func (c *Coordinator) Stop() {
	close(c.stopCh)
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

func (c *Coordinator) getOffsetSafe(group, topic string, partition int) (uint64, bool) {
	if topics, ok := c.offsets[group]; ok {
		if partitions, ok := topics[topic]; ok {
			if offset, ok := partitions[partition]; ok {
				return offset, true
			}
		}
	}
	return 0, false
}
