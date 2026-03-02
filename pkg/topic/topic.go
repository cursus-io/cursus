package topic

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/config"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const DefaultBufSize = 10000
const DefaultConsumerBufSize = 1000

// Topic represents a logical message stream divided into partitions and consumer groups.
type Topic struct {
	Name           string
	Partitions     []*Partition
	counter        uint64
	consumerGroups map[string]*types.ConsumerGroup
	mu             sync.RWMutex
	cfg            *config.Config
	streamManager  StreamManager
}

// NewTopic initializes a topic with partitions.
func NewTopic(name string, partitionCount int, hp HandlerProvider, cfg *config.Config, sm StreamManager) (*Topic, error) {
	partitions := make([]*Partition, partitionCount)
	for i := 0; i < partitionCount; i++ {
		dh, err := hp.GetHandler(name, i)
		if err != nil {
			return nil, fmt.Errorf("open handler for %s[%d]: %w", name, i, err)
		}
		partitions[i] = NewPartition(i, name, dh, sm, cfg)
	}
	return &Topic{
		Name:           name,
		Partitions:     partitions,
		consumerGroups: make(map[string]*types.ConsumerGroup),
		cfg:            cfg,
		streamManager:  sm,
	}, nil
}

func (t *Topic) GetPartitionForMessage(msg types.Message) int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	partitionsLen := uint64(len(t.Partitions))
	if partitionsLen == 0 {
		return -1
	}

	if msg.Key != "" {
		keyID := util.GenerateID(msg.Key)
		return int(keyID % partitionsLen)
	}

	oldCounter := atomic.AddUint64(&t.counter, 1) - 1
	return int(oldCounter % partitionsLen)
}

// AddPartitions extends the topic with new partitions.
func (t *Topic) AddPartitions(extra int, hp HandlerProvider) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := 0; i < extra; i++ {
		idx := len(t.Partitions)
		dh, err := hp.GetHandler(t.Name, idx)
		if err != nil {
			util.Error("❌ failed to attach partition %d for topic '%s': %v\n", idx, t.Name, err)
			return
		}
		newP := NewPartition(idx, t.Name, dh, t.streamManager, t.cfg)
		t.Partitions = append(t.Partitions, newP)
	}
}

// RegisterConsumerGroup registers a consumer group to the topic.
func (t *Topic) RegisterConsumerGroup(groupName string, consumerCount int) *types.ConsumerGroup {
	t.mu.Lock()
	defer t.mu.Unlock()

	if g, ok := t.consumerGroups[groupName]; ok {
		return g
	}

	group := &types.ConsumerGroup{
		Name:             groupName,
		Consumers:        make([]*types.Consumer, consumerCount),
		CommittedOffsets: make(map[int]uint64),
	}

	for i := 0; i < consumerCount; i++ {
		group.Consumers[i] = &types.Consumer{
			ID: i,
		}
	}

	t.consumerGroups[groupName] = group
	return group
}

// DeregisterConsumerGroup removes a consumer group from the topic.
func (t *Topic) DeregisterConsumerGroup(groupName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.consumerGroups[groupName]; !ok {
		return fmt.Errorf("consumer group '%s' does not exist", groupName)
	}

	delete(t.consumerGroups, groupName)
	util.Info("Consumer group '%s' deregistered from topic '%s'", groupName, t.Name)
	return nil
}

// Publish sends a message to one partition.
func (t *Topic) Publish(msg types.Message) {
	idx := t.GetPartitionForMessage(msg)

	t.mu.RLock()
	defer t.mu.RUnlock()

	if idx == -1 {
		util.Error("❌ No partitions available for topic '%s'", t.Name)
		return
	}
	if idx < 0 || idx >= len(t.Partitions) {
		util.Error("❌ Partition index %d out of range for topic '%s'", idx, t.Name)
		return
	}

	p := t.Partitions[idx]
	p.Enqueue(msg)
}

func (t *Topic) PublishSync(msg types.Message) error {
	idx := t.GetPartitionForMessage(msg)

	t.mu.RLock()
	defer t.mu.RUnlock()

	if idx == -1 {
		return fmt.Errorf("no partitions available for topic '%s'", t.Name)
	}
	if idx < 0 || idx >= len(t.Partitions) {
		return fmt.Errorf("partition index %d out of range", idx)
	}

	return t.Partitions[idx].EnqueueSync(msg)
}

func (t *Topic) PublishBatchSync(msgs []types.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	partitioned := make(map[int][]types.Message)
	for _, msg := range msgs {
		idx := t.GetPartitionForMessage(msg)
		if idx != -1 {
			partitioned[idx] = append(partitioned[idx], msg)
		}
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	for idx, pm := range partitioned {
		if idx < 0 || idx >= len(t.Partitions) {
			continue
		}

		p := t.Partitions[idx]
		if err := p.EnqueueBatchSync(pm); err != nil {
			return fmt.Errorf("partition %d: failed to publish batch: %w", idx, err)
		}
	}
	return nil
}

func (t *Topic) GetPartition(partitionID int) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partitionID < 0 || partitionID >= len(t.Partitions) {
		return nil, fmt.Errorf("partition %d out of range for topic '%s' (0-%d)", partitionID, t.Name, len(t.Partitions)-1)
	}

	return t.Partitions[partitionID], nil
}

func (t *Topic) ReadSafeMessages(partitionID int, offset uint64, max int) ([]types.Message, error) {
	p, err := t.GetPartition(partitionID)
	if err != nil {
		return nil, err
	}
	return p.ReadCommitted(offset, max)
}

// applyAssignments connects partitions to consumers according to coordinator results.
func (t *Topic) applyAssignments(groupName string, assignments map[string][]int) {
	group := t.consumerGroups[groupName]
	if group == nil {
		return
	}

	util.Debug("Applied assignments for group '%s': %v", groupName, assignments)
}

func (t *Topic) GetCommittedOffset(groupName string, partition int) (uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	group, ok := t.consumerGroups[groupName]
	if !ok {
		return 0, false
	}

	offset, ok := group.CommittedOffsets[partition]
	return offset, ok
}

func (t *Topic) CommitOffset(groupName string, partition int, offset uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if partition < 0 || partition >= len(t.Partitions) {
		return fmt.Errorf("partition %d out of range [0, %d)", partition, len(t.Partitions))
	}

	group, ok := t.consumerGroups[groupName]
	if !ok {
		return fmt.Errorf("consumer group '%s' not found", groupName)
	}

	if group.CommittedOffsets == nil {
		group.CommittedOffsets = make(map[int]uint64)
	}
	group.CommittedOffsets[partition] = offset
	return nil
}

func (t *Topic) NewMessageSignal(partition int) <-chan struct{} {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		util.Warn("NewMessageSignal called with invalid partition %d for topic '%s'", partition, t.Name)
		return nil
	}
	return t.Partitions[partition].newMessageCh
}
