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
	Name            string
	Partitions      []*Partition
	counter         uint64
	consumerGroups  map[string]*types.ConsumerGroup
	mu              sync.RWMutex
	cfg             *config.Config
	streamManager   StreamManager
	IsIdempotent    bool
	IsEventSourcing bool
	Policy          Policy
	txnResolver     TransactionDecisionResolver
}

func (t *Topic) SetTransactionDecisionResolver(resolver TransactionDecisionResolver) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.txnResolver = resolver
	for _, partition := range t.Partitions {
		partition.SetTransactionDecisionResolver(resolver)
	}
}

type storagePolicySetter interface {
	SetStoragePolicy(cleanupPolicy string, hours int, bytes int64)
}

type retentionPolicySetter interface {
	SetRetentionPolicy(hours int, bytes int64)
}

type cleanupPolicySetter interface {
	SetCleanupPolicy(policy string)
}

type policyHandlerProvider interface {
	GetHandlerWithPolicy(topic string, partitionID int, cleanupPolicy string, retentionHours int, retentionBytes int64) (types.StorageHandler, error)
}

func getHandlerWithStoragePolicy(provider HandlerProvider, topic string, partitionID int, policy Policy) (types.StorageHandler, error) {
	if policyProvider, ok := provider.(policyHandlerProvider); ok {
		return policyProvider.GetHandlerWithPolicy(topic, partitionID, policy.CleanupPolicy, policy.RetentionHours, policy.RetentionBytes)
	}
	handler, err := provider.GetHandler(topic, partitionID)
	if err != nil {
		return nil, err
	}
	applyStoragePolicy(handler, policy)
	return handler, nil
}

func applyStoragePolicy(handler types.StorageHandler, policy Policy) {
	if setter, ok := handler.(storagePolicySetter); ok {
		setter.SetStoragePolicy(policy.CleanupPolicy, policy.RetentionHours, policy.RetentionBytes)
		return
	}
	if setter, ok := handler.(retentionPolicySetter); ok {
		setter.SetRetentionPolicy(policy.RetentionHours, policy.RetentionBytes)
	}
	if setter, ok := handler.(cleanupPolicySetter); ok {
		setter.SetCleanupPolicy(policy.CleanupPolicy)
	}
}

// NewTopic initializes a topic with partitions.
func NewTopic(name string, partitionCount int, hp HandlerProvider, cfg *config.Config, sm StreamManager, idempotent bool, eventSourcing bool) (*Topic, error) {
	policy := DefaultPolicy()
	if cfg != nil && cfg.CleanupPolicy != "" {
		policy.CleanupPolicy = cfg.CleanupPolicy
	}
	return NewTopicWithPolicy(name, partitionCount, hp, cfg, sm, idempotent, eventSourcing, policy)
}

func NewTopicWithPolicy(name string, partitionCount int, hp HandlerProvider, cfg *config.Config, sm StreamManager, idempotent bool, eventSourcing bool, policy Policy) (*Topic, error) {
	normalizedPolicy, err := policy.Normalize()
	if err != nil {
		return nil, err
	}
	if err := validateCleanupPolicyForTopic(normalizedPolicy, cfg, eventSourcing); err != nil {
		return nil, err
	}

	partitions := make([]*Partition, partitionCount)
	for i := 0; i < partitionCount; i++ {
		dh, err := getHandlerWithStoragePolicy(hp, name, i, normalizedPolicy)
		if err != nil {
			closePartiallyInitializedTopic(name, hp, partitions[:i])
			return nil, fmt.Errorf("open handler for %s[%d]: %w", name, i, err)
		}
		p := NewPartition(i, name, dh, sm, cfg)
		p.isIdempotent = idempotent
		p.RecoverProducerStateFromLog()
		p.StartProducerStateMaintenance()
		partitions[i] = p
	}
	return &Topic{
		Name:            name,
		Partitions:      partitions,
		consumerGroups:  make(map[string]*types.ConsumerGroup),
		cfg:             cfg,
		streamManager:   sm,
		IsIdempotent:    idempotent,
		IsEventSourcing: eventSourcing,
		Policy:          normalizedPolicy,
	}, nil
}

func closePartiallyInitializedTopic(name string, provider HandlerProvider, partitions []*Partition) {
	for _, partition := range partitions {
		partition.Close()
	}
	if closer, ok := provider.(topicHandlerCloser); ok {
		closer.CloseTopicHandlers(name)
		return
	}
	for _, partition := range partitions {
		if err := partition.dh.Close(); err != nil {
			util.Warn("Failed to close storage handler for partially initialized topic %s[%d]: %v", name, partition.ID, err)
		}
	}
}

// getPartitionIndex computes the target partition index without acquiring any lock.
// The caller must hold at least RLock and pass the current partition count.
func (t *Topic) getPartitionIndex(msg types.Message, partitionsLen int) int {
	if partitionsLen == 0 {
		return -1
	}

	if t.Policy.Partitioner == PartitionerHashKey && msg.Key != "" {
		keyID := util.GenerateID(msg.Key)
		return int(keyID % uint64(partitionsLen))
	}

	oldCounter := atomic.AddUint64(&t.counter, 1) - 1
	return int(oldCounter % uint64(partitionsLen))
}

// GetPartitionForMessage returns the partition index for a message.
// This is intended for external callers (e.g. TopicManager). Internal publish
// methods use getPartitionIndex under an already-held RLock to avoid TOCTOU races.
func (t *Topic) GetPartitionForMessage(msg types.Message) int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.getPartitionIndex(msg, len(t.Partitions))
}

// AddPartitions extends the topic with new partitions.
// Returns an error if any partition fails to initialize; partitions created
// before the failure are kept.
func (t *Topic) AddPartitions(extra int, hp HandlerProvider) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := 0; i < extra; i++ {
		idx := len(t.Partitions)
		dh, err := getHandlerWithStoragePolicy(hp, t.Name, idx, t.Policy)
		if err != nil {
			return fmt.Errorf("failed to attach partition %d for topic '%s': %w", idx, t.Name, err)
		}
		newP := NewPartition(idx, t.Name, dh, t.streamManager, t.cfg)
		newP.SetTransactionDecisionResolver(t.txnResolver)
		newP.isIdempotent = t.IsIdempotent
		newP.RecoverProducerStateFromLog()
		newP.StartProducerStateMaintenance()
		t.Partitions = append(t.Partitions, newP)
	}
	return nil
}

func (t *Topic) ApplyPolicy(policy Policy) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Policy = policy
	for _, partition := range t.Partitions {
		applyStoragePolicy(partition.dh, policy)
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
		Name:      groupName,
		Consumers: make([]*types.Consumer, consumerCount),
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
// Partition selection and enqueue happen under a single RLock to prevent
// TOCTOU races with AddPartitions.
func (t *Topic) Publish(msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx := t.getPartitionIndex(msg, len(t.Partitions))
	if idx == -1 {
		return fmt.Errorf("no partitions available for topic '%s'", t.Name)
	}

	t.Partitions[idx].Enqueue(msg)
	return nil
}

func (t *Topic) PublishToPartition(partition int, msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		return fmt.Errorf("partition %d out of range for topic '%s' (0-%d)", partition, t.Name, len(t.Partitions)-1)
	}

	t.Partitions[partition].Enqueue(msg)
	return nil
}

// PublishSync sends a message synchronously to one partition.
// Partition selection and enqueue happen under a single RLock to prevent
// TOCTOU races with AddPartitions.
func (t *Topic) PublishSync(msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx := t.getPartitionIndex(msg, len(t.Partitions))
	if idx == -1 {
		return fmt.Errorf("no partitions available for topic '%s'", t.Name)
	}

	return t.Partitions[idx].EnqueueSync(msg)
}

func (t *Topic) PublishToPartitionSync(partition int, msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		return fmt.Errorf("partition %d out of range for topic '%s' (0-%d)", partition, t.Name, len(t.Partitions)-1)
	}

	return t.Partitions[partition].EnqueueSync(msg)
}
func (t *Topic) PublishToPartitionSyncIdempotent(partition int, msg types.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		return fmt.Errorf("partition %d out of range for topic '%s' (0-%d)", partition, t.Name, len(t.Partitions)-1)
	}

	return t.Partitions[partition].EnqueueSyncIdempotent(msg)
}

// PublishBatchSync sends a batch of messages synchronously, grouping by partition.
// Partition selection and enqueue happen under a single RLock to prevent
// TOCTOU races with AddPartitions.
func (t *Topic) PublishBatchSync(msgs []types.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	partitionsLen := len(t.Partitions)
	partitioned := make(map[int][]types.Message)
	for _, msg := range msgs {
		idx := t.getPartitionIndex(msg, partitionsLen)
		if idx != -1 {
			partitioned[idx] = append(partitioned[idx], msg)
		}
	}

	for idx, pm := range partitioned {
		if err := t.Partitions[idx].EnqueueBatchSync(pm); err != nil {
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

func (t *Topic) NewMessageSignal(partition int) <-chan struct{} {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		util.Warn("NewMessageSignal called with invalid partition %d for topic '%s'", partition, t.Name)
		return nil
	}
	return t.Partitions[partition].newMessageCh
}
