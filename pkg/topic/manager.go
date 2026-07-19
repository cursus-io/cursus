package topic

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

var ErrTopicNotFound = errors.New("topic not found")

type StreamManager interface {
	AddStream(key string, streamConn *stream.StreamConnection, readFn func(offset uint64, max int) ([]types.Message, error), legacyCommitInterval time.Duration) error
	RemoveStream(key string)
	GetStreamsForPartition(topic string, partition int) []*stream.StreamConnection
	StopStream(key string)
}

type TopicManager struct {
	topics        map[string]*Topic
	stopCh        chan struct{}
	hp            HandlerProvider
	mu            sync.RWMutex
	cfg           *config.Config
	StreamManager StreamManager
	coordinator   *coordinator.Coordinator
	txnResolver   TransactionDecisionResolver
	metadataStore *topicMetadataStore

	metadataLoadFailure             string
	metadataOrphanTopicCount        int
	metadataDurabilityWarning       string
	metadataDurabilityWarningsTotal uint64
}

// HandlerProvider defines an interface to provide disk handlers.
type HandlerProvider interface {
	GetHandler(topic string, partitionID int) (types.StorageHandler, error)
}

type existingPartitionProvider interface {
	ExistingPartitionCount(topic string) (int, error)
}

type topicHandlerCloser interface {
	CloseTopicHandlers(topic string)
}

func (tm *TopicManager) SetCoordinator(cd *coordinator.Coordinator) {
	tm.coordinator = cd
}

func (tm *TopicManager) SetTransactionDecisionResolver(resolver TransactionDecisionResolver) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.txnResolver = resolver
	for _, t := range tm.topics {
		t.SetTransactionDecisionResolver(resolver)
	}
}

func NewTopicManager(cfg *config.Config, hp HandlerProvider, sm StreamManager) *TopicManager {
	tm := &TopicManager{
		topics:        make(map[string]*Topic),
		stopCh:        make(chan struct{}),
		hp:            hp,
		cfg:           cfg,
		StreamManager: sm,
		metadataStore: newTopicMetadataStore(cfg, hp),
	}
	return tm
}

func (tm *TopicManager) CreateTopic(name string, partitionCount int, idempotent bool, eventSourcing bool) error {
	policy := DefaultPolicy()
	if tm.cfg != nil && tm.cfg.CleanupPolicy != "" {
		policy.CleanupPolicy = tm.cfg.CleanupPolicy
	}
	return tm.CreateTopicWithPolicy(name, partitionCount, idempotent, eventSourcing, policy)
}

func (tm *TopicManager) CreateTopicWithPolicy(name string, partitionCount int, idempotent bool, eventSourcing bool, policy Policy) error {
	definition, err := (Definition{
		Name:          name,
		Partitions:    partitionCount,
		Idempotent:    idempotent,
		EventSourcing: eventSourcing,
		Policy:        policy,
	}).Normalize()
	if err != nil {
		return err
	}
	if err := validateCleanupPolicyForTopic(definition.Policy, tm.cfg, eventSourcing); err != nil {
		return err
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if existing, ok := tm.topics[name]; ok {
		current := existing.Definition()
		definition.Idempotent = current.Idempotent
		definition.EventSourcing = current.EventSourcing
		if err := validateCleanupPolicyForTopic(definition.Policy, tm.cfg, current.EventSourcing); err != nil {
			return err
		}
		if partitionCount < current.Partitions {
			util.Error("cannot decrease partitions for topic '%s' (%d -> %d)", name, current.Partitions, partitionCount)
			return fmt.Errorf("cannot decrease partition count for topic '%s': %d -> %d", name, current.Partitions, partitionCount)
		}
		if err := existing.applyDefinition(partitionCount, definition.Policy, tm.hp, tm.persistDefinitionLocked); err != nil {
			return fmt.Errorf("update topic '%s': %w", name, err)
		}
		if partitionCount > current.Partitions {
			util.Info("topic '%s' partitions increased: %d -> %d", name, current.Partitions, partitionCount)
		} else {
			util.Info("topic '%s' already exists with %d partitions", name, current.Partitions)
		}
		return nil
	}
	if err := tm.rejectOrphanedStorageLocked(name); err != nil {
		return err
	}

	created, err := NewTopicWithPolicy(
		name,
		partitionCount,
		tm.hp,
		tm.cfg,
		tm.StreamManager,
		idempotent,
		eventSourcing,
		definition.Policy,
	)
	if err != nil {
		util.Error("failed to create topic '%s': %v", name, err)
		return fmt.Errorf("failed to create topic '%s': %w", name, err)
	}
	created.SetTransactionDecisionResolver(tm.txnResolver)
	if err := tm.persistDefinitionLocked(definition); err != nil {
		closePartiallyInitializedTopic(name, tm.hp, created.Partitions)
		return err
	}
	tm.topics[name] = created
	return nil
}
func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topics[name]
}

// ExistingPartitionCount reports the partition count already known in memory or
// persisted by the storage provider without creating new partitions.
func (tm *TopicManager) ExistingPartitionCount(name string) (int, error) {
	tm.mu.RLock()
	if existing := tm.topics[name]; existing != nil {
		count := len(existing.Partitions)
		tm.mu.RUnlock()
		return count, nil
	}
	provider := tm.hp
	tm.mu.RUnlock()
	if discovery, ok := provider.(existingPartitionProvider); ok {
		return discovery.ExistingPartitionCount(name)
	}
	return 0, nil
}

func (tm *TopicManager) GetLastOffset(topicName string, partitionID int) uint64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	t := tm.topics[topicName]
	if t == nil {
		return 0
	}

	if partitionID < 0 || partitionID >= len(t.Partitions) {
		return 0
	}

	p := t.Partitions[partitionID]
	return p.dh.GetAbsoluteOffset()
}

func (tm *TopicManager) ReadTopicPartition(topicName string, partitionID int, offset uint64, max int) ([]types.Message, error) {
	t := tm.GetTopic(topicName)
	if t == nil {
		return nil, fmt.Errorf("topic '%s' does not exist", topicName)
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		return nil, err
	}
	return p.dh.ReadMessages(offset, max)
}

// Async (acks=0)
func (tm *TopicManager) Publish(topicName string, msg *types.Message) error {
	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	partition := t.GetPartitionForMessage(*msg)
	return tm.publishInternal(topicName, partition, msg, false, false)
}

func (tm *TopicManager) PublishToPartition(topicName string, partition int, msg *types.Message) error {
	return tm.publishInternal(topicName, partition, msg, false, false)
}

// Sync (acks=1)
func (tm *TopicManager) PublishWithAck(topicName string, msg *types.Message) error {
	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	partition := t.GetPartitionForMessage(*msg)
	return tm.publishInternal(topicName, partition, msg, true, false)
}

func (tm *TopicManager) PublishToPartitionWithAck(topicName string, partition int, msg *types.Message) error {
	return tm.publishInternal(topicName, partition, msg, true, false)
}
func (tm *TopicManager) PublishToPartitionWithAckIdempotent(topicName string, partition int, msg *types.Message) error {
	return tm.publishInternal(topicName, partition, msg, true, true)
}

func (tm *TopicManager) processBatchMessages(topicName string, messages []types.Message, async bool) error {
	if len(messages) == 0 {
		return nil
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	var partition int
	partitioned := make(map[int][]types.Message)
	skippedCount := 0

	for _, m := range messages {
		msg := m

		partition = t.GetPartitionForMessage(msg)
		if partition == -1 {
			util.Debug("tm: skipping message for topic '%s' (no valid partition found)", topicName)
			skippedCount++
			continue
		}

		partitioned[partition] = append(partitioned[partition], msg)
	}

	if len(partitioned) == 0 && skippedCount > 0 {
		return fmt.Errorf("failed to route any messages in batch for topic '%s' (all partitions returned -1)", topicName)
	}

	if err := tm.executeBatch(t, partitioned, async); err != nil {
		return err
	}

	return nil
}

func (tm *TopicManager) executeBatch(t *Topic, partitioned map[int][]types.Message, async bool) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(partitioned))

	t.mu.RLock()
	for idx, msgs := range partitioned {
		if idx < 0 || idx >= len(t.Partitions) {
			util.Error("tm: invalid partition index %d in batch", idx)
			continue
		}

		wg.Add(1)
		targetPartition := t.Partitions[idx]
		batch := msgs

		go func(p *Partition, msgs []types.Message) {
			defer wg.Done()
			var err error
			if async {
				err = p.EnqueueBatch(msgs)
			} else {
				err = p.EnqueueBatchSync(msgs)
			}
			if err != nil {
				util.Error("tm: execute batch partition %d Error: %v", p.id, err)
				errCh <- fmt.Errorf("partition %d: %w", p.id, err)
			}
		}(targetPartition, batch)
	}

	wg.Wait()
	t.mu.RUnlock()
	close(errCh)

	var lastErr error
	for err := range errCh {
		lastErr = err
		util.Error("Batch error: %v", err)
	}
	return lastErr
}

// Batch Sync (acks=1)
func (tm *TopicManager) PublishBatchSync(topicName string, messages []types.Message) error {
	return tm.processBatchMessages(topicName, messages, false)
}

// Batch Async (acks=0)
func (tm *TopicManager) PublishBatchAsync(topicName string, messages []types.Message) error {
	return tm.processBatchMessages(topicName, messages, true)
}

func (tm *TopicManager) publishInternal(topicName string, partition int, msg *types.Message, requireAck bool, forceIdempotent bool) error {
	util.Debug("Starting publish. Topic: %s, RequireAck: %v, ForceIdempotent: %v, ProducerID: %s, SeqNum: %d", topicName, requireAck, forceIdempotent, msg.ProducerID, msg.SeqNum)
	start := time.Now()

	t := tm.GetTopic(topicName)
	if t == nil {
		util.Warn("tm: topic '%s' does not exist", topicName)
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	if forceIdempotent && !requireAck {
		return fmt.Errorf("forced idempotent publish requires acknowledgements")
	}

	if requireAck {
		if forceIdempotent {
			if err := t.PublishToPartitionSyncIdempotent(partition, *msg); err != nil {
				return fmt.Errorf("sync idempotent publish failed: %w", err)
			}
		} else if err := t.PublishToPartitionSync(partition, *msg); err != nil {
			return fmt.Errorf("sync publish failed: %w", err)
		}
	} else {
		if err := t.PublishToPartition(partition, *msg); err != nil {
			return fmt.Errorf("async publish failed: %w", err)
		}
	}

	elapsed := time.Since(start).Seconds()
	metrics.MessagesProcessed.Inc()
	metrics.LatencyHist.Observe(elapsed)
	return nil
}

func (tm *TopicManager) RegisterConsumerGroup(topicName, groupName string, consumerCount int) (*types.ConsumerGroup, error) {
	t := tm.GetTopic(topicName)
	if t == nil {
		util.Warn("tm: topic '%s' does not exist", topicName)
		return nil, fmt.Errorf("topic '%s' does not exist", topicName)
	}

	group := t.RegisterConsumerGroup(groupName, consumerCount)

	if tm.coordinator != nil {
		if err := tm.coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); err != nil {
			if deregErr := t.DeregisterConsumerGroup(groupName); deregErr != nil {
				util.Error("Failed to rollback consumer group '%s' on topic '%s': %v", groupName, topicName, deregErr)
			}
			return nil, fmt.Errorf("failed to register group '%s' with coordinator: %w", groupName, err)
		}

		assignments := tm.coordinator.GetAssignments(groupName)
		t.applyAssignments(groupName, assignments)
	}

	return group, nil
}

func (tm *TopicManager) Flush() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, t := range tm.topics {
		for _, p := range t.Partitions {
			if p.dh != nil {
				if f, ok := p.dh.(interface{ Flush() }); ok {
					f.Flush()
				}
			}
		}
	}
}

func (tm *TopicManager) Stop() {
	close(tm.stopCh)
}

func (tm *TopicManager) DeleteTopic(name string) bool {
	deleted, err := tm.DeleteTopicDurable(name)
	if err != nil {
		util.Warn("Failed to durably delete topic %s: %v", name, err)
		return false
	}
	return deleted
}

// DeleteTopicDurable commits the metadata deletion before removing local data.
func (tm *TopicManager) DeleteTopicDurable(name string) (bool, error) {
	if err := ValidateName(name); err != nil {
		return false, fmt.Errorf("invalid topic name: %w", err)
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	current := tm.topics[name]
	if current == nil {
		return false, nil
	}
	if err := tm.persistRemovalLocked(name); err != nil {
		return false, err
	}

	delete(tm.topics, name)
	for _, partition := range current.Partitions {
		partition.Close()
	}
	if closer, ok := tm.hp.(topicHandlerCloser); ok {
		closer.CloseTopicHandlers(name)
	} else {
		for _, partition := range current.Partitions {
			if err := partition.dh.Close(); err != nil {
				util.Warn("Failed to close storage handler for deleted topic %s[%d]: %v", name, partition.ID(), err)
			}
		}
	}
	if err := tm.deleteTopicLogDirLocked(name); err != nil {
		// The durable definition removal is already committed. Keep the command
		// result aligned with the logical state and leave physical cleanup for an
		// operator or a later retry instead of reporting a false rollback.
		util.Warn("Failed to clean log directory for deleted topic %s: %v", name, err)
	}
	return true, nil
}
func (tm *TopicManager) deleteTopicLogDirLocked(name string) error {
	if tm.cfg == nil || strings.TrimSpace(tm.cfg.LogDir) == "" {
		return nil
	}

	root, err := filepath.Abs(tm.cfg.LogDir)
	if err != nil {
		return err
	}
	target, err := filepath.Abs(filepath.Join(tm.cfg.LogDir, name))
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return err
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("refusing to delete topic path outside log root: %s", target)
	}
	return os.RemoveAll(target)
}

func (tm *TopicManager) ListTopics() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	return names
}

func (tm *TopicManager) GetLogDir(topicName string, partitionID int) string {
	return filepath.Join(tm.cfg.LogDir, topicName, fmt.Sprintf("partition_%d", partitionID))
}

func (tm *TopicManager) EnsureDefaultGroups() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	for name, t := range tm.topics {
		t.RegisterConsumerGroup("default-group", 1)
		util.Info("Registered default-group for topic '%s'", name)
	}
}
