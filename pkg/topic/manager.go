package topic

import (
	"fmt"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type StreamManager interface {
	AddStream(key string, streamConn *stream.StreamConnection, readFn func(offset uint64, max int) ([]types.Message, error), commitInterval time.Duration) error
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
	producerState map[string]map[int]map[string]int64 // Topic -> Partition -> ProducerID -> LastSeq
}

// HandlerProvider defines an interface to provide disk handlers.
type HandlerProvider interface {
	GetHandler(topic string, partitionID int) (types.StorageHandler, error)
}

func (tm *TopicManager) SetCoordinator(cd *coordinator.Coordinator) {
	tm.coordinator = cd
}

func NewTopicManager(cfg *config.Config, hp HandlerProvider, sm StreamManager) *TopicManager {
	tm := &TopicManager{
		topics:        make(map[string]*Topic),
		stopCh:        make(chan struct{}),
		hp:            hp,
		cfg:           cfg,
		StreamManager: sm,
		producerState: make(map[string]map[int]map[string]int64),
	}
	return tm
}

func (tm *TopicManager) CreateTopic(name string, partitionCount int, idempotent bool) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if existing, ok := tm.topics[name]; ok {
		current := len(existing.Partitions)
		switch {
		case partitionCount < current:
			util.Error("cannot decrease partitions for topic '%s' (%d -> %d)", name, current, partitionCount)
			return fmt.Errorf("cannot decrease partition count for topic '%s': %d -> %d", name, current, partitionCount)
		case partitionCount > current:
			existing.AddPartitions(partitionCount-current, tm.hp)
			util.Info("topic '%s' partitions increased: %d -> %d", name, current, len(existing.Partitions))
			return nil
		default:
			util.Info("topic '%s' already exists with %d partitions", name, current)
			return nil
		}
	}

	t, err := NewTopic(name, partitionCount, tm.hp, tm.cfg, tm.StreamManager, idempotent)
	if err != nil {
		util.Error("failed to create topic '%s': %v", name, err)
		return fmt.Errorf("failed to create topic '%s': %w", name, err)
	}
	tm.topics[name] = t
	return nil
}

func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topics[name]
}

func (tm *TopicManager) GetLastOffset(topicName string, partitionID int) uint64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	t := tm.GetTopic(topicName)
	if t == nil {
		return 0
	}

	if partitionID < 0 || partitionID >= len(t.Partitions) {
		return 0
	}

	p := t.Partitions[partitionID]
	return p.dh.GetAbsoluteOffset()
}

// Async (acks=0)
func (tm *TopicManager) Publish(topicName string, msg *types.Message) error {
	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	partition := t.GetPartitionForMessage(*msg)
	return tm.publishInternal(topicName, partition, msg, false)
}

// Sync (acks=1)
func (tm *TopicManager) PublishWithAck(topicName string, msg *types.Message) error {
	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	partition := t.GetPartitionForMessage(*msg)
	return tm.publishInternal(topicName, partition, msg, true)
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

		// Check for duplicate before routing; do NOT advance sequence state yet.
		if msg.ProducerID != "" && msg.SeqNum > 0 {
			tm.mu.Lock()
			isDup := tm.isDuplicateProducer(topicName, -1, &msg)
			tm.mu.Unlock()
			if isDup {
				util.Debug("tm: skipping duplicate message (Global) in batch from producer %s (seq %d)", msg.ProducerID, msg.SeqNum)
				continue
			}
		}

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

	// Advance sequence state only after all writes succeeded.
	tm.mu.Lock()
	for _, msgs := range partitioned {
		for i := range msgs {
			if msgs[i].ProducerID != "" && msgs[i].SeqNum > 0 {
				tm.markProducerSequence(topicName, -1, &msgs[i])
			}
		}
	}
	tm.mu.Unlock()

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
	t.mu.RUnlock()

	wg.Wait()
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

func (tm *TopicManager) publishInternal(topicName string, _ int, msg *types.Message, requireAck bool) error {
	util.Debug("Starting publish. Topic: %s, RequireAck: %v, ProducerID: %s, SeqNum: %d", topicName, requireAck, msg.ProducerID, msg.SeqNum)
	start := time.Now()

	t := tm.GetTopic(topicName)
	if t == nil {
		util.Warn("tm: topic '%s' does not exist", topicName)
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	// Check for duplicate before writing; do NOT advance sequence state yet.
	if msg.ProducerID != "" && msg.SeqNum > 0 {
		tm.mu.Lock()
		isDup := tm.isDuplicateProducer(topicName, -1, msg)
		tm.mu.Unlock()
		if isDup {
			util.Debug("tm: skipping duplicate message (Global) from producer %s (seq %d)", msg.ProducerID, msg.SeqNum)
			return nil
		}
	}

	if requireAck {
		if err := t.PublishSync(*msg); err != nil {
			return fmt.Errorf("sync publish failed: %w", err)
		}
	} else {
		t.Publish(*msg)
	}

	// Advance sequence state only after the write has been accepted.
	if msg.ProducerID != "" && msg.SeqNum > 0 {
		tm.mu.Lock()
		tm.markProducerSequence(topicName, -1, msg)
		tm.mu.Unlock()
	}

	elapsed := time.Since(start).Seconds()
	metrics.MessagesProcessed.Inc()
	metrics.LatencyHist.Observe(elapsed)
	return nil
}

// isDuplicateProducer checks whether msg is a duplicate based on producer sequence state.
// It does NOT mutate state; call markProducerSequence after a successful write.
func (tm *TopicManager) isDuplicateProducer(topicName string, partition int, msg *types.Message) bool {
	if tm.producerState[topicName] == nil {
		return false
	}
	if tm.producerState[topicName][partition] == nil {
		return false
	}
	lastSeq, ok := tm.producerState[topicName][partition][msg.ProducerID]
	return ok && int64(msg.SeqNum) <= lastSeq
}

// markProducerSequence records the producer's latest sequence number after a successful write.
func (tm *TopicManager) markProducerSequence(topicName string, partition int, msg *types.Message) {
	if tm.producerState[topicName] == nil {
		tm.producerState[topicName] = make(map[int]map[string]int64)
	}
	if tm.producerState[topicName][partition] == nil {
		tm.producerState[topicName][partition] = make(map[string]int64)
	}
	tm.producerState[topicName][partition][msg.ProducerID] = int64(msg.SeqNum)
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
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if _, ok := tm.topics[name]; ok {
		delete(tm.topics, name)
		delete(tm.producerState, name)
		return true
	}
	return false
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

func (tm *TopicManager) EnsureDefaultGroups() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	for name, t := range tm.topics {
		t.RegisterConsumerGroup("default-group", 1)
		util.Info("Registered default-group for topic '%s'", name)
	}
}

