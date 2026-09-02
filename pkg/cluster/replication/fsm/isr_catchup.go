package fsm

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/types"
)

const MaxReplicaCatchupRecords = 1024

// ReplicaCatchupRequest asks the current partition leader for a bounded raw
// committed-log range. LeaderAddress is local routing metadata and is not sent.
type ReplicaCatchupRequest struct {
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	BrokerID       string `json:"broker_id"`
	NextOffset     uint64 `json:"next_offset"`
	CommittedHWM   uint64 `json:"committed_hwm"`
	Leader         string `json:"leader"`
	LeaderEpoch    int    `json:"leader_epoch"`
	LifecycleEpoch uint64 `json:"lifecycle_epoch"`
	MaxRecords     int    `json:"max_records"`
	LeaderAddress  string `json:"-"`
}

// ReplicaCatchupBatch carries an exact, contiguous committed-log range under
// the same partition fences as the request.
type ReplicaCatchupBatch struct {
	Topic          string          `json:"topic"`
	Partition      int             `json:"partition"`
	BrokerID       string          `json:"broker_id"`
	StartOffset    uint64          `json:"start_offset"`
	CommittedHWM   uint64          `json:"committed_hwm"`
	Leader         string          `json:"leader"`
	LeaderEpoch    int             `json:"leader_epoch"`
	LifecycleEpoch uint64          `json:"lifecycle_epoch"`
	Messages       []types.Message `json:"messages"`
}

// ISRCatchupProof fences ISR re-admission with the authoritative partition
// boundary and the current leader and topic lifecycle generations.
type ISRCatchupProof struct {
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	BrokerID       string `json:"broker_id"`
	CommittedHWM   uint64 `json:"committed_hwm"`
	LocalLEO       uint64 `json:"local_leo"`
	LocalHWM       uint64 `json:"local_hwm"`
	LeaderEpoch    int    `json:"leader_epoch"`
	LifecycleEpoch uint64 `json:"lifecycle_epoch"`
}

// ValidateISRCatchupProof validates a proof against the current FSM view. The
// boolean result reports whether a Raft transition is still required.
func (f *BrokerFSM) ValidateISRCatchupProof(proof ISRCatchupProof) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.validateISRCatchupProofLocked(proof)
}

func (f *BrokerFSM) validateISRCatchupProofLocked(proof ISRCatchupProof) (bool, error) {
	if proof.Topic == "" || proof.Partition < 0 || proof.BrokerID == "" {
		return false, fmt.Errorf("invalid ISR catch-up proof identity")
	}
	key := proof.Topic + "-" + strconv.Itoa(proof.Partition)
	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return false, fmt.Errorf("partition metadata %s not found", key)
	}
	if !containsString(metadata.Replicas, proof.BrokerID) {
		return false, fmt.Errorf("broker %s is not a configured replica for %s", proof.BrokerID, key)
	}
	if !metadata.CommittedHWMKnown {
		return false, fmt.Errorf("%w: partition %s has no authoritative committed HWM", ErrUnsupportedRecoveryProtocol, key)
	}
	if proof.CommittedHWM != metadata.CommittedHWM {
		return false, fmt.Errorf("committed HWM mismatch for %s: current=%d proof=%d", key, metadata.CommittedHWM, proof.CommittedHWM)
	}
	if proof.LocalLEO != metadata.CommittedHWM || proof.LocalHWM != metadata.CommittedHWM {
		return false, fmt.Errorf(
			"replica %s is not synchronized for %s: leo=%d hwm=%d committed_hwm=%d",
			proof.BrokerID, key, proof.LocalLEO, proof.LocalHWM, metadata.CommittedHWM,
		)
	}
	if proof.LeaderEpoch != metadata.LeaderEpoch {
		return false, fmt.Errorf("stale leader epoch for %s: current=%d proof=%d", key, metadata.LeaderEpoch, proof.LeaderEpoch)
	}
	if proof.LifecycleEpoch != metadata.LifecycleEpoch {
		return false, fmt.Errorf("stale topic lifecycle epoch for %s: current=%d proof=%d", key, metadata.LifecycleEpoch, proof.LifecycleEpoch)
	}
	definition := f.topicState[proof.Topic]
	if definition == nil {
		return false, fmt.Errorf("topic definition %s not found", proof.Topic)
	}
	if definition.LifecycleEpoch != metadata.LifecycleEpoch {
		return false, fmt.Errorf(
			"partition lifecycle epoch conflicts with topic %s: topic=%d partition=%d",
			proof.Topic, definition.LifecycleEpoch, metadata.LifecycleEpoch,
		)
	}
	if !containsString(metadata.Replicas, metadata.Leader) {
		return false, fmt.Errorf("partition leader %s is not a configured replica for %s", metadata.Leader, key)
	}
	if containsString(metadata.ISR, proof.BrokerID) {
		return false, nil
	}
	return true, nil
}

func (f *BrokerFSM) applyISRCatchupCommand(jsonData string) interface{} {
	var proof ISRCatchupProof
	if err := decodeStrictJSON([]byte(jsonData), &proof); err != nil {
		return fmt.Errorf("decode ISR catch-up proof: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	required, err := f.validateISRCatchupProofLocked(proof)
	if err != nil || !required {
		return err
	}

	key := proof.Topic + "-" + strconv.Itoa(proof.Partition)
	metadata := f.partitionMetadata[key]
	members := make(map[string]struct{}, len(metadata.ISR)+1)
	for _, brokerID := range metadata.ISR {
		if !containsString(metadata.Replicas, brokerID) {
			return fmt.Errorf("ISR broker %s is not a configured replica for %s", brokerID, key)
		}
		members[brokerID] = struct{}{}
	}
	members[proof.BrokerID] = struct{}{}

	ordered := make([]string, 0, len(members))
	for _, brokerID := range metadata.Replicas {
		if _, ok := members[brokerID]; ok {
			ordered = append(ordered, brokerID)
		}
	}
	metadata.ISR = ordered
	return nil
}

// BuildISRCatchupProofs returns proofs only for local, synchronized replicas
// that are currently outside ISR under the same topic lifecycle generation.
func (f *BrokerFSM) BuildISRCatchupProofs(brokerID string) []ISRCatchupProof {
	if brokerID == "" {
		return nil
	}

	f.mu.RLock()
	metadata := make(map[string]PartitionMetadata, len(f.partitionMetadata))
	for key, value := range f.partitionMetadata {
		if value != nil {
			copy := *value
			copy.Replicas = append([]string(nil), value.Replicas...)
			copy.ISR = append([]string(nil), value.ISR...)
			metadata[key] = copy
		}
	}
	definitions := copyTopicState(f.topicState)
	topicManager := f.tm
	f.mu.RUnlock()
	if topicManager == nil {
		return nil
	}

	proofs := make([]ISRCatchupProof, 0)
	for key, partitionMetadata := range metadata {
		if !partitionMetadata.CommittedHWMKnown ||
			!containsString(partitionMetadata.Replicas, brokerID) ||
			containsString(partitionMetadata.ISR, brokerID) {
			continue
		}
		separator := strings.LastIndex(key, "-")
		if separator <= 0 {
			continue
		}
		partitionID, err := strconv.Atoi(key[separator+1:])
		if err != nil {
			continue
		}
		topicName := key[:separator]
		definition := definitions[topicName]
		localTopic := topicManager.GetTopic(topicName)
		if definition == nil || localTopic == nil ||
			definition.LifecycleEpoch != partitionMetadata.LifecycleEpoch ||
			localTopic.Definition().LifecycleEpoch != partitionMetadata.LifecycleEpoch {
			continue
		}
		partition, err := localTopic.GetPartition(partitionID)
		if err != nil {
			continue
		}
		leo, hwm := partition.NextOffset(), partition.GetHWM()
		if leo != partitionMetadata.CommittedHWM || hwm != partitionMetadata.CommittedHWM {
			continue
		}
		proofs = append(proofs, ISRCatchupProof{
			Topic: topicName, Partition: partitionID, BrokerID: brokerID,
			CommittedHWM: partitionMetadata.CommittedHWM, LocalLEO: leo, LocalHWM: hwm,
			LeaderEpoch: partitionMetadata.LeaderEpoch, LifecycleEpoch: partitionMetadata.LifecycleEpoch,
		})
	}
	sort.Slice(proofs, func(i, j int) bool {
		if proofs[i].Topic != proofs[j].Topic {
			return proofs[i].Topic < proofs[j].Topic
		}
		return proofs[i].Partition < proofs[j].Partition
	})
	return proofs
}

// BuildReplicaCatchupRequests returns one bounded-range request for each local
// follower below the authoritative committed HWM. A lagging ISR member is also
// repaired while its eviction is still propagating through Raft.
func (f *BrokerFSM) BuildReplicaCatchupRequests(brokerID string) []ReplicaCatchupRequest {
	if brokerID == "" {
		return nil
	}
	f.mu.RLock()
	metadata := make(map[string]PartitionMetadata, len(f.partitionMetadata))
	for key, value := range f.partitionMetadata {
		if value != nil {
			copy := *value
			copy.Replicas = append([]string(nil), value.Replicas...)
			copy.ISR = append([]string(nil), value.ISR...)
			metadata[key] = copy
		}
	}
	definitions := copyTopicState(f.topicState)
	topicManager := f.tm
	brokers := make(map[string]BrokerInfo, len(f.brokers))
	for id, broker := range f.brokers {
		if broker != nil {
			brokers[id] = *broker
		}
	}
	f.mu.RUnlock()
	if topicManager == nil {
		return nil
	}

	requests := make([]ReplicaCatchupRequest, 0)
	for key, meta := range metadata {
		if !meta.CommittedHWMKnown || !containsString(meta.Replicas, brokerID) || meta.Leader == brokerID {
			continue
		}
		separator := strings.LastIndex(key, "-")
		if separator <= 0 {
			continue
		}
		partitionID, err := strconv.Atoi(key[separator+1:])
		if err != nil {
			continue
		}
		topicName := key[:separator]
		definition := definitions[topicName]
		localTopic := topicManager.GetTopic(topicName)
		leader, leaderKnown := brokers[meta.Leader]
		if definition == nil || localTopic == nil || !leaderKnown || leader.Addr == "" ||
			definition.LifecycleEpoch != meta.LifecycleEpoch || localTopic.Definition().LifecycleEpoch != meta.LifecycleEpoch {
			continue
		}
		partition, err := localTopic.GetPartition(partitionID)
		if err != nil {
			continue
		}
		leo := partition.NextOffset()
		if leo >= meta.CommittedHWM {
			if leo == meta.CommittedHWM || !containsString(meta.ISR, brokerID) {
				if err := partition.ReconcileCommittedHWM(meta.CommittedHWM); err == nil {
					partition.FlushDisk()
				}
			}
			continue
		}
		requests = append(requests, ReplicaCatchupRequest{
			Topic: topicName, Partition: partitionID, BrokerID: brokerID,
			NextOffset: leo, CommittedHWM: meta.CommittedHWM,
			Leader: meta.Leader, LeaderEpoch: meta.LeaderEpoch, LifecycleEpoch: meta.LifecycleEpoch,
			MaxRecords: MaxReplicaCatchupRecords, LeaderAddress: leader.Addr,
		})
	}
	sort.Slice(requests, func(i, j int) bool {
		if requests[i].Topic != requests[j].Topic {
			return requests[i].Topic < requests[j].Topic
		}
		return requests[i].Partition < requests[j].Partition
	})
	return requests
}

// FetchReplicaCatchup validates the request against current Raft metadata and
// returns only raw records below the authoritative committed HWM.
func (f *BrokerFSM) FetchReplicaCatchup(request ReplicaCatchupRequest) (ReplicaCatchupBatch, error) {
	if request.Topic == "" || request.Partition < 0 || request.BrokerID == "" || request.Leader == "" {
		return ReplicaCatchupBatch{}, fmt.Errorf("invalid replica catch-up identity")
	}
	if request.MaxRecords <= 0 || request.MaxRecords > MaxReplicaCatchupRecords {
		return ReplicaCatchupBatch{}, fmt.Errorf("invalid replica catch-up limit %d", request.MaxRecords)
	}
	key := request.Topic + "-" + strconv.Itoa(request.Partition)
	f.mu.RLock()
	meta := f.partitionMetadata[key]
	if meta == nil {
		f.mu.RUnlock()
		return ReplicaCatchupBatch{}, fmt.Errorf("partition metadata %s not found", key)
	}
	current := *meta
	current.Replicas = append([]string(nil), meta.Replicas...)
	f.mu.RUnlock()
	if !containsString(current.Replicas, request.BrokerID) {
		return ReplicaCatchupBatch{}, fmt.Errorf("broker %s is not a configured replica for %s", request.BrokerID, key)
	}
	if !current.CommittedHWMKnown {
		return ReplicaCatchupBatch{}, fmt.Errorf("%w: partition %s has no authoritative committed HWM", ErrUnsupportedRecoveryProtocol, key)
	}
	if request.Leader != current.Leader || request.LeaderEpoch != current.LeaderEpoch {
		return ReplicaCatchupBatch{}, fmt.Errorf("stale leader fence for %s", key)
	}
	if request.LifecycleEpoch != current.LifecycleEpoch {
		return ReplicaCatchupBatch{}, fmt.Errorf("stale topic lifecycle epoch for %s", key)
	}
	if request.CommittedHWM != current.CommittedHWM {
		return ReplicaCatchupBatch{}, fmt.Errorf("stale committed HWM for %s: current=%d requested=%d", key, current.CommittedHWM, request.CommittedHWM)
	}
	if request.NextOffset > current.CommittedHWM {
		return ReplicaCatchupBatch{}, fmt.Errorf("catch-up offset %d exceeds committed HWM %d", request.NextOffset, current.CommittedHWM)
	}
	messages, err := f.ReadCommittedLogRange(request.Topic, request.Partition, request.NextOffset, current.CommittedHWM, request.MaxRecords)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	if request.NextOffset < current.CommittedHWM && len(messages) == 0 {
		return ReplicaCatchupBatch{}, fmt.Errorf("committed catch-up range at %d is unavailable", request.NextOffset)
	}
	return ReplicaCatchupBatch{
		Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
		StartOffset: request.NextOffset, CommittedHWM: current.CommittedHWM,
		Leader: current.Leader, LeaderEpoch: current.LeaderEpoch, LifecycleEpoch: current.LifecycleEpoch,
		Messages: messages,
	}, nil
}

// ReadCommittedLogRange returns raw log records for replica catch-up. Unlike a
// consumer read, this includes transaction markers and aborted records because
// followers need an exact copy of every durable offset.
func (f *BrokerFSM) ReadCommittedLogRange(topicName string, partitionID int, offset, committedHWM uint64, max int) ([]types.Message, error) {
	if max <= 0 || offset >= committedHWM {
		return nil, nil
	}
	f.mu.RLock()
	topicManager := f.tm
	f.mu.RUnlock()
	if topicManager == nil {
		return nil, fmt.Errorf("topic manager is not available")
	}
	localTopic := topicManager.GetTopic(topicName)
	if localTopic == nil {
		return nil, fmt.Errorf("topic %s is not materialized", topicName)
	}
	partition, err := localTopic.GetPartition(partitionID)
	if err != nil {
		return nil, err
	}
	if localHWM := partition.GetHWM(); localHWM < committedHWM {
		return nil, fmt.Errorf("local committed HWM %d is behind catch-up boundary %d", localHWM, committedHWM)
	}
	messages, err := partition.ReadMessages(offset, max)
	if err != nil {
		return nil, err
	}
	result := make([]types.Message, 0, len(messages))
	next := offset
	for _, message := range messages {
		if message.Offset >= committedHWM {
			break
		}
		if message.Offset != next {
			return nil, fmt.Errorf("non-contiguous local replica log: expected %d, got %d", next, message.Offset)
		}
		result = append(result, message)
		next++
	}
	return result, nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
