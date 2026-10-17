package fsm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const MaxReplicaCatchupRecords = 1024

var errReplicaEpochBoundaryUnavailable = errors.New("replica epoch boundary unavailable")

// ReplicaCatchupRequest asks a current in-sync replica for a bounded raw
// committed-log range. CommittedHWM is the follower's observed lower bound;
// the source may return a newer committed boundary. SourceAddress is local
// routing metadata and is not sent.
type ReplicaCatchupRequest struct {
	LocalWitness         *topic.ReplicaRequestWitness `json:"-"`
	Topic                string                       `json:"topic"`
	Partition            int                          `json:"partition"`
	BrokerID             string                       `json:"broker_id"`
	NextOffset           uint64                       `json:"next_offset"`
	CommittedHWM         uint64                       `json:"committed_hwm"`
	Leader               string                       `json:"leader"`
	SourceBroker         string                       `json:"source_broker,omitempty"`
	LeaderEpoch          int                          `json:"leader_epoch"`
	LifecycleEpoch       uint64                       `json:"lifecycle_epoch"`
	MaxRecords           int                          `json:"max_records"`
	PreviousLeaderEpoch  int64                        `json:"previous_leader_epoch,omitempty"`
	PreviousRecordDigest string                       `json:"previous_record_digest,omitempty"`
	SourceAddress        string                       `json:"-"`
	SnapshotCatchup      bool                         `json:"-"`
	RecoveryProtocol     int                          `json:"recovery_protocol,omitempty"`
	PrefixCandidates     []disk.LineagePrefix         `json:"prefix_candidates,omitempty"`
	RetainedPrefix       *disk.RetainedPrefix         `json:"retained_prefix,omitempty"`
}

// ReplicaCatchupBatch carries a committed logical range under the same
// partition fences as the request. Compacted batches may omit superseded
// physical records while EndOffset still advances the logical replica tail.
type ReplicaCatchupBatch struct {
	Topic             string              `json:"topic"`
	Partition         int                 `json:"partition"`
	BrokerID          string              `json:"broker_id"`
	StartOffset       uint64              `json:"start_offset"`
	EndOffset         uint64              `json:"end_offset,omitempty"`
	CommittedHWM      uint64              `json:"committed_hwm"`
	Leader            string              `json:"leader"`
	SourceBroker      string              `json:"source_broker,omitempty"`
	LeaderEpoch       int                 `json:"leader_epoch"`
	LifecycleEpoch    uint64              `json:"lifecycle_epoch"`
	Compacted         bool                `json:"compacted,omitempty"`
	Verified          bool                `json:"verified,omitempty"`
	TruncateTo        *uint64             `json:"truncate_to,omitempty"`
	Messages          []types.Message     `json:"messages"`
	Digest            string              `json:"digest"`
	RecoveryProtocol  int                 `json:"recovery_protocol,omitempty"`
	MatchedPrefix     *disk.LineagePrefix `json:"matched_prefix,omitempty"`
	BootstrapRequired bool                `json:"bootstrap_required,omitempty"`
	// RetainedPrefix echoes the exact challenge even when bootstrap is required.
	RetainedPrefix        *disk.RetainedPrefix `json:"retained_prefix,omitempty"`
	MatchedRetainedPrefix *disk.RetainedPrefix `json:"matched_retained_prefix,omitempty"`
}

// SealReplicaCatchupBatch binds the logical range, fences, source, and decoded
// records to a SHA-256 digest before the batch crosses the cluster transport.
func SealReplicaCatchupBatch(batch ReplicaCatchupBatch) (ReplicaCatchupBatch, error) {
	digest, err := replicaCatchupBatchDigest(batch)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	batch.Digest = digest
	return batch, nil
}

// ValidateReplicaCatchupBatchDigest rejects missing or changed recovery data.
func ValidateReplicaCatchupBatchDigest(batch ReplicaCatchupBatch) error {
	if batch.Digest == "" {
		return fmt.Errorf("replica catch-up checksum is missing")
	}
	digest, err := replicaCatchupBatchDigest(batch)
	if err != nil {
		return err
	}
	if digest != batch.Digest {
		return fmt.Errorf("replica catch-up checksum mismatch")
	}
	return nil
}

// AdvanceReplicaCatchupRequest moves the request boundary and its prefix proof
// together after a batch has been validated and durably applied.
func AdvanceReplicaCatchupRequest(request *ReplicaCatchupRequest, batch ReplicaCatchupBatch) error {
	if request == nil {
		return fmt.Errorf("replica catch-up request is nil")
	}
	endOffset := batch.EndOffset
	if endOffset == 0 && len(batch.Messages) > 0 {
		endOffset = batch.Messages[len(batch.Messages)-1].Offset + 1
	}
	if endOffset == request.NextOffset && batch.Verified && endOffset == request.CommittedHWM {
		return nil
	}
	if endOffset <= request.NextOffset {
		return fmt.Errorf("replica catch-up made no progress at offset %d", request.NextOffset)
	}
	request.LocalWitness = nil
	request.NextOffset = endOffset
	// Cumulative proofs must be regenerated from durable storage after applying.
	request.PrefixCandidates = nil
	request.RetainedPrefix = nil
	request.PreviousLeaderEpoch = 0
	request.PreviousRecordDigest = ""
	if request.RecoveryProtocol == BootstrapRecoveryProtocolVersion {
		return nil
	}
	if len(batch.Messages) == 0 {
		return nil
	}
	previous := batch.Messages[len(batch.Messages)-1]
	if previous.Offset+1 != endOffset {
		return nil
	}
	request.PreviousLeaderEpoch = previous.LeaderEpoch
	digest, err := replicaRecordDigest(previous)
	if err != nil {
		return fmt.Errorf("encode replica catch-up prefix proof: %w", err)
	}
	request.PreviousRecordDigest = digest
	return nil
}

func replicaCatchupBatchDigest(batch ReplicaCatchupBatch) (string, error) {
	batch.Digest = ""
	data, err := json.Marshal(batch)
	if err != nil {
		return "", fmt.Errorf("encode replica catch-up checksum payload: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
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
	metadata.RecoveryReplicas = removeString(metadata.RecoveryReplicas, proof.BrokerID)
	return nil
}

// BuildISRCatchupProofs returns proofs only for local, synchronized replicas
// that are currently outside ISR under the same topic lifecycle generation.
func (f *BrokerFSM) BuildISRCatchupProofs(brokerID string) []ISRCatchupProof {
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()
	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()
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
			copy.RecoveryReplicas = append([]string(nil), value.RecoveryReplicas...)
			metadata[key] = copy
		}
	}
	definitions := copyTopicState(f.topicState)
	topicManager := f.tm
	consumerCoordinator := f.cd
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
		// Keep a repaired offsets log eligible for recovery verification until
		// full overlay replay discharges its requirement. Joining ISR first can
		// stale the exact witness on a subsequent append without a recovery path.
		if topicName == config.ConsumerOffsetsTopicName && consumerCoordinator.PendingConsumerLogRepair(partitionID) != 0 {
			continue
		}
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
		if !partition.ReplicaCatchupVerified(partitionMetadata.LeaderEpoch, partitionMetadata.LifecycleEpoch, partitionMetadata.CommittedHWM) {
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

// MarkReplicaCatchupVerified records a completed, leader-fenced prefix check.
// A later ISR proof must still match the current metadata tuple.
func (f *BrokerFSM) MarkReplicaCatchupVerified(request ReplicaCatchupRequest, witnesses ...*topic.ReplicaCatchupWitness) error {
	if len(witnesses) != 1 || witnesses[0] == nil {
		return fmt.Errorf("actual receiver verification witness is required")
	}
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()
	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()
	key := request.Topic + "-" + strconv.Itoa(request.Partition)
	f.mu.RLock()
	meta := f.partitionMetadata[key]
	topicManager := f.tm
	if meta == nil || meta.Leader != request.Leader || meta.LeaderEpoch != request.LeaderEpoch ||
		meta.LifecycleEpoch != request.LifecycleEpoch || meta.CommittedHWM != request.CommittedHWM {
		f.mu.RUnlock()
		return fmt.Errorf("replica catch-up fence changed for %s", key)
	}
	f.mu.RUnlock()
	if topicManager == nil {
		return fmt.Errorf("topic manager is not available")
	}
	localTopic := topicManager.GetTopic(request.Topic)
	if localTopic == nil || localTopic.Definition().LifecycleEpoch != request.LifecycleEpoch {
		return fmt.Errorf("local topic lifecycle changed for %s", key)
	}
	partition, err := localTopic.GetPartition(request.Partition)
	if err != nil {
		return err
	}
	if partition.NextOffset() != request.CommittedHWM || partition.GetHWM() != request.CommittedHWM {
		return fmt.Errorf("replica catch-up boundary changed for %s", key)
	}
	return partition.AcceptReplicaCatchupWitness(witnesses[0], request.Leader, request.LeaderEpoch, request.LifecycleEpoch, request.CommittedHWM)
}

// BuildReplicaCatchupRequests returns one bounded-range request for each local
// replica below the authoritative committed HWM. The transfer source is a
// remote active ISR member and is independent from metadata leadership, so a
// restarted lagging leader can recover without a forced leader election.
func (f *BrokerFSM) BuildReplicaCatchupRequests(brokerID string) []ReplicaCatchupRequest {
	return f.BuildReplicaCatchupRequestsExcluding(brokerID, nil)
}

// Exclusions run before any tail/HWM mutation. A pending durable install always
// wins, even when no runtime bootstrap receiver is configured yet.
func (f *BrokerFSM) BuildReplicaCatchupRequestsExcluding(brokerID string, excluded func(string, int) bool) []ReplicaCatchupRequest {
	var requests []ReplicaCatchupRequest
	for _, key := range f.ReplicaCatchupCandidates(brokerID) {
		if excluded != nil && excluded(key.Topic, key.Partition) {
			continue
		}
		request, ok, err := f.BuildReplicaCatchupRequestContext(context.Background(), brokerID, key.Topic, key.Partition)
		if err == nil && ok {
			requests = append(requests, request)
		}
	}
	return requests
}

func addPreviousReplicaRecord(partition *topic.Partition, request *ReplicaCatchupRequest) error {
	if request.NextOffset == 0 || request.NextOffset <= partition.FirstOffset() {
		return nil
	}
	messages, err := partition.ReadMessages(request.NextOffset-1, 1)
	if err != nil || len(messages) != 1 || messages[0].Offset != request.NextOffset-1 {
		return fmt.Errorf("read previous replica record at %d", request.NextOffset-1)
	}
	request.PreviousLeaderEpoch = messages[0].LeaderEpoch
	request.PreviousRecordDigest, err = replicaRecordDigest(messages[0])
	return err
}

// ValidateReplicaRequestChallenge rechecks the immutable legacy challenge on
// the original receiver. Mode zero remains a last-record proof, not a full-prefix
// equivalence proof; the local witness separately excludes concurrent rewrites.
func ValidateReplicaRequestChallenge(partition *topic.Partition, request ReplicaCatchupRequest) error {
	if request.RecoveryProtocol != 0 {
		return nil
	}
	actual := request
	actual.PreviousLeaderEpoch, actual.PreviousRecordDigest = 0, ""
	if err := addPreviousReplicaRecord(partition, &actual); err != nil {
		return err
	}
	if actual.PreviousLeaderEpoch != request.PreviousLeaderEpoch || actual.PreviousRecordDigest != request.PreviousRecordDigest {
		return fmt.Errorf("local previous-record challenge changed")
	}
	return nil
}

func replicaRecordDigest(message types.Message) (string, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func selectReplicaCatchupSource(meta PartitionMetadata, localBrokerID string, brokers map[string]BrokerInfo) (BrokerInfo, bool) {
	candidates := make([]string, 0, len(meta.ISR))
	if meta.Leader != localBrokerID && containsString(meta.ISR, meta.Leader) {
		candidates = append(candidates, meta.Leader)
	}
	for _, brokerID := range meta.ISR {
		if brokerID != localBrokerID && brokerID != meta.Leader {
			candidates = append(candidates, brokerID)
		}
	}
	for _, brokerID := range candidates {
		broker, ok := brokers[brokerID]
		if ok && broker.Status == "active" && broker.Addr != "" && containsString(meta.Replicas, brokerID) {
			return broker, true
		}
	}
	return BrokerInfo{}, false
}

// FetchReplicaCatchup validates the request against current Raft metadata and
// returns only raw records below the authoritative committed HWM.
func (f *BrokerFSM) FetchReplicaCatchup(request ReplicaCatchupRequest) (ReplicaCatchupBatch, error) {
	return f.FetchReplicaCatchupContext(context.Background(), request)
}

// FetchReplicaCatchupContext uses short prepare/publication fences for mutually
// capable retained sources. Older modes retain their original fence lifetime.
// f.mu is always released before disk maintenance.
func (f *BrokerFSM) FetchReplicaCatchupContext(ctx context.Context, request ReplicaCatchupRequest) (ReplicaCatchupBatch, error) {
	if ctx == nil {
		return ReplicaCatchupBatch{}, fmt.Errorf("catch-up requires context")
	}
	if request.RecoveryProtocol == BootstrapRecoveryProtocolVersion || f.retainedSourceAvailable(request) {
		return f.fetchRetainedRecovery(ctx, request)
	}
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	defer unlock()
	return f.fetchReplicaCatchup(ctx, request)
}

func (f *BrokerFSM) fetchReplicaCatchup(ctx context.Context, request ReplicaCatchupRequest) (ReplicaCatchupBatch, error) {
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
	current.ISR = append([]string(nil), meta.ISR...)
	sourceBroker := request.SourceBroker
	if sourceBroker == "" {
		sourceBroker = request.Leader
	}
	var source, receiver BrokerInfo
	if broker := f.brokers[sourceBroker]; broker != nil {
		source = *broker
	}
	if broker := f.brokers[request.BrokerID]; broker != nil {
		receiver = *broker
	}
	f.mu.RUnlock()
	if !containsString(current.Replicas, request.BrokerID) {
		return ReplicaCatchupBatch{}, fmt.Errorf("broker %s is not a configured replica for %s", request.BrokerID, key)
	}
	if !current.CommittedHWMKnown {
		return ReplicaCatchupBatch{}, fmt.Errorf("%w: partition %s has no authoritative committed HWM", ErrUnsupportedRecoveryProtocol, key)
	}
	if !containsString(current.Replicas, sourceBroker) || !containsString(current.ISR, sourceBroker) {
		return ReplicaCatchupBatch{}, fmt.Errorf("source broker %s is not an in-sync replica for %s", sourceBroker, key)
	}
	if source.Status != "active" {
		return ReplicaCatchupBatch{}, fmt.Errorf("source broker %s is not active", sourceBroker)
	}
	if request.Leader != current.Leader || request.LeaderEpoch != current.LeaderEpoch {
		return ReplicaCatchupBatch{}, fmt.Errorf("stale leader fence for %s", key)
	}
	if request.LifecycleEpoch != current.LifecycleEpoch {
		return ReplicaCatchupBatch{}, fmt.Errorf("stale topic lifecycle epoch for %s", key)
	}
	if request.CommittedHWM > current.CommittedHWM {
		return ReplicaCatchupBatch{}, fmt.Errorf("committed HWM regressed for %s: current=%d requested=%d", key, current.CommittedHWM, request.CommittedHWM)
	}
	if request.NextOffset > current.CommittedHWM {
		return ReplicaCatchupBatch{}, fmt.Errorf("catch-up offset %d exceeds committed HWM %d", request.NextOffset, current.CommittedHWM)
	}
	if err := validateReplicaRecoveryRequest(request, source, receiver, current); err != nil {
		return ReplicaCatchupBatch{}, err
	}
	if request.PreviousRecordDigest != "" && request.NextOffset == 0 {
		return ReplicaCatchupBatch{}, fmt.Errorf("replica prefix proof cannot precede offset zero")
	}
	partition, err := f.replicaRecoveryPartition(request.Topic, request.Partition)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	f.mu.RLock()
	localTopic := f.tm.GetTopic(request.Topic)
	f.mu.RUnlock()
	if localTopic == nil || localTopic.Definition().LifecycleEpoch != request.LifecycleEpoch {
		return ReplicaCatchupBatch{}, fmt.Errorf("local source lifecycle changed")
	}
	localHWM, err := partition.ReplicaSourceHWMContext(ctx)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	if localHWM < current.CommittedHWM {
		return ReplicaCatchupBatch{}, fmt.Errorf("source committed HWM is behind recovery boundary")
	}
	generation, err := partition.CaptureReplicaContentGeneration(ctx)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	seal := func(batch ReplicaCatchupBatch) (ReplicaCatchupBatch, error) {
		if err := partition.CheckReplicaContentGeneration(ctx, generation); err != nil {
			return ReplicaCatchupBatch{}, err
		}
		sealed, err := SealReplicaCatchupBatch(batch)
		if err != nil {
			return ReplicaCatchupBatch{}, err
		}
		if err := ctx.Err(); err != nil {
			return ReplicaCatchupBatch{}, err
		}
		return sealed, nil
	}
	readPage := func(start, end uint64, max int) (disk.RetainedPage, error) {
		page, err := partition.ReadReplicaRetainedPage(ctx, start, end, max, util.MaxMessageSize)
		if err != nil {
			return disk.RetainedPage{}, err
		}
		if page.ContentGeneration != generation {
			return disk.RetainedPage{}, disk.ErrStaleContentGeneration
		}
		// Mode 0/1 hashes and wire records historically omit record identity.
		// Disk has already validated it; normalize only this detached wire view.
		for i := range page.Messages {
			page.Messages[i].Topic = ""
			page.Messages[i].Partition = 0
		}
		return page, nil
	}
	var matched *disk.LineagePrefix
	if request.RecoveryProtocol == IndexedRecoveryProtocolVersion {
		matched, err = partition.MatchReplicaLineageContext(ctx, request.PrefixCandidates, current.CommittedHWM)
		if err != nil {
			return ReplicaCatchupBatch{}, err
		}
		if matched == nil || matched.EndOffset < request.NextOffset {
			batch := ReplicaCatchupBatch{
				Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
				StartOffset: request.NextOffset, EndOffset: request.NextOffset, CommittedHWM: current.CommittedHWM,
				Leader: current.Leader, SourceBroker: sourceBroker, LeaderEpoch: current.LeaderEpoch, LifecycleEpoch: current.LifecycleEpoch,
				RecoveryProtocol: IndexedRecoveryProtocolVersion, MatchedPrefix: matched, BootstrapRequired: matched == nil,
			}
			if matched != nil {
				boundary := matched.EndOffset
				batch.TruncateTo = &boundary
			}
			return seal(batch)
		}
	}
	if request.PreviousRecordDigest != "" {
		previousPage, err := readPage(request.NextOffset-1, request.NextOffset, 1)
		previous := previousPage.Messages
		if err != nil {
			return ReplicaCatchupBatch{}, fmt.Errorf("read source record before catch-up offset %d: %w", request.NextOffset, err)
		}
		if len(previous) != 1 || previous[0].Offset != request.NextOffset-1 {
			return ReplicaCatchupBatch{}, fmt.Errorf("read source record before catch-up offset %d: record unavailable", request.NextOffset)
		}
		digest, err := replicaRecordDigest(previous[0])
		if err != nil {
			return ReplicaCatchupBatch{}, err
		}
		if digest != request.PreviousRecordDigest {
			if request.RecoveryProtocol != 0 {
				return ReplicaCatchupBatch{}, fmt.Errorf("last-record proof conflicts with matched full prefix")
			}
			// If retained epochs cannot prove a boundary, including legacy logs,
			// remove only the mismatched follower record;
			// the next request must prove the preceding prefix before recovery
			// can append authoritative committed records.
			truncateTo := request.NextOffset - 1
			if request.PreviousLeaderEpoch > 0 {
				boundary, err := replicaLeaderEpochEndOffsetContext(ctx, partition, generation, request.PreviousLeaderEpoch, request.NextOffset)
				if err != nil && !errors.Is(err, errReplicaEpochBoundaryUnavailable) {
					return ReplicaCatchupBatch{}, fmt.Errorf("find replica divergence boundary: %w", err)
				}
				if err == nil {
					truncateTo = boundary
				}
			}
			return seal(ReplicaCatchupBatch{
				Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
				StartOffset: request.NextOffset, EndOffset: request.NextOffset, CommittedHWM: current.CommittedHWM,
				Leader: current.Leader, SourceBroker: sourceBroker,
				LeaderEpoch: current.LeaderEpoch, LifecycleEpoch: current.LifecycleEpoch,
				TruncateTo: &truncateTo,
			})
		}
	}
	page, err := readPage(request.NextOffset, current.CommittedHWM, request.MaxRecords)
	if err != nil {
		return ReplicaCatchupBatch{}, err
	}
	f.mu.RLock()
	definition := copyTopicDefinition(f.topicState[request.Topic])
	f.mu.RUnlock()
	compactionEnabled := definition != nil && config.HasCleanupPolicy(definition.Policy.CleanupPolicy, config.CleanupPolicyCompact)
	if page.Compacted && !compactionEnabled {
		return ReplicaCatchupBatch{}, fmt.Errorf("non-contiguous committed range for uncompacted topic %s", request.Topic)
	}
	return seal(ReplicaCatchupBatch{
		Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
		StartOffset: request.NextOffset, EndOffset: page.EndOffset, CommittedHWM: current.CommittedHWM,
		Leader: current.Leader, SourceBroker: sourceBroker,
		LeaderEpoch: current.LeaderEpoch, LifecycleEpoch: current.LifecycleEpoch,
		Compacted: page.Compacted, Verified: page.Exhausted && page.EndOffset == current.CommittedHWM, Messages: page.Messages,
		RecoveryProtocol: request.RecoveryProtocol, MatchedPrefix: matched,
	})
}

// replicaLeaderEpochEndOffset finds a later retained epoch before the follower's
// tail. Missing epoch metadata is distinguished from unreadable storage so only
// the former permits digest-based rollback.
func (f *BrokerFSM) replicaLeaderEpochEndOffset(topicName string, partitionID int, leaderEpoch int64, before uint64) (uint64, error) {
	partition, err := f.replicaRecoveryPartition(topicName, partitionID)
	if err != nil {
		return 0, err
	}
	generation, err := partition.CaptureReplicaContentGeneration(context.Background())
	if err != nil {
		return 0, err
	}
	return replicaLeaderEpochEndOffsetContext(context.Background(), partition, generation, leaderEpoch, before)
}

func replicaLeaderEpochEndOffsetContext(ctx context.Context, partition *topic.Partition, generation disk.ContentGeneration, leaderEpoch int64, before uint64) (uint64, error) {
	if leaderEpoch <= 0 {
		return 0, fmt.Errorf("%w: legacy record has no leader epoch", errReplicaEpochBoundaryUnavailable)
	}
	offset, err := partition.ReplicaFirstOffsetContext(ctx)
	if err != nil {
		return 0, err
	}
	for offset < before {
		page, err := partition.ReadReplicaRetainedPage(ctx, offset, before, MaxReplicaCatchupRecords, util.MaxMessageSize)
		if err != nil {
			return 0, err
		}
		if page.ContentGeneration != generation {
			return 0, disk.ErrStaleContentGeneration
		}
		if len(page.Messages) == 0 {
			return 0, fmt.Errorf("leader epoch scan stopped at offset %d", offset)
		}
		for _, message := range page.Messages {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if message.LeaderEpoch == 0 {
				return 0, fmt.Errorf("%w: record %d has no leader epoch", errReplicaEpochBoundaryUnavailable, message.Offset)
			}
			if message.LeaderEpoch > leaderEpoch {
				return message.Offset, nil
			}
		}
		offset = page.EndOffset
	}
	return 0, fmt.Errorf("%w: source has no epoch boundary after follower epoch %d", errReplicaEpochBoundaryUnavailable, leaderEpoch)
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
	for _, message := range messages {
		if message.Offset >= committedHWM {
			break
		}
		result = append(result, message)
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

func removeString(values []string, unwanted string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != unwanted {
			result = append(result, value)
		}
	}
	return result
}
