package fsm

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

type PartitionMetadata struct {
	Leader         string   `json:"leader"`
	Replicas       []string `json:"replicas"`
	ISR            []string `json:"isr"`
	LeaderEpoch    int      `json:"leader_epoch"`
	CommittedHWM   uint64   `json:"committed_hwm"`
	PartitionCount int      `json:"partition_count"`
	Idempotent     bool     `json:"idempotent"`
	LifecycleEpoch uint64   `json:"lifecycle_epoch,omitempty"`
}

type TopicCommand struct {
	Name              string                 `json:"name,omitempty"`
	Partitions        int                    `json:"partitions,omitempty"`
	Idempotent        bool                   `json:"idempotent,omitempty"`
	EventSourcing     bool                   `json:"event_sourcing,omitempty"`
	LeaderID          string                 `json:"leader_id,omitempty"`
	ReplicationFactor int                    `json:"replication_factor,omitempty"`
	Policy            topic.Policy           `json:"policy,omitempty"`
	Definition        *topic.Definition      `json:"definition,omitempty"`
	Patch             *topic.DefinitionPatch `json:"patch,omitempty"`
}

func (f *BrokerFSM) applyTopicCommand(jsonData string) interface{} {
	var topicCmd TopicCommand
	if err := json.Unmarshal([]byte(jsonData), &topicCmd); err != nil {
		util.Error("FSM: Failed to unmarshal topic command: %v", err)
		return err
	}
	patchCommand := topicCmd.Definition != nil || topicCmd.Patch != nil
	if patchCommand && topicCmd.Definition == nil {
		return fmt.Errorf("topic patch command is missing the default definition")
	}

	base := topic.Definition{
		Name:              topicCmd.Name,
		Partitions:        topicCmd.Partitions,
		ReplicationFactor: topicCmd.ReplicationFactor,
		Idempotent:        topicCmd.Idempotent,
		EventSourcing:     topicCmd.EventSourcing,
		Policy:            topicCmd.Policy,
	}
	if topicCmd.Definition != nil {
		base = *topicCmd.Definition
	}
	base, err := base.Normalize()
	if err != nil {
		return fmt.Errorf("invalid topic definition: %w", err)
	}
	topicCmd.Name = base.Name
	var appliedDefinition topic.Definition

	stageResult := func() interface{} {
		f.mu.Lock()
		defer f.mu.Unlock()

		stagedTopics := copyTopicState(f.topicState)
		stagedPartitions := copyPartitionMetadataState(f.partitionMetadata)
		currentPartitions := 0
		currentTopic := stagedTopics[topicCmd.Name]
		if currentTopic == nil {
			currentTopic = legacyTopicState(stagedPartitions)[topicCmd.Name]
		}
		if currentTopic == nil {
			if f.cd != nil {
				if references := f.cd.TopicGroupReferences(topicCmd.Name); len(references) != 0 {
					return fmt.Errorf("topic %q lifecycle cleanup is pending for consumer group %q", topicCmd.Name, references[0].Name)
				}
			}
			if f.txn != nil {
				_, affected, stateErr := f.txn.StateWithoutTopicReferences(topicCmd.Name)
				if stateErr != nil {
					return fmt.Errorf("topic %q lifecycle cleanup is pending: %w", topicCmd.Name, stateErr)
				}
				if len(affected) != 0 {
					return fmt.Errorf("topic %q lifecycle cleanup is pending for transaction %q", topicCmd.Name, affected[0])
				}
			}
		}

		definition := base
		if currentTopic != nil {
			currentPartitions = currentTopic.Partitions
			var patch topic.DefinitionPatch
			if patchCommand {
				if topicCmd.Patch != nil {
					patch = *topicCmd.Patch
				}
			} else {
				patch = legacyTopicDefinitionPatch(base, topicCmd.ReplicationFactor > 0)
			}
			definition, err = topic.MergeDefinitionPatch(*currentTopic, patch, true)
			if err != nil {
				return err
			}
		} else if patchCommand {
			var patch topic.DefinitionPatch
			if topicCmd.Patch != nil {
				patch = *topicCmd.Patch
			}
			definition, err = topic.MergeDefinitionPatch(base, patch, false)
			if err != nil {
				return err
			}
		}
		if config.HasCleanupPolicy(definition.Policy.CleanupPolicy, config.CleanupPolicyCompact) {
			return fmt.Errorf("cleanup policy compact is not supported in distributed mode")
		}

		var brokers []string
		for id, info := range f.brokers {
			if info.Status == "active" {
				brokers = append(brokers, id)
			}
		}
		sort.Strings(brokers)

		if len(brokers) == 0 {
			util.Error("FSM: No active brokers available for topic creation")
			return fmt.Errorf("no active brokers")
		}

		if definition.Partitions <= 0 {
			util.Error("FSM: Invalid partition count %d for topic %s", definition.Partitions, topicCmd.Name)
			return fmt.Errorf("invalid partition count: %d", definition.Partitions)
		}

		if topicCmd.LeaderID != "" {
			found := false
			for _, b := range brokers {
				if b == topicCmd.LeaderID {
					found = true
					break
				}
			}
			if !found {
				util.Error("FSM: Explicit leader %s not in active broker set %v", topicCmd.LeaderID, brokers)
				return fmt.Errorf("leader %s not in active broker set", topicCmd.LeaderID)
			}
		}

		replicationFactor := definition.ReplicationFactor
		if replicationFactor > len(brokers) {
			util.Warn("FSM: Requested RF %d exceeds active brokers %d. Capping to %d", replicationFactor, len(brokers), len(brokers))
			replicationFactor = len(brokers)
		}

		ring := util.NewConsistentHashRing(150, nil)
		ring.Add(brokers...)

		for i := 0; i < currentPartitions; i++ {
			key := topicCmd.Name + "-" + strconv.Itoa(i)
			if stagedPartitions[key] == nil {
				return fmt.Errorf("topic %q is missing partition metadata %d", topicCmd.Name, i)
			}
		}
		for i := 0; i < currentPartitions; i++ {
			key := topicCmd.Name + "-" + strconv.Itoa(i)
			stagedPartitions[key].PartitionCount = definition.Partitions
			stagedPartitions[key].LifecycleEpoch = definition.LifecycleEpoch
		}

		for i := currentPartitions; i < definition.Partitions; i++ {
			key := topicCmd.Name + "-" + strconv.Itoa(i)

			assignedLeader := topicCmd.LeaderID
			var replicas []string
			if assignedLeader == "" {
				replicas = ring.GetN(key, replicationFactor)
				if len(replicas) == 0 {
					return fmt.Errorf("no replicas assigned for partition %s", key)
				}
				assignedLeader = replicas[0]
			} else {
				// Explicit leader: build replica set starting from leader
				replicas = ring.GetN(key, replicationFactor)
				// Ensure the explicit leader is in the replica set
				leaderFound := false
				for _, r := range replicas {
					if r == assignedLeader {
						leaderFound = true
						break
					}
				}
				if !leaderFound {
					replicas[len(replicas)-1] = assignedLeader
				}
			}

			isrCopy := append([]string(nil), replicas...)

			stagedPartitions[key] = &PartitionMetadata{
				PartitionCount: definition.Partitions,
				Leader:         assignedLeader,
				LeaderEpoch:    1,
				Idempotent:     definition.Idempotent,
				LifecycleEpoch: definition.LifecycleEpoch,
				Replicas:       replicas,
				ISR:            isrCopy,
			}
			util.Info("FSM: Assigned leader %s to partition %s (replicas=%v)", assignedLeader, key, replicas)
		}

		stagedTopics[topicCmd.Name] = copyTopicDefinition(&definition)
		f.partitionMetadata = stagedPartitions
		f.topicState = stagedTopics
		appliedDefinition = definition
		return nil
	}()
	if stageResult != nil {
		return stageResult
	}

	if err := f.materializeTopicCreate(&appliedDefinition); err != nil {
		util.Error("FSM: Failed to create topic '%s' in local manager: %v", topicCmd.Name, err)
		return err
	}
	util.Info("FSM: Created topic '%s' with %d partitions", topicCmd.Name, appliedDefinition.Partitions)
	return nil
}

func legacyTopicDefinitionPatch(definition topic.Definition, includeReplicationFactor bool) topic.DefinitionPatch {
	partitions := definition.Partitions
	cleanupPolicy := definition.Policy.CleanupPolicy
	retentionHours := definition.Policy.RetentionHours
	retentionBytes := definition.Policy.RetentionBytes
	partitioner := definition.Policy.Partitioner
	authPolicy := definition.Policy.AuthPolicy
	readACL := append([]string(nil), definition.Policy.ReadACL...)
	writeACL := append([]string(nil), definition.Policy.WriteACL...)
	patch := topic.DefinitionPatch{
		Partitions:     &partitions,
		CleanupPolicy:  &cleanupPolicy,
		RetentionHours: &retentionHours,
		RetentionBytes: &retentionBytes,
		Partitioner:    &partitioner,
		AuthPolicy:     &authPolicy,
		ReadACL:        &readACL,
		WriteACL:       &writeACL,
	}
	if includeReplicationFactor {
		replicationFactor := definition.ReplicationFactor
		patch.ReplicationFactor = &replicationFactor
	}
	return patch
}

func (f *BrokerFSM) applyTopicDeleteCommand(jsonData string) interface{} {
	var payload struct {
		Topic    string `json:"topic"`
		IfExists bool   `json:"if_exists,omitempty"`
	}
	if err := json.Unmarshal([]byte(jsonData), &payload); err != nil {
		util.Error("FSM: Failed to unmarshal topic delete command: %v", err)
		return err
	}
	if err := topic.ValidateName(payload.Topic); err != nil {
		return fmt.Errorf("invalid topic name: %w", err)
	}
	if payload.Topic == config.ConsumerOffsetsTopicName {
		return fmt.Errorf("cannot delete broker-owned internal consumer metadata topic")
	}

	f.mu.RLock()
	found := f.topicState[payload.Topic] != nil
	for key := range f.partitionMetadata {
		if idx := strings.LastIndex(key, "-"); idx != -1 && key[:idx] == payload.Topic {
			found = true
		}
	}
	coordinatorRef := f.cd
	transactionManager := f.txn
	f.mu.RUnlock()
	if !found && !payload.IfExists {
		return fmt.Errorf("%w: %s", topic.ErrTopicNotFound, payload.Topic)
	}
	if coordinatorRef != nil {
		for _, reference := range coordinatorRef.TopicGroupReferences(payload.Topic) {
			if reference.MemberCount != 0 {
				return fmt.Errorf(
					"%w: topic %q has active consumer group %q with %d member(s)",
					topic.ErrTopicDeleteBlocked,
					payload.Topic,
					reference.Name,
					reference.MemberCount,
				)
			}
		}
	}
	if transactionManager != nil {
		if _, _, err := transactionManager.StateWithoutTopicReferences(payload.Topic); err != nil {
			return fmt.Errorf("%w: %v", topic.ErrTopicDeleteBlocked, err)
		}
	}
	if coordinatorRef != nil {
		if _, err := coordinatorRef.DeleteInactiveGroupsForTopic(payload.Topic); err != nil {
			return fmt.Errorf("delete consumer groups for topic %q: %w", payload.Topic, err)
		}
	}
	if transactionManager != nil {
		if _, err := transactionManager.PruneTopicReferences(payload.Topic); err != nil {
			return fmt.Errorf("%w: %v", topic.ErrTopicDeleteBlocked, err)
		}
	}

	f.mu.Lock()
	for key := range f.partitionMetadata {
		if idx := strings.LastIndex(key, "-"); idx != -1 && key[:idx] == payload.Topic {
			delete(f.partitionMetadata, key)
		}
	}
	delete(f.topicState, payload.Topic)
	delete(f.producerState, payload.Topic)
	f.mu.Unlock()

	if err := f.materializeTopicDelete(payload.Topic); err != nil {
		util.Warn("FSM: Topic '%s' was logically deleted with pending local storage cleanup: %v", payload.Topic, err)
		return topic.DeleteResult{Deleted: found, CleanupPending: true}
	}
	util.Info("FSM: Deleted topic '%s'", payload.Topic)
	if !found {
		return topic.DeleteResult{Deleted: false}
	}
	return nil
}

func (f *BrokerFSM) applyTopicTruncateCommand(jsonData string) interface{} {
	var payload struct {
		Topic            string `json:"topic"`
		ExpectedRevision uint64 `json:"expected_revision"`
	}
	if err := json.Unmarshal([]byte(jsonData), &payload); err != nil {
		return err
	}
	if err := topic.ValidateName(payload.Topic); err != nil {
		return fmt.Errorf("invalid topic name: %w", err)
	}
	if payload.Topic == config.ConsumerOffsetsTopicName {
		return fmt.Errorf("cannot truncate broker-owned internal consumer metadata topic")
	}
	if payload.ExpectedRevision == 0 {
		return fmt.Errorf("expected_revision must be greater than zero")
	}

	f.mu.RLock()
	current := copyTopicDefinition(f.topicState[payload.Topic])
	coordinatorRef := f.cd
	transactionManager := f.txn
	for _, broker := range f.brokers {
		if broker.Status == "active" && broker.LifecycleProtocol < TopicLifecycleProtocolVersion {
			f.mu.RUnlock()
			return fmt.Errorf(
				"topic truncate requires lifecycle protocol %d on every active broker; broker %q advertises %d",
				TopicLifecycleProtocolVersion, broker.ID, broker.LifecycleProtocol,
			)
		}
	}
	for key, metadata := range f.partitionMetadata {
		idx := strings.LastIndex(key, "-")
		if idx != -1 && key[:idx] == payload.Topic && metadata != nil && metadata.LeaderEpoch == math.MaxInt {
			f.mu.RUnlock()
			return fmt.Errorf("leader epoch overflow for partition %q", key)
		}
	}
	f.mu.RUnlock()
	if current == nil {
		return fmt.Errorf("%w: %s", topic.ErrTopicNotFound, payload.Topic)
	}
	if current.Revision != payload.ExpectedRevision {
		return fmt.Errorf(
			"%w for topic %q: current=%d expected=%d",
			topic.ErrTopicRevisionConflict, payload.Topic, current.Revision, payload.ExpectedRevision,
		)
	}
	if current.Revision == math.MaxUint64 || current.LifecycleEpoch == math.MaxUint64 {
		return fmt.Errorf("topic lifecycle counter overflow for %q", payload.Topic)
	}

	if coordinatorRef != nil {
		for _, reference := range coordinatorRef.TopicGroupReferences(payload.Topic) {
			if reference.MemberCount != 0 {
				return fmt.Errorf(
					"%w: topic %q has active consumer group %q with %d member(s)",
					topic.ErrTopicDeleteBlocked, payload.Topic, reference.Name, reference.MemberCount,
				)
			}
		}
	}
	if transactionManager != nil {
		if _, _, err := transactionManager.StateWithoutTopicReferences(payload.Topic); err != nil {
			return fmt.Errorf("%w: %v", topic.ErrTopicDeleteBlocked, err)
		}
	}
	if coordinatorRef != nil {
		if _, err := coordinatorRef.DeleteInactiveGroupsForTopic(payload.Topic); err != nil {
			return fmt.Errorf("delete consumer groups for topic %q: %w", payload.Topic, err)
		}
	}
	if transactionManager != nil {
		if _, err := transactionManager.PruneTopicReferences(payload.Topic); err != nil {
			return fmt.Errorf("%w: %v", topic.ErrTopicDeleteBlocked, err)
		}
	}

	target := *current
	target.Revision++
	target.LifecycleEpoch++
	f.mu.Lock()
	latest := f.topicState[payload.Topic]
	if latest == nil || latest.Revision != current.Revision || latest.LifecycleEpoch != current.LifecycleEpoch {
		f.mu.Unlock()
		return fmt.Errorf("%w for topic %q: authoritative definition changed during truncate", topic.ErrTopicRevisionConflict, payload.Topic)
	}
	stagedPartitions := copyPartitionMetadataState(f.partitionMetadata)
	for key, metadata := range stagedPartitions {
		idx := strings.LastIndex(key, "-")
		if idx == -1 || key[:idx] != payload.Topic || metadata == nil {
			continue
		}
		if metadata.LeaderEpoch == math.MaxInt {
			f.mu.Unlock()
			return fmt.Errorf("leader epoch overflow for partition %q", key)
		}
		metadata.CommittedHWM = 0
		metadata.LeaderEpoch++
		metadata.LifecycleEpoch = target.LifecycleEpoch
	}
	stagedLogs := make(map[uint64]*ReplicationEntry, len(f.logs))
	for index, entry := range f.logs {
		if entry != nil && entry.Topic == payload.Topic {
			continue
		}
		stagedLogs[index] = entry
	}
	f.partitionMetadata = stagedPartitions
	f.logs = stagedLogs
	delete(f.producerState, payload.Topic)
	f.topicState[payload.Topic] = copyTopicDefinition(&target)
	f.mu.Unlock()

	result := topic.TruncateResult{Topic: payload.Topic, Truncated: true, Definition: target}
	if err := f.materializeTopicTruncate(&target); err != nil {
		util.Warn("FSM: Topic %q truncate committed with pending local materialization: %v", payload.Topic, err)
		result.CleanupPending = true
		return result
	}
	return result
}

func (f *BrokerFSM) applyPartitionCommand(jsonData string) interface{} {
	parts := strings.SplitN(jsonData, ":", 2)
	if len(parts) != 2 {
		util.Error("FSM: Invalid partition command format: %s", jsonData)
		return fmt.Errorf("invalid partition command format")
	}

	key := parts[0]
	var metadata PartitionMetadata
	if err := json.Unmarshal([]byte(parts[1]), &metadata); err != nil {
		util.Error("FSM: Failed to unmarshal partition metadata for %s: %v", key, err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if current := f.partitionMetadata[key]; current != nil {
		if metadata.LeaderEpoch < current.LeaderEpoch {
			return fmt.Errorf("stale leader epoch for %s: current=%d requested=%d", key, current.LeaderEpoch, metadata.LeaderEpoch)
		}
		if metadata.Leader != current.Leader && metadata.LeaderEpoch <= current.LeaderEpoch {
			return fmt.Errorf("leader change for %s must advance epoch: current=%d requested=%d", key, current.LeaderEpoch, metadata.LeaderEpoch)
		}
		if metadata.CommittedHWM < current.CommittedHWM {
			return fmt.Errorf("committed HWM regression for %s: current=%d requested=%d", key, current.CommittedHWM, metadata.CommittedHWM)
		}
	}
	f.partitionMetadata[key] = &metadata
	util.Debug("FSM: Updated partition metadata for %s", key)
	return nil
}

type partitionCommitCommand struct {
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	Leader         string `json:"leader"`
	LeaderEpoch    int    `json:"leader_epoch"`
	HWM            uint64 `json:"hwm"`
	LifecycleEpoch uint64 `json:"lifecycle_epoch,omitempty"`
}

func (f *BrokerFSM) applyPartitionCommitCommand(jsonData string) interface{} {
	var cmd partitionCommitCommand
	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		return fmt.Errorf("unmarshal partition commit: %w", err)
	}
	key := fmt.Sprintf("%s-%d", cmd.Topic, cmd.Partition)

	f.mu.Lock()
	metadata := f.partitionMetadata[key]
	if metadata == nil {
		f.mu.Unlock()
		return fmt.Errorf("partition metadata %s not found", key)
	}
	if cmd.Leader != metadata.Leader {
		f.mu.Unlock()
		return fmt.Errorf("partition leader fenced for %s: current=%s requested=%s", key, metadata.Leader, cmd.Leader)
	}
	if cmd.LeaderEpoch != metadata.LeaderEpoch {
		f.mu.Unlock()
		return fmt.Errorf("stale leader epoch for %s: current=%d requested=%d", key, metadata.LeaderEpoch, cmd.LeaderEpoch)
	}
	if cmd.HWM < metadata.CommittedHWM {
		f.mu.Unlock()
		return fmt.Errorf("committed HWM regression for %s: current=%d requested=%d", key, metadata.CommittedHWM, cmd.HWM)
	}
	metadataLifecycleEpoch := metadata.LifecycleEpoch
	if metadataLifecycleEpoch == 0 {
		metadataLifecycleEpoch = topic.InitialLifecycleEpoch
	}
	if cmd.LifecycleEpoch == 0 {
		if metadataLifecycleEpoch > topic.InitialLifecycleEpoch {
			f.mu.Unlock()
			return fmt.Errorf("missing topic lifecycle epoch for %s", key)
		}
	} else if cmd.LifecycleEpoch != metadataLifecycleEpoch {
		f.mu.Unlock()
		return fmt.Errorf("stale topic lifecycle epoch for %s: current=%d requested=%d", key, metadataLifecycleEpoch, cmd.LifecycleEpoch)
	}
	metadata.CommittedHWM = cmd.HWM
	tm := f.tm
	f.mu.Unlock()

	if tm == nil {
		return nil
	}
	t := tm.GetTopic(cmd.Topic)
	if t == nil {
		return nil
	}
	p, err := t.GetPartition(cmd.Partition)
	if err != nil {
		return nil
	}
	if err := p.ApplyReplicaHWM(cmd.HWM); err != nil {
		util.Warn("FSM: Replica has not caught up to committed HWM for %s: %v", key, err)
		return nil
	}
	p.FlushDisk()
	return nil
}

// applyJoinGroupCommand restores group join state.
func (f *BrokerFSM) applyJoinGroupCommand(jsonData string) interface{} {
	var cmd struct {
		Group  string `json:"group"`
		Member string `json:"member"`
	}
	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("FSM: Failed to unmarshal join group: %v", err)
		return err
	}

	if f.cd != nil {
		_, err := f.cd.AddConsumer(cmd.Group, cmd.Member)
		if err != nil {
			return err
		}
		util.Info("FSM: Synced JOIN_GROUP group=%s member=%s", cmd.Group, cmd.Member)
	}
	return nil
}

func (f *BrokerFSM) applyGroupSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Type           string   `json:"type"`
		Group          string   `json:"group"`
		Member         string   `json:"member"`
		Members        []string `json:"members"`
		Topic          string   `json:"topic"`
		Generation     *int     `json:"generation"`
		PartitionCount int      `json:"partition_count"`
	}

	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("Failed to unmarshal group sync: %v", err)
		return err
	}

	if f.cd == nil {
		return fmt.Errorf("coordinator not ready")
	}

	switch cmd.Type {
	case "REGISTER":
		if f.tm == nil {
			return fmt.Errorf("topic manager not available in FSM")
		}
		t := f.tm.GetTopic(cmd.Topic)
		if t == nil {
			return fmt.Errorf("topic '%s' not found during group registration", cmd.Topic)
		}
		partitionCount := len(t.Partitions)
		if cmd.PartitionCount > 0 && cmd.PartitionCount != partitionCount {
			return fmt.Errorf("partition count mismatch for topic %s: requested=%d actual=%d", cmd.Topic, cmd.PartitionCount, partitionCount)
		}
		if err := f.cd.RegisterGroup(cmd.Topic, cmd.Group, partitionCount); err != nil {
			return err
		}
		util.Info("FSM: Registered group %s for topic %s", cmd.Group, cmd.Topic)
		return nil
	case "JOIN":
		if f.cd.GetGroup(cmd.Group) == nil {
			if f.tm == nil {
				return fmt.Errorf("topic manager not available in FSM")
			}
			t := f.tm.GetTopic(cmd.Topic)
			if t == nil {
				return fmt.Errorf("topic '%s' not found during group join", cmd.Topic)
			}

			if err := f.cd.RegisterGroup(cmd.Topic, cmd.Group, len(t.Partitions)); err != nil {
				util.Warn("FSM: Failed to auto-register group %s: %v", cmd.Group, err)
			} else {
				util.Info("FSM: Auto-registered group %s for topic %s", cmd.Group, cmd.Topic)
			}
		}

		_, err := f.cd.AddConsumer(cmd.Group, cmd.Member)
		if err != nil {
			return err
		}
	case "LEAVE":
		if cmd.Generation == nil {
			// Compatibility with metadata entries written before generation
			// fencing was introduced.
			return f.cd.RemoveConsumer(cmd.Group, cmd.Member)
		}
		return f.cd.RemoveConsumerForGeneration(cmd.Group, cmd.Member, *cmd.Generation)
	case "EXPIRE":
		if cmd.Generation == nil {
			return fmt.Errorf("missing generation for group expiration")
		}
		if len(cmd.Members) == 0 {
			return fmt.Errorf("missing members for group expiration")
		}
		return f.cd.ExpireConsumers(cmd.Group, *cmd.Generation, cmd.Members)
	}

	return nil
}

func (f *BrokerFSM) applyRegisterCommand(jsonData string) interface{} {
	var info BrokerInfo
	if err := json.Unmarshal([]byte(jsonData), &info); err != nil {
		util.Error("FSM: Failed to unmarshal registration: %v", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.brokers[info.ID] = &info

	util.Info("FSM: Member %s added to registry", info.ID)
	return nil
}

func (f *BrokerFSM) applyDeregisterCommand(jsonData string) interface{} {
	var info struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(jsonData), &info); err != nil {
		util.Error("FSM: Failed to unmarshal deregistration: %v", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if b, ok := f.brokers[info.ID]; ok {
		b.Status = "inactive"
		util.Info("FSM: Member %s marked as inactive", info.ID)
	}
	return nil
}

func (f *BrokerFSM) handleUnknownCommand(data string) interface{} {
	preview := data
	if len(preview) > 20 {
		preview = preview[:20]
	}
	util.Error("Unknown log entry type: %s", preview)
	return fmt.Errorf("unknown command: %s", preview)
}

// applyMessageCommand is defined in fsm_replication.go and is not duplicated here.
