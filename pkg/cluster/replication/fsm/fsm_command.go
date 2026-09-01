package fsm

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

var ErrPartitionCommitFenced = errors.New("partition commit fenced")

type PartitionMetadata struct {
	Leader       string   `json:"leader"`
	Replicas     []string `json:"replicas"`
	ISR          []string `json:"isr"`
	LeaderEpoch  int      `json:"leader_epoch"`
	CommittedHWM uint64   `json:"-"`
	// CommittedHWMKnown distinguishes metadata written before the committed
	// watermark existed from a real committed watermark of zero. It is an
	// in-memory compatibility marker; the presence of committed_hwm on the
	// wire is authoritative.
	CommittedHWMKnown bool `json:"-"`
	PartitionCount    int  `json:"partition_count"`
	Idempotent        bool `json:"idempotent"`
}

type partitionMetadataJSON struct {
	Leader         string   `json:"leader"`
	Replicas       []string `json:"replicas"`
	ISR            []string `json:"isr"`
	LeaderEpoch    int      `json:"leader_epoch"`
	CommittedHWM   *uint64  `json:"committed_hwm,omitempty"`
	PartitionCount int      `json:"partition_count"`
	Idempotent     bool     `json:"idempotent"`
}

func (m PartitionMetadata) MarshalJSON() ([]byte, error) {
	var committed *uint64
	if m.CommittedHWMKnown || m.CommittedHWM != 0 {
		value := m.CommittedHWM
		committed = &value
	}
	return json.Marshal(partitionMetadataJSON{
		Leader:         m.Leader,
		Replicas:       m.Replicas,
		ISR:            m.ISR,
		LeaderEpoch:    m.LeaderEpoch,
		CommittedHWM:   committed,
		PartitionCount: m.PartitionCount,
		Idempotent:     m.Idempotent,
	})
}

func (m *PartitionMetadata) UnmarshalJSON(data []byte) error {
	var decoded partitionMetadataJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*m = PartitionMetadata{
		Leader:            decoded.Leader,
		Replicas:          decoded.Replicas,
		ISR:               decoded.ISR,
		LeaderEpoch:       decoded.LeaderEpoch,
		PartitionCount:    decoded.PartitionCount,
		Idempotent:        decoded.Idempotent,
		CommittedHWMKnown: decoded.CommittedHWM != nil,
	}
	if decoded.CommittedHWM != nil {
		m.CommittedHWM = *decoded.CommittedHWM
	}
	return nil
}

type TopicCommand struct {
	Name              string       `json:"name"`
	Partitions        int          `json:"partitions"`
	Idempotent        bool         `json:"idempotent"`
	EventSourcing     bool         `json:"event_sourcing"`
	LeaderID          string       `json:"leader_id"`
	ReplicationFactor int          `json:"replication_factor,omitempty"`
	Policy            topic.Policy `json:"policy,omitempty"`
}

type TopicConfigCommand struct {
	Name                   string `json:"name"`
	MinInSyncReplicas      *int   `json:"min_in_sync_replicas,omitempty"`
	ResetMinInSyncReplicas bool   `json:"reset_min_in_sync_replicas,omitempty"`
}

func (f *BrokerFSM) applyTopicCommand(jsonData string) interface{} {
	var topicCmd TopicCommand
	if err := json.Unmarshal([]byte(jsonData), &topicCmd); err != nil {
		util.Error("FSM: Failed to unmarshal topic command: %v", err)
		return err
	}
	definition, err := (topic.Definition{
		Name:          topicCmd.Name,
		Partitions:    topicCmd.Partitions,
		Idempotent:    topicCmd.Idempotent,
		EventSourcing: topicCmd.EventSourcing,
		Policy:        topicCmd.Policy,
	}).Normalize()
	if err != nil {
		return fmt.Errorf("invalid topic definition: %w", err)
	}
	if config.HasCleanupPolicy(definition.Policy.CleanupPolicy, config.CleanupPolicyCompact) {
		return fmt.Errorf("cleanup policy compact is not supported in distributed mode")
	}
	topicCmd.Name = definition.Name
	topicCmd.Partitions = definition.Partitions
	topicCmd.Idempotent = definition.Idempotent
	topicCmd.EventSourcing = definition.EventSourcing
	topicCmd.Policy = definition.Policy

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
		if currentTopic != nil {
			if topicCmd.Partitions < currentTopic.Partitions {
				return fmt.Errorf("cannot decrease partition count for topic %q: %d -> %d", topicCmd.Name, currentTopic.Partitions, topicCmd.Partitions)
			}
			currentPartitions = currentTopic.Partitions
			topicCmd.Idempotent = currentTopic.Idempotent
			topicCmd.EventSourcing = currentTopic.EventSourcing
			if topicCmd.Policy.MinInSyncReplicas == nil {
				topicCmd.Policy.MinInSyncReplicas = currentTopic.Policy.Clone().MinInSyncReplicas
				definition.Policy.MinInSyncReplicas = topicCmd.Policy.MinInSyncReplicas
			}
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

		if topicCmd.Partitions <= 0 {
			util.Error("FSM: Invalid partition count %d for topic %s", topicCmd.Partitions, topicCmd.Name)
			return fmt.Errorf("invalid partition count: %d", topicCmd.Partitions)
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

		replicationFactor := topicCmd.ReplicationFactor
		if replicationFactor <= 0 {
			replicationFactor = 3 // default small-cluster replication factor
		}
		if replicationFactor > len(brokers) {
			util.Warn("FSM: Requested RF %d exceeds active brokers %d. Capping to %d", replicationFactor, len(brokers), len(brokers))
			replicationFactor = len(brokers)
		}
		topicCmd.ReplicationFactor = replicationFactor
		if definition.Policy.MinInSyncReplicas != nil && *definition.Policy.MinInSyncReplicas > replicationFactor {
			return fmt.Errorf(
				"min_in_sync_replicas %d exceeds replication factor %d",
				*definition.Policy.MinInSyncReplicas,
				replicationFactor,
			)
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
			stagedPartitions[key].PartitionCount = topicCmd.Partitions
		}

		for i := currentPartitions; i < topicCmd.Partitions; i++ {
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
				PartitionCount:    topicCmd.Partitions,
				Leader:            assignedLeader,
				LeaderEpoch:       1,
				CommittedHWMKnown: true,
				Idempotent:        topicCmd.Idempotent,
				Replicas:          replicas,
				ISR:               isrCopy,
			}
			util.Info("FSM: Assigned leader %s to partition %s (replicas=%v)", assignedLeader, key, replicas)
		}

		definition.Idempotent = topicCmd.Idempotent
		definition.EventSourcing = topicCmd.EventSourcing
		stagedTopics[topicCmd.Name] = copyTopicDefinition(&definition)
		f.partitionMetadata = stagedPartitions
		f.topicState = stagedTopics
		return nil
	}()
	if stageResult != nil {
		return stageResult
	}

	if err := f.materializeTopicCreate(&definition); err != nil {
		util.Error("FSM: Failed to create topic '%s' in local manager: %v", topicCmd.Name, err)
		return err
	}
	util.Info("FSM: Created topic '%s' with %d partitions", topicCmd.Name, topicCmd.Partitions)
	return nil
}

func (f *BrokerFSM) applyTopicConfigCommand(jsonData string) interface{} {
	var command TopicConfigCommand
	if err := json.Unmarshal([]byte(jsonData), &command); err != nil {
		return fmt.Errorf("unmarshal topic config: %w", err)
	}
	if err := topic.ValidateName(command.Name); err != nil {
		return fmt.Errorf("invalid topic name: %w", err)
	}
	if command.ResetMinInSyncReplicas && command.MinInSyncReplicas != nil {
		return fmt.Errorf("min_in_sync_replicas set and reset are mutually exclusive")
	}
	if !command.ResetMinInSyncReplicas && command.MinInSyncReplicas == nil {
		return fmt.Errorf("min_in_sync_replicas is required")
	}

	f.mu.RLock()
	current := copyTopicDefinition(f.topicState[command.Name])
	replicationFactor := 0
	if current != nil {
		for partition := 0; partition < current.Partitions; partition++ {
			metadata := f.partitionMetadata[command.Name+"-"+strconv.Itoa(partition)]
			if metadata == nil || len(metadata.Replicas) == 0 {
				replicationFactor = 0
				break
			}
			if replicationFactor == 0 || len(metadata.Replicas) < replicationFactor {
				replicationFactor = len(metadata.Replicas)
			}
		}
	}
	f.mu.RUnlock()
	if current == nil {
		return fmt.Errorf("%w: %s", topic.ErrTopicNotFound, command.Name)
	}
	if replicationFactor < 1 {
		return fmt.Errorf("replication factor unavailable for topic %q", command.Name)
	}

	var value *int
	if !command.ResetMinInSyncReplicas {
		valueCopy := *command.MinInSyncReplicas
		if valueCopy < 1 || valueCopy > replicationFactor {
			return fmt.Errorf(
				"min_in_sync_replicas must be between 1 and replication factor %d",
				replicationFactor,
			)
		}
		value = &valueCopy
	}
	updated := copyTopicDefinition(current)
	updated.Policy.MinInSyncReplicas = value
	normalized, err := updated.Normalize()
	if err != nil {
		return fmt.Errorf("invalid topic definition: %w", err)
	}

	if f.tm != nil {
		if err := f.tm.AlterTopicMinInSyncReplicas(command.Name, value, replicationFactor); err != nil {
			return fmt.Errorf("materialize topic config: %w", err)
		}
	}
	f.mu.Lock()
	f.topicState[command.Name] = copyTopicDefinition(&normalized)
	f.mu.Unlock()
	return nil
}

func (f *BrokerFSM) applyTopicDeleteCommand(jsonData string) interface{} {
	var payload struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal([]byte(jsonData), &payload); err != nil {
		util.Error("FSM: Failed to unmarshal topic delete command: %v", err)
		return err
	}
	if err := topic.ValidateName(payload.Topic); err != nil {
		return fmt.Errorf("invalid topic name: %w", err)
	}

	stageResult := func() interface{} {
		f.mu.Lock()
		defer f.mu.Unlock()

		found := f.topicState[payload.Topic] != nil
		for key := range f.partitionMetadata {
			if idx := strings.LastIndex(key, "-"); idx != -1 && key[:idx] == payload.Topic {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("%w: %s", topic.ErrTopicNotFound, payload.Topic)
		}

		for key := range f.partitionMetadata {
			if idx := strings.LastIndex(key, "-"); idx != -1 && key[:idx] == payload.Topic {
				delete(f.partitionMetadata, key)
			}
		}
		delete(f.topicState, payload.Topic)
		return nil
	}()
	if stageResult != nil {
		return stageResult
	}

	if err := f.materializeTopicDelete(payload.Topic); err != nil {
		return err
	}
	util.Info("FSM: Deleted topic '%s'", payload.Topic)
	return nil
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
		if current.CommittedHWMKnown && !metadata.CommittedHWMKnown {
			metadata.CommittedHWM = current.CommittedHWM
			metadata.CommittedHWMKnown = true
		}
		if metadata.CommittedHWMKnown && current.CommittedHWMKnown && metadata.CommittedHWM < current.CommittedHWM {
			return fmt.Errorf("committed HWM regression for %s: current=%d requested=%d", key, current.CommittedHWM, metadata.CommittedHWM)
		}
	}
	f.partitionMetadata[key] = &metadata
	util.Debug("FSM: Updated partition metadata for %s", key)
	return nil
}

type partitionCommitCommand struct {
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	Leader      string `json:"leader"`
	LeaderEpoch int    `json:"leader_epoch"`
	HWM         uint64 `json:"hwm"`
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
		return fmt.Errorf("%w for %s: current=%s requested=%s", ErrPartitionCommitFenced, key, metadata.Leader, cmd.Leader)
	}
	if cmd.LeaderEpoch != metadata.LeaderEpoch {
		f.mu.Unlock()
		return fmt.Errorf("%w for %s: current_epoch=%d requested_epoch=%d", ErrPartitionCommitFenced, key, metadata.LeaderEpoch, cmd.LeaderEpoch)
	}
	if cmd.HWM < metadata.CommittedHWM {
		f.mu.Unlock()
		return fmt.Errorf("committed HWM regression for %s: current=%d requested=%d", key, metadata.CommittedHWM, cmd.HWM)
	}
	metadata.CommittedHWM = cmd.HWM
	metadata.CommittedHWMKnown = true
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
