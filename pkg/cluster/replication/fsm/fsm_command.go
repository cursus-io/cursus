package fsm

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cursus-io/cursus/util"
)

type PartitionMetadata struct {
	Leader         string   `json:"leader"`
	Replicas       []string `json:"replicas"`
	ISR            []string `json:"isr"`
	LeaderEpoch    int      `json:"leader_epoch"`
	PartitionCount int      `json:"partition_count"`
	Idempotent     bool     `json:"idempotent"`
}

type TopicCommand struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	Idempotent        bool   `json:"idempotent"`
	LeaderID          string `json:"leader_id"`
	ReplicationFactor int    `json:"replication_factor,omitempty"`
}

func (f *BrokerFSM) applyTopicCommand(jsonData string) interface{} {
	var topicCmd TopicCommand
	if err := json.Unmarshal([]byte(jsonData), &topicCmd); err != nil {
		util.Error("FSM: Failed to unmarshal topic command: %v", err)
		return err
	}

	f.mu.Lock()

	var brokers []string
	for id, info := range f.brokers {
		if info.Status == "active" {
			brokers = append(brokers, id)
		}
	}
	sort.Strings(brokers)

	if len(brokers) == 0 {
		f.mu.Unlock()
		util.Error("FSM: No active brokers available for topic creation")
		return fmt.Errorf("no active brokers")
	}

	if topicCmd.Partitions <= 0 {
		f.mu.Unlock()
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
			f.mu.Unlock()
			util.Error("FSM: Explicit leader %s not in active broker set %v", topicCmd.LeaderID, brokers)
			return fmt.Errorf("leader %s not in active broker set", topicCmd.LeaderID)
		}
	}

	replicationFactor := topicCmd.ReplicationFactor
	if replicationFactor <= 0 {
		replicationFactor = 3 // default, same as Kafka
	}
	if replicationFactor > len(brokers) {
		util.Warn("FSM: Requested RF %d exceeds active brokers %d. Capping to %d", replicationFactor, len(brokers), len(brokers))
		replicationFactor = len(brokers)
	}

	ring := util.NewConsistentHashRing(150, nil)
	ring.Add(brokers...)

	for i := 0; i < topicCmd.Partitions; i++ {
		key := fmt.Sprintf("%s-%d", topicCmd.Name, i)

		assignedLeader := topicCmd.LeaderID
		var replicas []string
		if assignedLeader == "" {
			replicas = ring.GetN(key, replicationFactor)
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

		f.partitionMetadata[key] = &PartitionMetadata{
			PartitionCount: topicCmd.Partitions,
			Leader:         assignedLeader,
			LeaderEpoch:    1,
			Idempotent:     topicCmd.Idempotent,
			Replicas:       replicas,
			ISR:            isrCopy,
		}
		util.Info("FSM: Assigned leader %s to partition %s (replicas=%v)", assignedLeader, key, replicas)
	}

	tm := f.tm
	f.mu.Unlock()

	if tm != nil {
		if err := tm.CreateTopic(topicCmd.Name, topicCmd.Partitions, topicCmd.Idempotent); err != nil {
			util.Error("FSM: Failed to create topic '%s' in local manager: %v", topicCmd.Name, err)
		}
	}
	util.Info("FSM: Created topic '%s' with %d partitions", topicCmd.Name, topicCmd.Partitions)

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

	f.mu.Lock()

	for key := range f.partitionMetadata {
		if idx := strings.LastIndex(key, "-"); idx != -1 && key[:idx] == payload.Topic {
			delete(f.partitionMetadata, key)
		}
	}

	tm := f.tm
	f.mu.Unlock()

	if tm != nil {
		tm.DeleteTopic(payload.Topic)
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
	f.partitionMetadata[key] = &metadata
	util.Debug("FSM: Updated partition metadata for %s", key)
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
		Type   string `json:"type"`
		Group  string `json:"group"`
		Member string `json:"member"`
		Topic  string `json:"topic"`
	}

	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("Failed to unmarshal group sync: %v", err)
		return err
	}

	if f.cd == nil {
		return fmt.Errorf("coordinator not ready")
	}

	switch cmd.Type {
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
		_ = f.cd.RemoveConsumer(cmd.Group, cmd.Member)
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
