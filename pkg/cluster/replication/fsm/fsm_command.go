package fsm

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

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
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Idempotent bool   `json:"idempotent"`
	LeaderID   string `json:"leader_id"`
}

func (f *BrokerFSM) applyTopicCommand(jsonData string) interface{} {
	var topicCmd TopicCommand
	if err := json.Unmarshal([]byte(jsonData), &topicCmd); err != nil {
		util.Error("FSM: Failed to unmarshal topic command: %v", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

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

	for i := 0; i < topicCmd.Partitions; i++ {
		key := fmt.Sprintf("%s-%d", topicCmd.Name, i)

		assignedLeader := topicCmd.LeaderID
		if assignedLeader == "" {
			assignedLeader = brokers[i%len(brokers)]
		}

		replicasCopy := append([]string(nil), brokers...)
		isrCopy := append([]string(nil), brokers...)

		f.partitionMetadata[key] = &PartitionMetadata{
			PartitionCount: topicCmd.Partitions,
			Leader:         assignedLeader,
			LeaderEpoch:    1,
			Idempotent:     topicCmd.Idempotent,
			Replicas:       replicasCopy,
			ISR:            isrCopy,
		}
		util.Info("FSM: Assigned leader %s to partition %s", assignedLeader, key)
	}

	if f.tm != nil {
		f.tm.CreateTopic(topicCmd.Name, topicCmd.Partitions, topicCmd.Idempotent)
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
	defer f.mu.Unlock()

	prefix := payload.Topic + "-"
	for key := range f.partitionMetadata {
		if strings.HasPrefix(key, prefix) {
			delete(f.partitionMetadata, key)
		}
	}

	if f.tm != nil {
		f.tm.DeleteTopic(payload.Topic)
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

// applyJoinGroupCommand 복구
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

	info.LastSeen = time.Now()
	info.Status = "active"
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

// applyMessageCommand 메서드는 fsm_replication.go에 정의되어 있으므로 여기서는 중복 정의하지 않습니다.
