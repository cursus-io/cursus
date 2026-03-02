package fsm

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/util"
)

type PartitionMetadata struct {
	Leader         string // todo
	Replicas       []string
	ISR            []string
	LeaderEpoch    int64
	PartitionCount int
}

func (f *BrokerFSM) applyRegisterCommand(jsonData string) error {
	var broker BrokerInfo
	if err := json.Unmarshal([]byte(jsonData), &broker); err != nil {
		util.Error("Failed to unmarshal broker registration: %v", err)
		return err
	}

	if broker.ID == "" || broker.Addr == "" {
		return fmt.Errorf("invalid broker registration: %+v", broker)
	}

	f.storeBroker(broker.ID, &broker)
	util.Info("Registered broker %s at %s", broker.ID, broker.Addr)
	return nil
}

func (f *BrokerFSM) applyDeregisterCommand(brokerID string) error {
	f.removeBroker(brokerID)
	util.Info("Deregistered broker %s", brokerID)
	return nil
}

func (f *BrokerFSM) applyTopicCommand(data string) interface{} {
	var topicCmd struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
		LeaderID   string `json:"leader_id"`
	}

	if err := json.Unmarshal([]byte(data), &topicCmd); err != nil {
		util.Error("Failed to unmarshal topic command: %v", err)
		return err
	}

	leader := topicCmd.LeaderID
	if leader == "" {
		leader = f.getCurrentRaftLeaderID()
	}

	f.mu.Lock()
	f.partitionMetadata[topicCmd.Name] = &PartitionMetadata{
		PartitionCount: topicCmd.Partitions,
		Leader:         leader,
		LeaderEpoch:    1,
	}
	f.mu.Unlock()

	if f.tm != nil {
		f.tm.CreateTopic(topicCmd.Name, topicCmd.Partitions)
		util.Info("FSM: Created topic '%s' with %d partitions", topicCmd.Name, topicCmd.Partitions)
	}

	return nil
}

func (f *BrokerFSM) applyPartitionCommand(data string) error {
	key, metadata, err := f.parsePartitionCommand(data)
	if err != nil {
		return err
	}

	f.storePartitionMetadata(key, metadata)
	util.Debug("Updated partition metadata for %s", key)
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

func (f *BrokerFSM) applyGroupSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Type   string `json:"type"` // JOIN or LEAVE
		Group  string `json:"group"`
		Member string `json:"member"`
		Topic  string `json:"topic"`
	}

	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("Failed to unmarshal group sync: %v", err)
		return err
	}

	if f.cd == nil {
		util.Warn("Coordinator not set in FSM, skipping group sync")
		return nil
	}

	switch cmd.Type {
	case "JOIN":
		if f.cd.GetGroup(cmd.Group) == nil {
			util.Info("FSM: Group '%s' not found, creating implicitly for topic '%s'", cmd.Group, cmd.Topic)

			var partitionCount int
			if t := f.tm.GetTopic(cmd.Topic); t != nil {
				partitionCount = len(t.Partitions)
			}

			if err := f.cd.RegisterGroup(cmd.Topic, cmd.Group, partitionCount); err != nil {
				util.Error("FSM: Failed to implicitly register group: %v", err)
				return err
			}
		}

		_, err := f.cd.AddConsumer(cmd.Group, cmd.Member)
		if err != nil {
			util.Error("FSM: Failed to sync JOIN for member %s: %v", cmd.Member, err)
		} else {
			util.Info("FSM: Synced JOIN group=%s member=%s", cmd.Group, cmd.Member)
		}
	case "LEAVE":
		err := f.cd.RemoveConsumer(cmd.Group, cmd.Member)
		if err != nil {
			util.Warn("FSM: LEAVE failed (potentially already removed): %v", err)
		} else {
			util.Info("FSM: Synced LEAVE group=%s member=%s", cmd.Group, cmd.Member)
		}
	}
	return nil
}

func (f *BrokerFSM) applyJoinGroupCommand(data string) interface{} {
	var info BrokerInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return fmt.Errorf("failed to unmarshal join info: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	info.LastSeen = time.Now()
	info.Status = "active"
	f.brokers[info.ID] = &info

	util.Info("FSM: Member %s added to registry", info.ID)
	return nil
}
