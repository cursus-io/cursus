package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/util"
)

func (f *BrokerFSM) applyOffsetSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Group     string `json:"group"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    uint64 `json:"offset"`
	}

	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("Failed to unmarshal offset sync: %v", err)
		return err
	}

	if f.cd == nil {
		err := fmt.Errorf("FSM: Coordinator is nil (skipping offset sync for Topic: %s)", cmd.Topic)
		util.Error("%v", err)
		return err
	}

	err := f.cd.ApplyOffsetUpdateFromFSM(cmd.Group, cmd.Topic,
		[]coordinator.OffsetItem{
			{
				Partition: cmd.Partition,
				Offset:    cmd.Offset,
			},
		})
	if err != nil {
		util.Error("FSM: Failed to sync offset: %v", err)
		return err
	}
	return nil
}

func (f *BrokerFSM) applyBatchOffsetSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Group   string                   `json:"group"`
		Topic   string                   `json:"topic"`
		Offsets []coordinator.OffsetItem `json:"offsets"`
	}

	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("Failed to unmarshal batch offset sync: %v", err)
		return err
	}

	if f.cd == nil {
		err := fmt.Errorf("FSM: Coordinator is nil (skipping batch offset update)")
		util.Error("%v", err)
		return err
	}

	err := f.cd.ApplyOffsetUpdateFromFSM(cmd.Group, cmd.Topic, cmd.Offsets)
	if err != nil {
		util.Error("FSM: Failed to update coordinator state: %v", err)
		return err
	}

	if len(cmd.Offsets) > 0 {
		first := cmd.Offsets[0]
		last := cmd.Offsets[len(cmd.Offsets)-1]

		util.Debug("FSM: Synced BATCH_OFFSET group=%s topic=%s range=[Partition %d:Offset %d ~ Partition %d:Offset %d] count=%d", cmd.Group, cmd.Topic, first.Partition, first.Offset, last.Partition, last.Offset, len(cmd.Offsets))
	}
	return nil
}
