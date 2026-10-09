package fsm

import (
	"fmt"
	"strconv"
)

// ISRQuarantineCommand removes one divergent replica from ISR under the exact
// leader, lifecycle, and committed-boundary fence that observed the gap.
type ISRQuarantineCommand struct {
	ReqID            string   `json:"req_id,omitempty"`
	Topic            string   `json:"topic"`
	Partition        int      `json:"partition"`
	BrokerID         string   `json:"broker_id"`
	Leader           string   `json:"leader"`
	LeaderEpoch      int      `json:"leader_epoch"`
	LifecycleEpoch   uint64   `json:"lifecycle_epoch"`
	CommittedHWM     uint64   `json:"committed_hwm"`
	ExpectedISR      []string `json:"expected_isr"`
	ExpectedReplicas []string `json:"expected_replicas"`
}

func (f *BrokerFSM) applyISRQuarantineCommand(jsonData string) interface{} {
	var command ISRQuarantineCommand
	if err := decodeStrictJSON([]byte(jsonData), &command); err != nil {
		return fmt.Errorf("decode ISR quarantine command: %w", err)
	}
	if command.Topic == "" || command.Partition < 0 || command.BrokerID == "" || command.Leader == "" {
		return fmt.Errorf("invalid ISR quarantine identity")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	key := command.Topic + "-" + strconv.Itoa(command.Partition)
	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return fmt.Errorf("partition metadata %s not found", key)
	}
	if metadata.Leader != command.Leader || metadata.LeaderEpoch != command.LeaderEpoch {
		return fmt.Errorf("stale leader fence for %s", key)
	}
	if metadata.LifecycleEpoch != command.LifecycleEpoch {
		return fmt.Errorf("stale topic lifecycle epoch for %s", key)
	}
	if !metadata.CommittedHWMKnown || metadata.CommittedHWM != command.CommittedHWM {
		return fmt.Errorf("stale committed HWM for %s", key)
	}
	if command.BrokerID == metadata.Leader {
		return fmt.Errorf("cannot quarantine partition leader %s", command.BrokerID)
	}
	if !containsString(metadata.Replicas, command.BrokerID) {
		return fmt.Errorf("broker %s is not a configured replica for %s", command.BrokerID, key)
	}
	for _, broker := range f.brokers {
		if broker != nil && broker.Status == "active" && broker.LifecycleProtocol < ReplicaGapRecoveryProtocolVersion {
			return fmt.Errorf("replica gap recovery requires broker protocol %d on every active broker", ReplicaGapRecoveryProtocolVersion)
		}
	}
	if containsString(metadata.RecoveryReplicas, command.BrokerID) && !containsString(metadata.ISR, command.BrokerID) {
		return nil
	}
	if !sameStringSet(metadata.Replicas, command.ExpectedReplicas) {
		return fmt.Errorf("stale replica assignment for %s", key)
	}
	if !sameStringSet(metadata.ISR, command.ExpectedISR) || !containsString(metadata.ISR, command.BrokerID) {
		return fmt.Errorf("stale ISR membership for %s", key)
	}
	if !containsString(metadata.RecoveryReplicas, command.BrokerID) {
		metadata.RecoveryReplicas = append(metadata.RecoveryReplicas, command.BrokerID)
	}
	metadata.ISR = removeString(metadata.ISR, command.BrokerID)
	return nil
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	members := make(map[string]struct{}, len(left))
	for _, value := range left {
		members[value] = struct{}{}
	}
	if len(members) != len(left) {
		return false
	}
	for _, value := range right {
		if _, ok := members[value]; !ok {
			return false
		}
	}
	return true
}
