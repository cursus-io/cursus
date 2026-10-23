package fsm

import (
	"encoding/json"
	"fmt"
)

type RetentionTimeActivation struct {
	Voters []string `json:"voters"`
}

func (f *BrokerFSM) RetentionTimeEnabled() bool { return f.retentionTimeFlag.Load() }

func retentionTimeCapable(broker *BrokerInfo) bool {
	return broker != nil && broker.IncarnationID != "" && broker.IncarnationEpoch > 0 &&
		broker.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && broker.LifecycleProtocol >= RetentionTimeProtocolVersion
}

func validateRetentionTimeBrokers(brokers map[string]*BrokerInfo) error {
	if len(brokers) == 0 {
		return fmt.Errorf("retention time activation requires registered brokers")
	}
	for id, broker := range brokers {
		if !retentionTimeCapable(broker) {
			return fmt.Errorf("retention time activation requires complete recovery capability on broker %s", id)
		}
	}
	return nil
}

func (f *BrokerFSM) applyRetentionTimeActivation(data string) interface{} {
	var command RetentionTimeActivation
	if err := json.Unmarshal([]byte(data), &command); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.retentionTimeActivated {
		return nil
	}
	if len(command.Voters) == 0 {
		return fmt.Errorf("retention time activation requires current voters")
	}
	if err := validateRetentionTimeBrokers(f.brokers); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(command.Voters))
	for _, id := range command.Voters {
		if _, duplicate := seen[id]; duplicate || !retentionTimeCapable(f.brokers[id]) {
			return fmt.Errorf("retention time activation has invalid voter %s", id)
		}
		seen[id] = struct{}{}
	}
	f.retentionTimeActivated = true
	f.retentionTimeFlag.Store(true)
	return nil
}
