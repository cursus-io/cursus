package fsm

import (
	"encoding/json"
	"fmt"
)

type CooperativeConsumerActivation struct {
	Voters []string `json:"voters"`
}

func (f *BrokerFSM) CooperativeConsumerEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cooperativeConsumerActivated
}

func cooperativeConsumerCapable(broker *BrokerInfo) bool {
	return broker != nil && broker.IncarnationID != "" && broker.IncarnationEpoch > 0 &&
		broker.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && broker.LifecycleProtocol >= CooperativeConsumerProtocolVersion
}

func validateCooperativeConsumerBrokers(brokers map[string]*BrokerInfo) error {
	if len(brokers) == 0 {
		return fmt.Errorf("cooperative consumer activation requires registered brokers")
	}
	for id, broker := range brokers {
		if !cooperativeConsumerCapable(broker) {
			return fmt.Errorf("cooperative consumer activation requires complete recovery capability on broker %s", id)
		}
	}
	return nil
}

func (f *BrokerFSM) applyCooperativeConsumerActivation(data string) interface{} {
	var command CooperativeConsumerActivation
	if err := json.Unmarshal([]byte(data), &command); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cooperativeConsumerActivated {
		return nil
	}
	if len(command.Voters) == 0 {
		return fmt.Errorf("cooperative consumer activation requires current voters")
	}
	if err := validateCooperativeConsumerBrokers(f.brokers); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(command.Voters))
	for _, id := range command.Voters {
		if _, duplicate := seen[id]; duplicate || !cooperativeConsumerCapable(f.brokers[id]) {
			return fmt.Errorf("cooperative consumer activation has invalid voter %s", id)
		}
		seen[id] = struct{}{}
	}
	f.cooperativeConsumerActivated = true
	return nil
}
