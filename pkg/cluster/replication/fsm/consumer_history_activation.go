package fsm

import (
	"encoding/json"
	"fmt"
)

type ConsumerHistoryActivation struct {
	Voters []string `json:"voters"`
}

func (f *BrokerFSM) ConsumerHistoryEnabled() bool { return f.consumerHistoryFlag.Load() }

func consumerHistoryCapable(broker *BrokerInfo) bool {
	return broker != nil && broker.IncarnationID != "" && broker.IncarnationEpoch > 0 &&
		broker.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && broker.LifecycleProtocol >= ConsumerHistoryProtocolVersion
}

func validateConsumerHistoryBrokers(brokers map[string]*BrokerInfo) error {
	if len(brokers) == 0 {
		return fmt.Errorf("consumer history activation requires registered brokers")
	}
	for id, broker := range brokers {
		if !consumerHistoryCapable(broker) {
			return fmt.Errorf("consumer history activation requires complete recovery capability on broker %s", id)
		}
	}
	return nil
}

func (f *BrokerFSM) applyConsumerHistoryActivation(data string) interface{} {
	var command ConsumerHistoryActivation
	if err := json.Unmarshal([]byte(data), &command); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.consumerHistoryActivated {
		return nil
	}
	if len(command.Voters) == 0 {
		return fmt.Errorf("consumer history activation requires current voters")
	}
	if err := validateConsumerHistoryBrokers(f.brokers); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(command.Voters))
	for _, id := range command.Voters {
		if _, duplicate := seen[id]; duplicate || !consumerHistoryCapable(f.brokers[id]) {
			return fmt.Errorf("consumer history activation has invalid voter %s", id)
		}
		seen[id] = struct{}{}
	}
	f.consumerHistoryActivated = true
	f.consumerHistoryFlag.Store(true)
	return nil
}
