package fsm

import (
	"encoding/json"
	"fmt"
)

type ConsumerRoutingActivation struct {
	Voters []string `json:"voters"`
}

func (f *BrokerFSM) ConsumerMetadataRoutingEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.consumerMetadataRoutingActivated
}

func consumerRoutingCapable(broker *BrokerInfo) bool {
	return broker != nil && broker.IncarnationID != "" && broker.IncarnationEpoch > 0 &&
		broker.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && broker.LifecycleProtocol >= BrokerProtocolVersionCurrent
}

func validateConsumerRoutingBrokers(brokers map[string]*BrokerInfo) error {
	if len(brokers) == 0 {
		return fmt.Errorf("consumer routing activation requires registered brokers")
	}
	for id, broker := range brokers {
		if !consumerRoutingCapable(broker) {
			return fmt.Errorf("consumer routing activation requires complete recovery capability on broker %s", id)
		}
	}
	return nil
}

func (f *BrokerFSM) applyConsumerRoutingActivation(data string) interface{} {
	var command ConsumerRoutingActivation
	if err := json.Unmarshal([]byte(data), &command); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.consumerMetadataRoutingActivated {
		return nil
	}
	if len(command.Voters) == 0 {
		return fmt.Errorf("consumer routing activation requires current voters")
	}
	if err := validateConsumerRoutingBrokers(f.brokers); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(command.Voters))
	for _, id := range command.Voters {
		if _, duplicate := seen[id]; duplicate || !consumerRoutingCapable(f.brokers[id]) {
			return fmt.Errorf("consumer routing activation has invalid voter %s", id)
		}
		seen[id] = struct{}{}
	}
	f.consumerMetadataRoutingActivated = true
	return nil
}
