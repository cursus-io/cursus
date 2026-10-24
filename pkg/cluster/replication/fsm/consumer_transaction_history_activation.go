package fsm

import (
	"encoding/json"
	"fmt"
)

func (f *BrokerFSM) ConsumerTransactionHistoryEnabled() bool {
	return f.consumerTransactionHistoryFlag.Load()
}

func consumerTransactionHistoryCapable(broker *BrokerInfo) bool {
	return broker != nil && broker.IncarnationID != "" && broker.IncarnationEpoch > 0 &&
		broker.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && broker.LifecycleProtocol >= ConsumerTransactionHistoryProtocolVersion
}

func validateConsumerTransactionHistoryBrokers(brokers map[string]*BrokerInfo) error {
	if len(brokers) == 0 {
		return fmt.Errorf("consumer transaction history activation requires registered brokers")
	}
	for id, broker := range brokers {
		if !consumerTransactionHistoryCapable(broker) {
			return fmt.Errorf("consumer transaction history activation requires complete recovery capability on broker %s", id)
		}
	}
	return nil
}

func (f *BrokerFSM) applyConsumerTransactionHistoryActivation(data string) interface{} {
	var command ConsumerHistoryActivation
	if err := json.Unmarshal([]byte(data), &command); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.consumerHistoryActivated {
		return fmt.Errorf("consumer history activation required")
	}
	if f.consumerTransactionHistoryActivated {
		return nil
	}
	if len(command.Voters) == 0 {
		return fmt.Errorf("consumer transaction history activation requires current voters")
	}
	if err := validateConsumerTransactionHistoryBrokers(f.brokers); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(command.Voters))
	for _, id := range command.Voters {
		if _, duplicate := seen[id]; duplicate || !consumerTransactionHistoryCapable(f.brokers[id]) {
			return fmt.Errorf("consumer transaction history activation has invalid voter %s", id)
		}
		seen[id] = struct{}{}
	}
	f.consumerTransactionHistoryActivated = true
	f.consumerTransactionHistoryFlag.Store(true)
	return nil
}
