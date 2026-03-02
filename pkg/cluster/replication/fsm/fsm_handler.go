package fsm

import "github.com/cursus-io/cursus/util"

func (f *BrokerFSM) storeBroker(id string, broker *BrokerInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.brokers[id] = broker
}

func (f *BrokerFSM) removeBroker(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.brokers, id)
}

func (f *BrokerFSM) storePartitionMetadata(key string, metadata *PartitionMetadata) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitionMetadata[key] = metadata
}

func (f *BrokerFSM) UpdatePartitionISR(key string, isr []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if meta, ok := f.partitionMetadata[key]; ok {
		meta.ISR = append([]string(nil), isr...)
		util.Debug("Updated ISR for %s: %v", key, isr)
	} else {
		util.Warn("Partition metadata not found for %s. ISR not updated.", key)
	}
}

func (f *BrokerFSM) RegisterNotifier(requestID string) chan interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan interface{}, 1)
	f.notifiers[requestID] = ch
	return ch
}

func (f *BrokerFSM) UnregisterNotifier(requestID string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.notifiers, requestID)
}

func (f *BrokerFSM) notify(requestID string, result interface{}) {
	f.mu.RLock()
	ch, ok := f.notifiers[requestID]
	f.mu.RUnlock()

	if ok {
		select {
		case ch <- result:
		default:
			util.Debug("notification skipped for %s (channel closed or full)", requestID)
		}
	}
}
