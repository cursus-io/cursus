package fsm

import "github.com/cursus-io/cursus/pkg/topic"

func (f *BrokerFSM) SupportsTruncateReceipts() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.supportsTruncateReceiptsLocked()
}

func (f *BrokerFSM) supportsTruncateReceiptsLocked() bool {
	for _, broker := range f.brokers {
		if broker.Status == "active" && broker.LifecycleProtocol < TruncateReceiptProtocolVersion {
			return false
		}
	}
	return true
}

func (f *BrokerFSM) truncateReplayResult(d topic.Definition) topic.TruncateResult {
	f.mu.RLock()
	_, pending := f.topicMaterialization[d.Name]
	f.mu.RUnlock()
	if f.tm == nil || f.tm.IsTruncationPending(d.Name) || f.tm.GetTopic(d.Name) == nil {
		pending = true
	}
	return topic.TruncateResult{Topic: d.Name, Truncated: true, Definition: d, Replayed: true, CleanupPending: pending}
}
