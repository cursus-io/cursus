package fsm

import (
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func errorAckResponse(msg, producerID string, epoch int64) types.AckResponse {
	return types.AckResponse{
		Status:        "ERROR",
		ErrorMsg:      msg,
		ProducerID:    producerID,
		ProducerEpoch: epoch,
	}
}

func (f *BrokerFSM) UpdatePartitionISR(key string, isr []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	metadata, ok := f.partitionMetadata[key]
	if !ok {
		return
	}

	oldISR := metadata.ISR
	f.partitionMetadata[key].ISR = isr

	if isrMembershipChanged(oldISR, isr) {
		util.Debug("FSM: ISR updated for %s: %v -> %v", key, oldISR, isr)
	}
}

// isrMembershipChanged returns true if the two ISR slices differ in length or membership.
func isrMembershipChanged(a, b []string) bool {
	if len(a) != len(b) {
		return true
	}
	members := make(map[string]struct{}, len(a))
	for _, m := range a {
		members[m] = struct{}{}
	}
	for _, m := range b {
		if _, ok := members[m]; !ok {
			return true
		}
	}
	return false
}
