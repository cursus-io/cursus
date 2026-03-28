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

	if len(oldISR) != len(isr) {
		util.Debug("FSM: ISR updated for %s: %v -> %v", key, oldISR, isr)
	}
}
