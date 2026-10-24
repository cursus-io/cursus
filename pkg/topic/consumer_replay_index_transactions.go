package topic

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

// ConsumerReplayIndexTransactionVisible resolves immutable cached evidence in
// the current committed view without rescanning hidden disk frames. The caller
// must validate the cache's durable prefix identity before and after replay.
func (v *PartitionReadView) ConsumerReplayIndexTransactionVisible(m types.Message, end uint64) (bool, error) {
	if err := v.check(); err != nil {
		return false, err
	}
	if v.p.topic != config.ConsumerOffsetsTopicName || v.offline || v.snapshotCut != nil || end > v.CommittedEnd() || m.Offset < v.FirstOffset() || m.Offset >= end {
		return false, fmt.Errorf("transaction index requires an ordinary committed metadata cut")
	}
	if err := v.p.RecoveryError(); err != nil {
		return false, err
	}
	if err := v.p.hwmReadinessError(); err != nil {
		return false, err
	}
	v.p.txnMarkerMu.RLock()
	defer v.p.txnMarkerMu.RUnlock()
	return isReadCommittedVisible(m, end, v.p.txnMarkers, v.p.txnResolver), nil
}
