package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// TransactionControlMarkerPayload identifies the broker-generated control record.
const TransactionControlMarkerPayload = "__cursus_txn_control_marker__"

func TransactionMarkerControlBytes(marker string, coordinatorEpoch int64) ([]byte, []byte, error) {
	if coordinatorEpoch < -(1<<31) || coordinatorEpoch > (1<<31)-1 {
		return nil, nil, fmt.Errorf("coordinator epoch out of int32 range: %d", coordinatorEpoch)
	}
	var markerType int16
	switch marker {
	case TransactionMarkerCommit:
		markerType = 0
	case TransactionMarkerAbort:
		markerType = 1
	default:
		return nil, nil, fmt.Errorf("invalid transaction marker %q", marker)
	}
	key := make([]byte, 4)
	binary.BigEndian.PutUint16(key[0:2], 0)
	binary.BigEndian.PutUint16(key[2:4], uint16(markerType))
	value := bytes.Buffer{}
	if err := binary.Write(&value, binary.BigEndian, int16(0)); err != nil {
		return nil, nil, fmt.Errorf("encode transaction marker value version: %w", err)
	}
	epoch32 := int32(coordinatorEpoch) // #nosec G115 -- bounded to int32 range above.
	if err := binary.Write(&value, binary.BigEndian, epoch32); err != nil {
		return nil, nil, fmt.Errorf("encode transaction marker coordinator epoch: %w", err)
	}
	return key, value.Bytes(), nil
}
