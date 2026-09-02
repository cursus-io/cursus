package controller

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/types"
)

func transactionCoordinatorKey(txnID string) string {
	return "txn:" + txnID
}

func parseTxnProducerEpoch(args map[string]string, command string) (string, int64, string) {
	producerID := firstNonEmpty(args["producerId"], args["producer_id"])
	if producerID == "" {
		return "", 0, fmt.Sprintf("ERROR: missing_producer_id command=%s", command)
	}
	epoch, err := parseOptionalInt64(args["epoch"])
	if err != nil {
		return "", 0, fmt.Sprintf("ERROR: invalid_epoch reason=%q", err.Error())
	}
	return producerID, epoch, ""
}

func parseTxnOffsetPairs(command string) (map[int]uint64, error) {
	partsIndex := strings.LastIndex(command, " ")
	if partsIndex == -1 {
		return nil, fmt.Errorf("missing offset pairs")
	}
	partitionData := command[partsIndex+1:]
	if !strings.Contains(partitionData, ":") {
		return nil, fmt.Errorf("missing offset pairs")
	}
	pairs := strings.Split(partitionData, ",")
	offsets := make(map[int]uint64, len(pairs))
	for _, pair := range pairs {
		keyValue := strings.Split(pair, ":")
		if len(keyValue) != 2 || !strings.HasPrefix(keyValue[0], "P") {
			return nil, fmt.Errorf("invalid pair %s", pair)
		}
		partition, err := strconv.Atoi(strings.TrimPrefix(keyValue[0], "P"))
		if err != nil {
			return nil, err
		}
		offset, err := strconv.ParseUint(keyValue[1], 10, 64)
		if err != nil {
			return nil, err
		}
		offsets[partition] = offset
	}
	if len(offsets) == 0 {
		return nil, fmt.Errorf("no offsets supplied")
	}
	return offsets, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func parseRequiredPositiveUint64(value string) (uint64, error) {
	if value == "" {
		return 0, fmt.Errorf("missing seqNum")
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	if parsed == 0 {
		return 0, fmt.Errorf("seqNum must be greater than zero")
	}
	return parsed, nil
}

func parseOptionalInt64(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func transactionMarkerControlBytes(marker string, coordinatorEpoch int64) ([]byte, []byte, error) {
	if coordinatorEpoch < -(1<<31) || coordinatorEpoch > (1<<31)-1 {
		return nil, nil, fmt.Errorf("coordinator epoch out of int32 range: %d", coordinatorEpoch)
	}
	var markerType int16
	switch marker {
	case types.TransactionMarkerCommit:
		markerType = 0
	case types.TransactionMarkerAbort:
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
