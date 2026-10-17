package controller

import (
	"fmt"
	"strconv"

	"github.com/cursus-io/cursus/pkg/types"
)

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
	return types.TransactionMarkerControlBytes(marker, coordinatorEpoch)
}
