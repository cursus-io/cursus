package fsm

import (
	"encoding/json"
	"fmt"
	"strings"

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

func (f *BrokerFSM) parsePartitionCommand(data string) (string, *PartitionMetadata, error) {
	startIdx := strings.Index(data, "{")
	if startIdx == -1 {
		return "", nil, fmt.Errorf("invalid PARTITION command: JSON metadata not found")
	}

	prefix := data[:startIdx]
	prefix = strings.TrimPrefix(prefix, "PARTITION:")
	key := strings.TrimSuffix(prefix, ":")

	if key == "" {
		return "", nil, fmt.Errorf("invalid PARTITION command: missing key")
	}

	var metadata PartitionMetadata
	dec := json.NewDecoder(strings.NewReader(data[startIdx:]))
	if err := dec.Decode(&metadata); err != nil {
		util.Error("Failed to unmarshal partition metadata for key %s: %v", key, err)
		return "", nil, err
	}

	return key, &metadata, nil
}
