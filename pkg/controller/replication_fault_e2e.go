//go:build e2e_faults

package controller

import (
	"fmt"

	"github.com/cursus-io/cursus/internal/e2efaults"
	"github.com/cursus-io/cursus/pkg/types"
)

func injectedReplicaAppendSkip(topicName string, partition int, messages []types.Message) bool {
	if len(messages) == 0 {
		return false
	}
	return e2efaults.ConsumeReplicaAppendSkip(topicName, partition, messages[0].Offset)
}

func injectedReplicaCatchupError(topicName string) error {
	if e2efaults.ReplicaCatchupPaused(topicName) {
		return fmt.Errorf("injected e2e replica catch-up pause for topic %s", topicName)
	}
	return nil
}

func injectedReplicaAppendFailure(topicName string, partition int, messages []types.Message) bool {
	if len(messages) == 0 {
		return false
	}
	return e2efaults.ConsumeReplicaAppendFailure(topicName, partition, messages[0].Offset)
}
