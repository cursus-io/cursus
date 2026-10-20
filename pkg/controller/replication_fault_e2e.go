//go:build e2e_faults

package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cursus-io/cursus/internal/e2efaults"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

// Hold one authenticated response before taking any metadata or partition gate.
// This lets a test move the leader while an older response is in flight.
func injectedReplicaResponseDelay(ctx context.Context, name string) error {
	if os.Getenv(e2efaults.RuntimeGateEnvironment) != "1" {
		return nil
	}
	if err := topic.ValidateName(name); err != nil {
		return err
	}
	path := filepath.Join(e2efaults.SentinelRoot, "replica-response-hold", name)
	if err := os.Rename(path, path+".active"); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := os.WriteFile(path+".entered", nil, 0o600); err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path + ".release"); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func injectedReplicaAppendSkip(topicName string, partition int, messages []types.Message) bool {
	if len(messages) == 0 {
		return false
	}
	return e2efaults.ConsumeReplicaAppendSkip(topicName, partition, messages[0].Offset)
}

// Exit after a durable incremental append but before publishing its HWM/proof.
func injectedReplicaCatchupCrash(name string, partition int) {
	if os.Getenv(e2efaults.RuntimeGateEnvironment) != "1" || topic.ValidateName(name) != nil || partition < 0 {
		return
	}
	path := filepath.Join(e2efaults.SentinelRoot, "replica-catchup-crash", fmt.Sprintf("%s-%d", name, partition))
	if os.Remove(path) == nil {
		os.Exit(88)
	}
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
