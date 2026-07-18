package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/util"
)

const defaultHeartbeatTimeout = 3 * time.Second

type CommandApplier interface {
	ApplyCommand(prefix string, data []byte) error
	IsLeader() bool
}

type ISRManager struct {
	fsm              *fsm.BrokerFSM
	brokerID         string
	applier          CommandApplier
	mu               sync.RWMutex
	lastSeen         map[string]time.Time
	heartbeatTimeout time.Duration
	leaderSince      time.Time

	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
}

// NewISRManager creates a new ISRManager. The provided ctx controls the lifetime
// of the background ISR-check goroutine started by Start().
func NewISRManager(ctx context.Context, fsm *fsm.BrokerFSM, brokerID string, heartbeatTimeout time.Duration, applier CommandApplier) *ISRManager {
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = defaultHeartbeatTimeout
	}

	lastSeen := make(map[string]time.Time)
	if brokerID != "" {
		lastSeen[brokerID] = time.Now()
	}

	childCtx, cancel := context.WithCancel(ctx)

	return &ISRManager{
		fsm:              fsm,
		brokerID:         brokerID,
		applier:          applier,
		lastSeen:         lastSeen,
		heartbeatTimeout: heartbeatTimeout,
		ctx:              childCtx,
		cancel:           cancel,
	}
}

func (i *ISRManager) Start() {
	i.startOnce.Do(func() {
		go func() {
			// Check ISR and Heartbeats more frequently
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					i.UpdateHeartbeat(i.brokerID)
					i.refreshAllISRs()
					i.CleanStaleHeartbeats()
				case <-i.ctx.Done():
					return
				}
			}
		}()
	})
}

// Stop cancels the ISR manager's context, shutting down the background goroutine.
func (i *ISRManager) Stop() {
	if i.cancel != nil {
		i.cancel()
	}
}

func (i *ISRManager) refreshAllISRs() {
	if i.applier != nil && !i.applier.IsLeader() {
		return
	}

	partitionKeys := i.fsm.GetAllPartitionKeys()
	for _, key := range partitionKeys {
		idx := strings.LastIndex(key, "-")
		if idx == -1 {
			continue
		}
		topic := key[:idx]
		partition, err := strconv.Atoi(key[idx+1:])
		if err != nil {
			util.Warn("ISRManager: skipping invalid partition key: %s", key)
			continue
		}

		i.ComputeISR(topic, partition)
	}
}

func (i *ISRManager) UpdateHeartbeat(brokerID string) {
	if brokerID == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.lastSeen[brokerID] = time.Now()
}

func (i *ISRManager) SetLeader(isLeader bool) {
	i.mu.Lock()
	if isLeader {
		if i.leaderSince.IsZero() {
			i.leaderSince = time.Now()
			// Mark self as alive immediately
			i.lastSeen[i.brokerID] = time.Now()
			util.Info("ISRManager: Successfully became leader at %v", i.leaderSince)
		}
	} else {
		i.leaderSince = time.Time{}
	}
	i.mu.Unlock()

	if isLeader {
		// Force immediate refresh instead of waiting for ticker
		go i.refreshAllISRs()
	}
}

func (i *ISRManager) ComputeISR(topic string, partition int) []string {
	key := topic + "-" + strconv.Itoa(partition)
	var currentISR []string

	i.mu.RLock()
	metadata := i.fsm.GetPartitionMetadata(key)
	i.mu.RUnlock()

	if metadata == nil {
		return nil
	}

	// A heartbeat proves liveness, not log synchronization. Only existing ISR
	// members may remain in ISR; catch-up must explicitly re-admit a replica.
	i.mu.RLock()
	for _, replica := range metadata.ISR {
		if broker := i.fsm.GetBroker(replica); broker != nil && broker.Status != "active" {
			continue
		}
		if lastSeen, ok := i.lastSeen[replica]; ok {
			if time.Since(lastSeen) < i.heartbeatTimeout {
				currentISR = append(currentISR, replica)
			}
		}
	}
	i.mu.RUnlock()

	// If we are Raft leader, propose changes if needed
	if i.applier != nil && i.applier.IsLeader() {
		needsUpdate := false

		// 1. Check if ISR count changed
		if len(currentISR) != len(metadata.ISR) {
			needsUpdate = true
		} else {
			// Compare members
			isrMap := make(map[string]bool)
			for _, m := range metadata.ISR {
				isrMap[m] = true
			}
			for _, m := range currentISR {
				if !isrMap[m] {
					needsUpdate = true
					break
				}
			}
		}

		// 2. Check if leader is dead
		leaderAlive := false
		for _, m := range currentISR {
			if m == metadata.Leader {
				leaderAlive = true
				break
			}
		}

		if !leaderAlive || needsUpdate {
			newMetadata := *metadata
			newMetadata.ISR = currentISR

			if !leaderAlive && len(currentISR) > 0 {
				sort.Strings(currentISR)
				newMetadata.Leader = currentISR[0]
				newMetadata.LeaderEpoch++
				util.Info("ISRManager: Failover for %s: %s -> %s", key, metadata.Leader, newMetadata.Leader)
			} else if !leaderAlive && len(currentISR) == 0 {
				util.Warn("ISRManager: No clean leader candidate for %s; partition remains unavailable", key)
			}

			data, err := json.Marshal(newMetadata)
			if err != nil {
				util.Error("ISRManager: Failed to marshal metadata for %s: %v", key, err)
				return currentISR
			}
			// The FSM expects PARTITION:<topic>-<partition>:<json>
			// ApplyCommand(prefix, data) results in "prefix:data"
			payload := []byte(fmt.Sprintf("%s:%s", key, string(data)))
			if err := i.applier.ApplyCommand("PARTITION", payload); err != nil {
				util.Error("ISRManager: Failed to apply ISR update for %s: %v", key, err)
			}
		}
	}

	return currentISR
}

func (i *ISRManager) GetISR(topic string, partition int) []string {
	key := topic + "-" + strconv.Itoa(partition)
	metadata := i.fsm.GetPartitionMetadata(key)
	if metadata == nil {
		return nil
	}
	isr := make([]string, len(metadata.ISR))
	copy(isr, metadata.ISR)
	return isr
}

func (i *ISRManager) HasQuorum(topic string, partition int, minISR int) bool {
	isr := i.GetISR(topic, partition)
	return len(isr) >= minISR
}

func (i *ISRManager) CleanStaleHeartbeats() {
	i.mu.Lock()
	defer i.mu.Unlock()
	now := time.Now()
	for id, last := range i.lastSeen {
		if id != i.brokerID && now.Sub(last) > i.heartbeatTimeout {
			delete(i.lastSeen, id)
		}
	}
}
