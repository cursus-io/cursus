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

	proofMu             sync.Mutex
	emptyProofs         map[string]map[string]receivedEmptyISRProof
	emptyPending        map[string]fsm.ISREmptyRecoveryCommand
	emptyRecoveryActive map[string]bool

	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	done      chan struct{}
}

type receivedEmptyISRProof struct {
	proof      fsm.ISRCatchupProof
	receivedAt time.Time
}

// NewISRManager creates a new ISRManager. The provided ctx controls the lifetime
// of the background ISR-check goroutine started by Start().
func NewISRManager(ctx context.Context, brokerFSM *fsm.BrokerFSM, brokerID string, heartbeatTimeout time.Duration, applier CommandApplier, configuredMinISR ...int) *ISRManager {
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = defaultHeartbeatTimeout
	}

	lastSeen := make(map[string]time.Time)
	if brokerID != "" {
		lastSeen[brokerID] = time.Now()
	}

	childCtx, cancel := context.WithCancel(ctx)
	defaultMinISR := 1
	if len(configuredMinISR) > 0 && configuredMinISR[0] > 0 {
		defaultMinISR = configuredMinISR[0]
	}
	if brokerFSM != nil {
		brokerFSM.SetDefaultMinISR(defaultMinISR)
	}

	return &ISRManager{
		fsm:                 brokerFSM,
		brokerID:            brokerID,
		applier:             applier,
		lastSeen:            lastSeen,
		heartbeatTimeout:    heartbeatTimeout,
		emptyProofs:         make(map[string]map[string]receivedEmptyISRProof),
		emptyPending:        make(map[string]fsm.ISREmptyRecoveryCommand),
		emptyRecoveryActive: make(map[string]bool),
		ctx:                 childCtx,
		cancel:              cancel,
		done:                make(chan struct{}),
	}
}

func (i *ISRManager) Start() {
	i.startOnce.Do(func() {
		go func() {
			defer close(i.done)
			// Check ISR and Heartbeats more frequently
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				if i.ctx.Err() != nil {
					return
				}
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

// Stop cancels and joins the ISR owner, including when it has not started yet.
func (i *ISRManager) Stop() {
	if i.cancel != nil {
		i.cancel()
	}
	i.startOnce.Do(func() { close(i.done) })
	<-i.done
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

// IsBrokerAlive reports heartbeat liveness with a leadership grace period so
// a newly elected leader does not immediately deactivate peers before it has
// had a chance to observe their heartbeats.
func (i *ISRManager) IsBrokerAlive(brokerID string) bool {
	if brokerID == "" {
		return false
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.isBrokerAliveLocked(brokerID, time.Now())
}

func (i *ISRManager) isBrokerAliveLocked(brokerID string, now time.Time) bool {
	if brokerID == i.brokerID {
		return true
	}
	if !i.leaderSince.IsZero() && now.Sub(i.leaderSince) <= i.heartbeatTimeout {
		return true
	}
	lastSeen, ok := i.lastSeen[brokerID]
	return ok && now.Sub(lastSeen) <= i.heartbeatTimeout
}

func (i *ISRManager) BuildCatchupProofs() []fsm.ISRCatchupProof {
	if i.fsm == nil {
		return nil
	}
	return i.fsm.BuildISRCatchupProofs(i.brokerID)
}

func (i *ISRManager) SubmitCatchupProofs(nodeID string, proofs []fsm.ISRCatchupProof) error {
	for _, proof := range proofs {
		if proof.BrokerID != nodeID {
			recordISRProof(ISRProofOutcomeRejected, ISRProofReasonIdentityMismatch)
			return fmt.Errorf("ISR catch-up proof broker %q does not match authenticated heartbeat node %q", proof.BrokerID, nodeID)
		}
	}
	if i.applier == nil || !i.applier.IsLeader() {
		return nil
	}
	if i.fsm == nil {
		recordISRProof(ISRProofOutcomeRejected, ISRProofReasonFSMUnavailable)
		return fmt.Errorf("FSM is unavailable for ISR catch-up validation")
	}
	if err := i.retryPendingEmptyISRRecoveries(); err != nil {
		return err
	}
	if len(proofs) == 0 {
		return nil
	}
	requiredProofs := make([]fsm.ISRCatchupProof, 0, len(proofs))
	emptyKeys := make(map[string]struct{})
	for _, proof := range proofs {
		if isEmptyISRRecoveryProof(proof) {
			if err := i.fsm.ValidateEmptyISRRecoveryProof(proof); err != nil {
				recordISRProof(ISRProofOutcomeRejected, ISRProofReasonValidation)
				return err
			}
			key := emptyISRPartitionKey(proof)
			i.proofMu.Lock()
			if i.emptyProofs[key] == nil {
				i.emptyProofs[key] = make(map[string]receivedEmptyISRProof)
			}
			i.emptyProofs[key][proof.BrokerID] = receivedEmptyISRProof{proof: proof, receivedAt: time.Now()}
			i.proofMu.Unlock()
			emptyKeys[key] = struct{}{}
			continue
		}
		required, err := i.fsm.ValidateISRCatchupProof(proof)
		if err != nil {
			recordISRProof(ISRProofOutcomeRejected, ISRProofReasonValidation)
			return err
		}
		if required {
			requiredProofs = append(requiredProofs, proof)
		} else {
			recordISRProof(ISRProofOutcomeAccepted, ISRProofReasonAlreadyInISR)
		}
	}
	for key := range emptyKeys {
		if err := i.tryEmptyISRRecovery(key); err != nil {
			return err
		}
	}
	for _, proof := range requiredProofs {
		data, err := json.Marshal(proof)
		if err != nil {
			recordISRProof(ISRProofOutcomeRejected, ISRProofReasonEncoding)
			return fmt.Errorf("marshal ISR catch-up proof: %w", err)
		}
		if err := i.applier.ApplyCommand("ISR_CATCHUP", data); err != nil {
			recordISRProof(ISRProofOutcomeRejected, ISRProofReasonApply)
			return fmt.Errorf("apply ISR catch-up proof for %s-%d: %w", proof.Topic, proof.Partition, err)
		}
		recordISRProof(ISRProofOutcomeAccepted, ISRProofReasonApplied)
	}
	return nil
}

func isEmptyISRRecoveryProof(proof fsm.ISRCatchupProof) bool {
	return proof.LineagePrefix != nil || proof.RetainedPrefix != nil
}

func emptyISRPartitionKey(proof fsm.ISRCatchupProof) string {
	return proof.Topic + "-" + strconv.Itoa(proof.Partition)
}

func (i *ISRManager) retryPendingEmptyISRRecoveries() error {
	i.proofMu.Lock()
	keys := make([]string, 0, len(i.emptyPending))
	for key := range i.emptyPending {
		keys = append(keys, key)
	}
	i.proofMu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		if err := i.tryEmptyISRRecovery(key); err != nil {
			return err
		}
	}
	return nil
}

func (i *ISRManager) tryEmptyISRRecovery(key string) error {
	if i.applier == nil || !i.applier.IsLeader() {
		return nil
	}
	i.proofMu.Lock()
	if i.emptyRecoveryActive[key] {
		i.proofMu.Unlock()
		return nil
	}
	command, pending := i.emptyPending[key]
	var proofs []fsm.ISRCatchupProof
	if !pending {
		now := time.Now()
		for brokerID, received := range i.emptyProofs[key] {
			if now.Sub(received.receivedAt) > i.heartbeatTimeout {
				delete(i.emptyProofs[key], brokerID)
				continue
			}
			proofs = append(proofs, received.proof)
		}
	}
	i.emptyRecoveryActive[key] = true
	i.proofMu.Unlock()

	if !pending {
		var ready bool
		var err error
		command, ready, err = i.fsm.BuildISREmptyRecoveryCommand(proofs)
		if err != nil || !ready {
			i.proofMu.Lock()
			delete(i.emptyRecoveryActive, key)
			i.proofMu.Unlock()
			return err
		}
		i.proofMu.Lock()
		i.emptyPending[key] = command
		i.proofMu.Unlock()
	}
	data, err := json.Marshal(command)
	if err == nil {
		err = i.applier.ApplyCommand("ISR_EMPTY_RECOVER", data)
	}
	i.proofMu.Lock()
	delete(i.emptyRecoveryActive, key)
	if err == nil {
		delete(i.emptyPending, key)
		delete(i.emptyProofs, key)
	}
	i.proofMu.Unlock()
	if err != nil {
		return fmt.Errorf("apply empty ISR recovery for %s: %w", key, err)
	}
	return nil
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
	i.proofMu.Lock()
	i.emptyProofs = make(map[string]map[string]receivedEmptyISRProof)
	i.emptyPending = make(map[string]fsm.ISREmptyRecoveryCommand)
	i.emptyRecoveryActive = make(map[string]bool)
	i.proofMu.Unlock()

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
	now := time.Now()
	for _, replica := range metadata.ISR {
		if broker := i.fsm.GetBroker(replica); broker != nil && broker.Status != "active" {
			continue
		}
		if i.isBrokerAliveLocked(replica, now) {
			currentISR = append(currentISR, replica)
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
	i.proofMu.Lock()
	for key, proofs := range i.emptyProofs {
		for brokerID, received := range proofs {
			if now.Sub(received.receivedAt) > i.heartbeatTimeout {
				delete(proofs, brokerID)
			}
		}
		if len(proofs) == 0 {
			delete(i.emptyProofs, key)
		}
	}
	i.proofMu.Unlock()
}
