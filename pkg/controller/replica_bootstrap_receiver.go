package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	clustercontroller "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

type replicaBootstrapKey struct {
	topic     string
	partition int
}
type replicaBootstrapAttempt struct {
	key        replicaBootstrapKey
	request    fsm.BootstrapBeginRequest
	address    string
	done       chan struct{}
	err        error // Published by closing done, after all cleanup and admission release.
	cancel     context.CancelFunc
	ticket     *fsm.ReplicaBootstrapReceiverTicket // Guarded by the registry mutex.
	installing bool
}

type replicaBootstrapRecovery struct {
	ch                    *CommandHandler
	state                 *fsm.BrokerFSM
	client                ConsumerMetadataRecoveryClient
	brokerID, incarnation string
	port                  int
	ctx                   context.Context
	cancel                context.CancelFunc
	timeout               time.Duration
	mu                    sync.Mutex
	closed                bool
	attempts              map[replicaBootstrapKey]*replicaBootstrapAttempt
	offsets               *contextMutex
	owners                sync.WaitGroup
	closeOnce             sync.Once
	cleanupErr            error
}

// ConfigureReplicaBootstrapRecovery registers a process-owned receiver. It does
// not advertise capability or start a scheduler; shutdown joins its owners before
// closing the metadata provider and derived-state dependencies.
func (ch *CommandHandler) ConfigureReplicaBootstrapRecovery(ctx context.Context, incarnation string, client ConsumerMetadataRecoveryClient) error {
	if ch == nil || ctx == nil || client == nil || incarnation == "" {
		return fsm.ErrBootstrapTransferUnavailable
	}
	v := reflect.ValueOf(client)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		if v.IsNil() {
			return fsm.ErrBootstrapTransferUnavailable
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ch.bootstrapMu.Lock()
	defer ch.bootstrapMu.Unlock()
	if ch.closed || ch.replicaBootstrap != nil || ch.Config == nil || ch.TopicManager == nil || ch.ESHandler == nil || ch.Cluster == nil || ch.Cluster.Router == nil || ch.Cluster.RaftManager == nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	state, id := ch.Cluster.RaftManager.GetFSM(), ch.Cluster.Router.BrokerID()
	if state == nil || id == "" {
		return fsm.ErrBootstrapTransferUnavailable
	}
	timeout := 7 * time.Minute
	if ch.recoveryBudget != nil {
		var err error
		timeout, err = ch.Config.RecoveryBootstrapTimeout()
		if err != nil {
			return err
		}
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	ch.replicaBootstrap = &replicaBootstrapRecovery{ch: ch, state: state, client: client, brokerID: id, incarnation: incarnation, port: ch.Config.DiscoveryPort, ctx: ownerCtx, cancel: cancel, timeout: timeout, attempts: make(map[replicaBootstrapKey]*replicaBootstrapAttempt), offsets: newContextMutex()}
	ch.Cluster.SetReplicaBootstrapRecovery(ch)
	return nil
}

func (ch *CommandHandler) replicaBootstrapOwner() *replicaBootstrapRecovery {
	if ch == nil {
		return nil
	}
	ch.bootstrapMu.Lock()
	defer ch.bootstrapMu.Unlock()
	return ch.replicaBootstrap
}

// CloseReplicaBootstrapRecovery must precede provider, partition and catalog
// shutdown. No controller/FSM gates are held while joining filesystem work.
func (ch *CommandHandler) CloseReplicaBootstrapRecovery() error {
	if r := ch.replicaBootstrapOwner(); r != nil {
		return r.close()
	}
	return nil
}

func (r *replicaBootstrapRecovery) close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		r.owners.Wait()
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cleanupErr
}

func (r *replicaBootstrapRecovery) rememberCleanup(err error) {
	if err == nil || errors.Is(err, disk.ErrBootstrapPending) {
		return
	}
	r.mu.Lock()
	if r.cleanupErr == nil {
		r.cleanupErr = err
	}
	r.mu.Unlock()
}

func (ch *CommandHandler) ReplicaBootstrapExcluded(name string, partition int) bool {
	r := ch.replicaBootstrapOwner()
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts[replicaBootstrapKey{name, partition}] != nil
}

// reserve never waits for an inbound slot and never creates a waiting goroutine.
// Offsets serialization precedes the shared pool, leaving a slot for its provider.
func (r *replicaBootstrapRecovery) reserve(key replicaBootstrapKey) (func(), bool) {
	if key.topic == config.ConsumerOffsetsTopicName {
		select {
		case <-r.offsets.token:
		default:
			return nil, false
		}
	}
	select {
	case bootstrapInboundPermits <- struct{}{}:
		return func() {
			<-bootstrapInboundPermits
			if key.topic == config.ConsumerOffsetsTopicName {
				r.offsets.Unlock()
			}
		}, true
	default:
		if key.topic == config.ConsumerOffsetsTopicName {
			r.offsets.Unlock()
		}
		return nil, false
	}
}

func (r *replicaBootstrapRecovery) launchLocked(a *replicaBootstrapAttempt, run func(context.Context) error) bool {
	if r.closed || r.ctx.Err() != nil || r.attempts[a.key] != nil {
		return false
	}
	release, ok := r.reserve(a.key)
	if !ok {
		return false
	}
	r.attempts[a.key] = a
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	a.cancel = cancel
	r.owners.Add(1)
	go func() {
		defer r.owners.Done()
		a.err = run(ctx)
		cancel()
		release()
		close(a.done)
	}()
	return true
}

// RecoverReplicaBootstrap accepts only an authenticated full-bootstrap response.
// Returning yields the scheduler; it never cancels the process-owned attempt.
func (ch *CommandHandler) RecoverReplicaBootstrap(ctx context.Context, request fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) error {
	if ctx == nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	if err := clustercontroller.ValidateReplicaCatchupBatch(request, batch); !errors.Is(err, fsm.ErrReplicaBootstrapRequired) {
		if err != nil {
			return err
		}
		return fsm.ErrBootstrapTransferUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r := ch.replicaBootstrapOwner()
	if r == nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	key := replicaBootstrapKey{request.Topic, request.Partition}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.ctx.Err() != nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	if a := r.attempts[key]; a != nil {
		return nil
	}
	if request.BrokerID != r.brokerID {
		return fsm.ErrBootstrapTransferStaleFence
	}
	requester, source := r.state.GetBroker(r.brokerID), r.state.GetBroker(request.SourceBroker)
	if !consumerRecoveryProcess(requester) || requester.IncarnationID != r.incarnation || !consumerRecoveryProcess(source) {
		return fsm.ErrBootstrapTransferStaleFence
	}
	id, err := fsm.NewBootstrapAttemptID()
	if err != nil {
		return err
	}
	a := &replicaBootstrapAttempt{key: key, address: source.Addr, done: make(chan struct{})}
	a.request = fsm.BootstrapBeginRequest{Version: fsm.BootstrapTransferVersion, Purpose: fsm.BootstrapPurposeReplica, Requester: fsm.BootstrapProcessIdentity{BrokerID: requester.ID, IncarnationID: requester.IncarnationID, IncarnationEpoch: requester.IncarnationEpoch}, Source: fsm.BootstrapProcessIdentity{BrokerID: source.ID, IncarnationID: source.IncarnationID, IncarnationEpoch: source.IncarnationEpoch}, Topic: request.Topic, Partition: request.Partition, Leader: request.Leader, LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch, AttemptID: id, MinimumEnd: batch.CommittedHWM}
	r.launchLocked(a, func(ctx context.Context) error { return r.receive(ctx, a) })
	return nil
}

// ResumePendingReplicaBootstraps runs before ordinary materialization/request
// building. The returned exclusions include completions consumed on this pass,
// requiring a later fresh request and witness before ISR admission.
func (ch *CommandHandler) ResumePendingReplicaBootstraps(ctx context.Context) (map[string]bool, error) {
	excluded := make(map[string]bool)
	if ctx == nil {
		return excluded, fsm.ErrBootstrapTransferUnavailable
	}
	r := ch.replicaBootstrapOwner()
	if r == nil {
		return excluded, nil
	}
	var result error
	r.mu.Lock()
	for key, a := range r.attempts {
		excluded[fmt.Sprintf("%s-%d", key.topic, key.partition)] = true
		select {
		case <-a.done:
			result = errors.Join(result, a.err)
			delete(r.attempts, key)
		default:
			if a.ticket != nil && !a.installing && !r.state.ReplicaBootstrapReceiverCurrent(a.ticket) {
				a.cancel()
			}
		}
	}
	r.mu.Unlock()
	names := ch.TopicManager.ListTopics()
	sort.SliceStable(names, func(i, j int) bool {
		if names[i] == config.ConsumerOffsetsTopicName {
			return names[j] != names[i]
		}
		if names[j] == config.ConsumerOffsetsTopicName {
			return false
		}
		return names[i] < names[j]
	})
	for _, name := range names {
		t := ch.TopicManager.GetTopic(name)
		if t == nil {
			continue
		}
		for id := 0; id < t.Definition().Partitions; id++ {
			if err := ctx.Err(); err != nil {
				return excluded, errors.Join(result, err)
			}
			p, err := t.GetPartition(id)
			if err != nil {
				result = errors.Join(result, err)
				continue
			}
			if !p.BootstrapPending() {
				continue
			}
			key, display := replicaBootstrapKey{name, id}, fmt.Sprintf("%s-%d", name, id)
			if excluded[display] {
				continue
			}
			excluded[display] = true
			m, err := p.BootstrapStatus()
			if m == nil {
				result = errors.Join(result, fmt.Errorf("%s: pending bootstrap manifest unavailable: %w", display, errors.Join(topic.ErrBootstrapPending, err)))
				continue
			}
			a := &replicaBootstrapAttempt{key: key, done: make(chan struct{})}
			r.mu.Lock()
			r.launchLocked(a, func(ctx context.Context) error { return r.install(ctx, p, *m, nil, nil, nil) })
			r.mu.Unlock()
		}
	}
	return excluded, result
}
