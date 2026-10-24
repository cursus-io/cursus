package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

// Inbound admission is independent of the six outbound source captures. Future
// receiver owners must share this pool, including their local publication work.
var bootstrapInboundPermits = make(chan struct{}, 2)

type ConsumerMetadataRecoveryClient interface {
	BeginBootstrapTransfer(context.Context, string, int, fsm.BootstrapBeginRequest) (fsm.BootstrapBeginResponse, error)
	ReadBootstrapTransferChunk(context.Context, string, int, fsm.BootstrapChunkRequest, int64) (fsm.BootstrapChunkResponse, error)
	ReleaseBootstrapTransfer(context.Context, string, int, fsm.BootstrapReleaseRequest) (fsm.BootstrapReleaseResponse, error)
}

type consumerMetadataCacheEntry struct {
	request     fsm.BootstrapBeginRequest
	bounds      disk.DetachedRawBounds
	certificate string
	generation  string // first certificate of this adopted raw generation
	store       *topic.DetachedMetadataStore
}

type consumerMetadataRecoveryCache struct {
	ch                    *CommandHandler
	state                 *fsm.BrokerFSM
	incarnation, brokerID string
	client                ConsumerMetadataRecoveryClient
	root                  *disk.DetachedBootstrapRoot
	budget                *topic.DetachedMetadataBudget
	maxPartitions         int
	port                  int
	permits               chan struct{}
	ctx                   context.Context
	cancel                context.CancelFunc
	operation             *contextMutex
	mu                    sync.Mutex
	closed                bool
	owners, leases        sync.WaitGroup
	closeOnce             sync.Once
	closeErr, cleanupErr  error
	entries               map[int]*consumerMetadataCacheEntry
	// Instance seam runs with real adopted ownership and inbound admission held.
	constructStore func(context.Context, *disk.AdoptedRawStore, *topic.DetachedMetadataBudget) (*topic.DetachedMetadataStore, error)
}

func consumerMetadataPartitionLimit(cfg *config.Config) (int, error) {
	perTopic, total := cfg.MaxPartitionsPerTopic, cfg.MaxPartitions
	if perTopic <= 0 {
		perTopic = 1024
	}
	if total <= 0 {
		total = 100000
	}
	p := min(perTopic, total)
	if p > math.MaxInt-2 {
		return 0, fmt.Errorf("consumer metadata adopted-store limit overflow")
	}
	return p, nil
}

// ConfigureConsumerMetadataRecovery may precede registration and topology.
// Authority is checked dynamically for every stage, against this process ID.
func (ch *CommandHandler) ConfigureConsumerMetadataRecovery(ctx context.Context, incarnation string, client ConsumerMetadataRecoveryClient) error {
	if ch == nil || ctx == nil || incarnation == "" || client == nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	v := reflect.ValueOf(client)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		if v.IsNil() {
			return fsm.ErrBootstrapTransferUnavailable
		}
	}
	ch.consumerRecoveryInitMu.Lock()
	defer ch.consumerRecoveryInitMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	ch.bootstrapMu.Lock()
	closed := ch.closed
	ch.bootstrapMu.Unlock()
	if closed || ch.consumerRecoveryCache != nil || ch.Config == nil || ch.Cluster == nil || ch.Cluster.Router == nil || ch.Cluster.RaftManager == nil {
		return fsm.ErrBootstrapTransferUnavailable
	}
	state, brokerID := ch.Cluster.RaftManager.GetFSM(), ch.Cluster.Router.BrokerID()
	if state == nil || brokerID == "" {
		return fsm.ErrBootstrapTransferUnavailable
	}
	p, err := consumerMetadataPartitionLimit(ch.Config)
	if err != nil {
		return err
	}
	limits, err := disk.BootstrapLimitsFromConfig(ch.Config)
	if err != nil {
		return err
	}
	root, err := disk.NewDetachedBootstrapRoot(filepath.Join(ch.Config.LogDir, "@recovery", "consumer-metadata"), limits, ch.Config.DiskMinFreeBytes, ch.Config.DiskMinFreePercent, p+2)
	if err != nil {
		return err
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	c := &consumerMetadataRecoveryCache{ch: ch, state: state, incarnation: incarnation, brokerID: brokerID, client: client, root: root, budget: topic.NewDetachedMetadataBudget(), maxPartitions: p, port: ch.Config.DiscoveryPort, permits: bootstrapInboundPermits, ctx: ownerCtx, cancel: cancel, operation: newContextMutex(), entries: make(map[int]*consumerMetadataCacheEntry)}
	c.constructStore = topic.NewDetachedMetadataStore
	ch.consumerRecoveryCache = c
	ch.SetConsumerMetadataRecoveryProvider(c.stage)
	if ch.Coordinator != nil {
		ch.Coordinator.SetConsumerCleanupProvider(ch.prepareConsumerCleanup)
	}
	// This one process-lifetime observer never owns a stage or a staged lease.
	go func() { <-ownerCtx.Done(); _ = c.close() }()
	return nil
}

func (c *consumerMetadataRecoveryCache) rememberCleanup(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	if c.cleanupErr == nil {
		c.cleanupErr = err
	}
	c.mu.Unlock()
}

func retireConsumerMetadataStore(store *topic.DetachedMetadataStore) error {
	err := store.Close()
	// Retirement succeeds while tracked leases pin the generation. Physical
	// deletion failures are distinct errors and must remain observable.
	if err == disk.ErrDetachedBootstrapPending {
		return nil
	}
	return err
}

func (c *consumerMetadataRecoveryCache) close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		c.owners.Wait()
		for p, entry := range c.entries {
			c.rememberCleanup(retireConsumerMetadataStore(entry.store))
			delete(c.entries, p)
		}
		// Readers never need the operation or admission mutex to release leases.
		c.leases.Wait()
		c.mu.Lock()
		c.closeErr = c.cleanupErr
		c.mu.Unlock()
		c.closeErr = errors.Join(c.closeErr, c.root.Close())
	})
	return c.closeErr
}

func (c *consumerMetadataRecoveryCache) stage(ctx context.Context, request ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
	if ctx == nil {
		return nil, fsm.ErrBootstrapTransferUnavailable
	}
	if len(request.Metadata) == 0 || len(request.Metadata) > c.maxPartitions || len(request.Partitions) == 0 || len(request.Partitions) > len(request.Metadata) {
		return nil, fsm.ErrBootstrapTransferLimit
	}
	c.mu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, fsm.ErrBootstrapTransferUnavailable
	}
	c.owners.Add(1)
	c.mu.Unlock()
	defer c.owners.Done()
	callerCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := c.operation.LockContext(ctx); err != nil {
		return nil, err
	}
	defer c.operation.Unlock()
	request.Metadata = cloneConsumerRecoveryMetadata(request.Metadata)
	request.Partitions = append([]int(nil), request.Partitions...)
	if err := c.validateVector(request); err != nil {
		return nil, err
	}
	requester, err := c.requester()
	if err != nil {
		return nil, err
	}
	for p, entry := range c.entries {
		if p >= len(request.Metadata) || request.Metadata[p].LifecycleEpoch != entry.request.LifecycleEpoch {
			delete(c.entries, p)
			if err := retireConsumerMetadataStore(entry.store); err != nil {
				c.rememberCleanup(err)
				return nil, err
			}
		}
	}
	for _, p := range request.Partitions {
		if err := c.stagePartition(ctx, requester, p, request.Metadata[p]); err != nil {
			return nil, err
		}
	}
	readCtx, cancelRead := context.WithCancel(callerCtx)
	stopRead := context.AfterFunc(c.ctx, cancelRead)
	staged := &stagedConsumerMetadata{ch: c.ch, ctx: readCtx, releaseContext: func() { stopRead(); cancelRead() }, count: len(request.Metadata), leases: make(map[int]*topic.DetachedMetadataLease), sources: make(map[int]ConsumerMetadataRecoverySource), historyIdentities: make(map[int]string)}
	for _, p := range request.Partitions {
		entry := c.entries[p]
		lease, err := entry.store.Acquire()
		if err != nil {
			c.rememberCleanup(staged.Close())
			return nil, err
		}
		staged.leases[p] = lease
		staged.historyIdentities[p] = entry.generation
		s := entry.request.Source
		staged.sources[p] = ConsumerMetadataRecoverySource{BrokerID: s.BrokerID, IncarnationID: s.IncarnationID, IncarnationEpoch: s.IncarnationEpoch}
	}
	c.mu.Lock()
	if c.closed || ctx.Err() != nil {
		c.mu.Unlock()
		c.rememberCleanup(staged.Close())
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fsm.ErrBootstrapTransferUnavailable
	}
	c.leases.Add(1)
	staged.done = func(err error) { c.rememberCleanup(err); c.leases.Done() }
	c.mu.Unlock()
	return staged, nil
}

func (c *consumerMetadataRecoveryCache) validateVector(r ConsumerMetadataRecoveryStageRequest) error {
	if len(r.Metadata) == 0 || len(r.Metadata) > c.maxPartitions || len(r.Partitions) == 0 || len(r.Partitions) > len(r.Metadata) {
		return fsm.ErrBootstrapTransferLimit
	}
	for _, m := range r.Metadata {
		if m.PartitionCount != len(r.Metadata) || !m.CommittedHWMKnown || m.LifecycleEpoch == 0 || m.LifecycleEpoch != r.Metadata[0].LifecycleEpoch || m.Leader == "" || m.LeaderEpoch < 0 {
			return ErrConsumerRecoveryPending
		}
	}
	seen := make(map[int]bool, len(r.Partitions))
	for _, p := range r.Partitions {
		if p < 0 || p >= len(r.Metadata) || seen[p] {
			return ErrConsumerRecoveryPending
		}
		if containsReplica(r.Metadata[p].Replicas, c.brokerID) {
			return ErrConsumerRecoveryPending
		}
		seen[p] = true
	}
	return nil
}

func consumerRecoveryProcess(b *fsm.BrokerInfo) bool {
	return b != nil && b.ID != "" && b.Status == "active" && b.IncarnationID != "" && b.IncarnationEpoch > 0 && b.RecoveryProtocol >= fsm.BootstrapRecoveryProtocolVersion
}

func (c *consumerMetadataRecoveryCache) requester() (fsm.BootstrapProcessIdentity, error) {
	b := c.state.GetBroker(c.brokerID)
	if !consumerRecoveryProcess(b) || b.IncarnationID != c.incarnation {
		return fsm.BootstrapProcessIdentity{}, ErrConsumerRecoveryPending
	}
	return fsm.BootstrapProcessIdentity{BrokerID: b.ID, IncarnationID: b.IncarnationID, IncarnationEpoch: b.IncarnationEpoch}, nil
}
