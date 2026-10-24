package fsm

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type metadataSourceStorage struct {
	*disk.DiskHandler
	afterAnchor func()
	export      func(context.Context) error
	anchorErr   error
	captures    int
}

func (s *metadataSourceStorage) CaptureContentGenerationContext(ctx context.Context) (disk.ContentGeneration, error) {
	s.captures++
	return s.DiskHandler.CaptureContentGenerationContext(ctx)
}
func (s *metadataSourceStorage) GetFirstOffsetContext(ctx context.Context) (uint64, error) {
	if s.anchorErr != nil {
		return 0, s.anchorErr
	}
	a, e := s.DiskHandler.GetFirstOffsetContext(ctx)
	if s.afterAnchor != nil {
		s.afterAnchor()
	}
	return a, e
}
func (s *metadataSourceStorage) ExportRetainedBootstrapContext(ctx context.Context, g disk.ContentGeneration, a, start, end uint64, f disk.BootstrapFence) (*disk.BootstrapExport, error) {
	if s.export != nil {
		if err := s.export(ctx); err != nil {
			return nil, err
		}
	}
	return s.DiskHandler.ExportRetainedBootstrapContext(ctx, g, a, start, end, f)
}

type metadataSourceProvider struct {
	*disk.DiskManager
	storage *metadataSourceStorage
}

func (p *metadataSourceProvider) GetHandlerWithPolicy(name string, id int, policy string, hours int, bytes int64) (types.StorageHandler, error) {
	h, e := p.DiskManager.GetHandlerWithPolicy(name, id, policy, hours, bytes)
	if e != nil {
		return nil, e
	}
	p.storage = &metadataSourceStorage{DiskHandler: h.(*disk.DiskHandler)}
	return p.storage, nil
}
func metadataSourceFSM(t *testing.T) (*BrokerFSM, *topic.Partition, *metadataSourceStorage, BootstrapBeginRequest) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = .1
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	provider := &metadataSourceProvider{DiskManager: dm}
	tm := topic.NewTopicManager(cfg, provider, nil)
	require.NoError(t, tm.CreateTopic(config.ConsumerOffsetsTopicName, 1, false, false))
	p, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 0, Key: "same", Payload: "zero"}, {Offset: 1, Key: "same", Payload: "one"}}, true))
	require.NoError(t, p.ApplyReplicaHWM(2))
	f := NewBrokerFSM(tm, nil)
	r := metadataBeginRequest()
	r.FixedEnd = 2
	def := tm.GetTopic(r.Topic).Definition()
	r.LifecycleEpoch = def.LifecycleEpoch
	f.topicState[r.Topic] = &def
	f.partitionMetadata[r.Topic+"-0"] = &PartitionMetadata{Leader: r.Leader, LeaderEpoch: r.LeaderEpoch, LifecycleEpoch: r.LifecycleEpoch, PartitionCount: 1, CommittedHWMKnown: true, CommittedHWM: 3, Replicas: []string{r.Source.BrokerID}, ISR: []string{r.Source.BrokerID}}
	for _, id := range []BootstrapProcessIdentity{r.Source, r.Requester} {
		f.brokers[id.BrokerID] = &BrokerInfo{ID: id.BrokerID, Status: "active", IncarnationID: id.IncarnationID, IncarnationEpoch: id.IncarnationEpoch, RecoveryProtocol: BootstrapRecoveryProtocolVersion}
	}
	return f, p, provider.storage, r
}
func publishMetadataTicket(t *testing.T, f *BrokerFSM, ticket *MetadataBootstrapSourceTicket) *MetadataBootstrapContinuity {
	t.Helper()
	var cert *MetadataBootstrapContinuity
	require.NoError(t, f.WithMetadataBootstrapSourceFence(context.Background(), ticket, func(scope *MetadataBootstrapPublication) error {
		var err error
		cert, err = scope.TryPublishCurrent(func() {})
		return err
	}))
	require.NotNil(t, cert)
	return cert
}
func TestMetadataBootstrapSourceAuthorizationBeforeCapture(t *testing.T) {
	for _, change := range []string{"requester inactive", "requester missing", "requester incarnation", "source incarnation", "source inactive", "source missing", "source nonISR", "source nonreplica", "source capability", "requester capability", "local identity", "topic", "count", "lifecycle", "unknown hwm", "low hwm"} {
		t.Run(change, func(t *testing.T) {
			f, _, s, r := metadataSourceFSM(t)
			local := r.Source
			switch change {
			case "requester inactive":
				f.brokers[r.Requester.BrokerID].Status = "inactive"
			case "requester missing":
				delete(f.brokers, r.Requester.BrokerID)
			case "requester incarnation":
				r.Requester.IncarnationEpoch++
			case "source incarnation":
				f.brokers[r.Source.BrokerID].IncarnationID = "new"
			case "source inactive":
				f.brokers[r.Source.BrokerID].Status = "inactive"
			case "source missing":
				delete(f.brokers, r.Source.BrokerID)
			case "source nonISR":
				f.partitionMetadata[r.Topic+"-0"].ISR = nil
			case "source nonreplica":
				f.partitionMetadata[r.Topic+"-0"].Replicas = nil
			case "source capability":
				f.brokers[r.Source.BrokerID].RecoveryProtocol = 1
			case "requester capability":
				f.brokers[r.Requester.BrokerID].RecoveryProtocol = 1
			case "local identity":
				local.BrokerID = "another"
			case "topic":
				r.Topic = "orders"
			case "count":
				f.partitionMetadata[r.Topic+"-0"].PartitionCount++
			case "lifecycle":
				r.LifecycleEpoch++
			case "unknown hwm":
				f.partitionMetadata[r.Topic+"-0"].CommittedHWMKnown = false
			case "low hwm":
				f.partitionMetadata[r.Topic+"-0"].CommittedHWM = 1
			}
			_, err := f.PrepareMetadataBootstrapSource(context.Background(), local, r, nil)
			require.Error(t, err)
			require.Zero(t, s.captures)
		})
	}
}
func TestMetadataBootstrapSourceFullSuffixAndSameEnd(t *testing.T) {
	f, p, s, r := metadataSourceFSM(t)
	ctx := context.Background()
	ticket, err := f.PrepareMetadataBootstrapSource(ctx, r.Source, r, nil)
	require.NoError(t, err)
	export, err := ticket.ExportContext(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, export.Close()) }()
	require.Equal(t, uint64(2), export.Manifest().EndOffset)
	require.Equal(t, int64(0), export.Manifest().Files[1].Size)
	cert := publishMetadataTicket(t, f, ticket)
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 2, Payload: "two"}}, true))
	require.NoError(t, p.ApplyReplicaHWM(3))
	require.NoError(t, s.RollSegmentAt(3))
	r.PriorCertificateID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	r.SuffixStart = 2
	r.FixedEnd = 3
	suffix, err := f.PrepareMetadataBootstrapSource(ctx, r.Source, r, cert)
	require.NoError(t, err)
	next, err := suffix.ExportContext(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, next.Close()) }()
	require.Equal(t, uint64(2), next.Manifest().StartOffset)
	cert = publishMetadataTicket(t, f, suffix)
	r.SuffixStart = 3
	empty, err := f.PrepareMetadataBootstrapSource(ctx, r.Source, r, cert)
	require.NoError(t, err)
	zero, err := empty.ExportContext(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, zero.Close()) }()
	require.Zero(t, zero.RetainedWork().CanonicalWriteBytes)
	publishMetadataTicket(t, f, empty)
}
func TestMetadataBootstrapSourceRejectsABAAndStaleFinal(t *testing.T) {
	for _, stage := range []string{"anchor", "after export", "leader", "requester", "count", "pointer", "lifecycle", "source incarnation", "source inactive", "source missing", "requester missing", "source capability", "requester capability", "source ISR", "closed", "recovery pending", "low local HWM"} {
		t.Run(stage, func(t *testing.T) {
			f, p, s, r := metadataSourceFSM(t)
			mutate := func() {
				require.NoError(t, s.TruncateTo(0))
				require.NoError(t, s.WriteBatchSync([]types.DiskMessage{{Topic: r.Topic, Offset: 0, Payload: "new"}, {Topic: r.Topic, Offset: 1, Payload: "one"}}))
			}
			if stage == "anchor" {
				s.afterAnchor = mutate
			}
			ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
			if stage == "anchor" {
				require.ErrorIs(t, err, ErrBootstrapTransferStaleFence)
				return
			}
			require.NoError(t, err)
			export, err := ticket.ExportContext(context.Background())
			require.NoError(t, err)
			defer func() { require.NoError(t, export.Close()) }()
			switch stage {
			case "after export":
				mutate()
			case "leader":
				f.partitionMetadata[r.Topic+"-0"].LeaderEpoch++
			case "requester":
				f.brokers[r.Requester.BrokerID].IncarnationEpoch++
			case "count":
				f.topicState[r.Topic].Partitions++
			case "pointer":
				f.tm.GetTopic(r.Topic).Partitions[0] = nil
			case "lifecycle":
				f.topicState[r.Topic].LifecycleEpoch++
			case "source incarnation":
				f.brokers[r.Source.BrokerID].IncarnationEpoch++
			case "source inactive":
				f.brokers[r.Source.BrokerID].Status = "inactive"
			case "source missing":
				delete(f.brokers, r.Source.BrokerID)
			case "requester missing":
				delete(f.brokers, r.Requester.BrokerID)
			case "source capability":
				f.brokers[r.Source.BrokerID].RecoveryProtocol = 1
			case "requester capability":
				f.brokers[r.Requester.BrokerID].RecoveryProtocol = 1
			case "source ISR":
				f.partitionMetadata[r.Topic+"-0"].ISR = nil
			case "closed":
				p.Close()
			case "recovery pending":
				p.MarkReconciliationPending(errors.New("test repair pending"))
			case "low local HWM":
				// Snapshot recovery lowers visibility under the partition lock;
				// direct field mutation races with the checkpoint worker.
				require.NoError(t, p.ReconcileSnapshotHWM(0))
			}
			called := false
			err = f.WithMetadataBootstrapSourceFence(context.Background(), ticket, func(scope *MetadataBootstrapPublication) error {
				_, e := scope.TryPublishCurrent(func() { called = true })
				return e
			})
			require.Error(t, err)
			require.False(t, called)
		})
	}
}

func TestMetadataBootstrapPublicationChecksOriginalTokenInsideScope(t *testing.T) {
	f, _, s, r := metadataSourceFSM(t)
	ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
	require.NoError(t, err)
	var cert *MetadataBootstrapContinuity
	err = f.WithMetadataBootstrapSourceFence(context.Background(), ticket, func(scope *MetadataBootstrapPublication) error {
		require.NoError(t, s.TruncateTo(0))
		var err error
		cert, err = scope.TryPublishCurrent(func() { t.Fatal("stale publication") })
		return err
	})
	require.ErrorIs(t, err, ErrBootstrapTransferStaleFence)
	require.Nil(t, cert)
}

func TestMetadataBootstrapPublicationScopeWaitsForInFlight(t *testing.T) {
	f, _, _, r := metadataSourceFSM(t)
	ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
	require.NoError(t, err)
	entered, release, outerReturned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	published := make(chan error, 1)
	go func() {
		err := f.WithMetadataBootstrapSourceFence(context.Background(), ticket, func(scope *MetadataBootstrapPublication) error {
			go func() { _, err := scope.TryPublishCurrent(func() { close(entered); <-release }); published <- err }()
			<-entered
			return nil
		})
		published <- err
		close(outerReturned)
	}()
	<-entered
	if f.transitionMu.TryLock() {
		f.transitionMu.Unlock()
		t.Fatal("FSM fence released during publication")
	}
	select {
	case <-outerReturned:
		t.Fatal("scope did not wait")
	default:
	}
	close(release)
	require.NoError(t, <-published)
	require.NoError(t, <-published)
	<-outerReturned
}

func TestMetadataBootstrapFullFallbackAndInvalidContinuities(t *testing.T) {
	f, _, _, r := metadataSourceFSM(t)
	ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
	require.NoError(t, err)
	cert := publishMetadataTicket(t, f, ticket)
	r.PriorCertificateID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	r.SuffixStart = 2
	full, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
	require.NoError(t, err)
	require.Zero(t, full.start)
	require.Equal(t, r, full.request)
	for _, field := range []string{"partition", "source", "anchor", "leader", "end", "generation"} {
		t.Run(field, func(t *testing.T) {
			bad := *cert
			switch field {
			case "partition":
				bad.partition = nil
			case "source":
				bad.source.IncarnationEpoch++
			case "anchor":
				bad.anchor++
			case "leader":
				bad.leaderEpoch++
			case "end":
				bad.end++
			case "generation":
				bad.generation = disk.ContentGeneration{}
			}
			_, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, &bad)
			require.ErrorIs(t, err, ErrBootstrapContinuityMiss)
		})
	}
	other := NewBrokerFSM(nil, nil)
	require.ErrorIs(t, other.WithMetadataBootstrapSourceFence(context.Background(), ticket, func(*MetadataBootstrapPublication) error { t.Fatal("foreign ticket"); return nil }), ErrBootstrapTransferStaleFence)
}

func TestMetadataBootstrapContextCancellation(t *testing.T) {
	f, _, _, r := metadataSourceFSM(t)
	ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
	require.NoError(t, err)
	for _, materialization := range []bool{false, true} {
		gate := &f.transitionMu
		if materialization {
			gate = &f.materializationMu
		}
		gate.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err = f.PrepareMetadataBootstrapSource(ctx, r.Source, r, nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, f.WithMetadataBootstrapSourceFence(ctx, ticket, func(*MetadataBootstrapPublication) error { return nil }), context.DeadlineExceeded)
		cancel()
		gate.Unlock()
	}
}

func TestMetadataBootstrapCompactionInvalidatesAndCorruptionPropagates(t *testing.T) {
	for _, mutation := range []string{"compaction", "corruption"} {
		t.Run(mutation, func(t *testing.T) {
			f, _, s, r := metadataSourceFSM(t)
			ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
			require.NoError(t, err)
			cert := publishMetadataTicket(t, f, ticket)
			if mutation == "compaction" {
				require.NoError(t, s.RollSegmentAt(2))
				result, err := s.EnforceCompaction()
				require.NoError(t, err)
				require.Positive(t, result.RecordsRemoved)
				_, err = ticket.ExportContext(context.Background())
				require.ErrorIs(t, err, ErrBootstrapTransferStaleFence)
				r.PriorCertificateID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
				r.SuffixStart = 2
				_, err = f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, cert)
				require.ErrorIs(t, err, ErrBootstrapContinuityMiss)
			} else {
				require.NoError(t, os.WriteFile(s.GetSegmentPath(0), []byte("corrupt frame"), 0600))
				_, err = ticket.ExportContext(context.Background())
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrBootstrapContinuityMiss)
			}
		})
	}
}
func TestMetadataBootstrapContinuityAndScope(t *testing.T) {
	f, _, s, r := metadataSourceFSM(t)
	ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
	require.NoError(t, err)
	var escaped *MetadataBootstrapPublication
	var cert *MetadataBootstrapContinuity
	require.NoError(t, f.WithMetadataBootstrapSourceFence(context.Background(), ticket, func(scope *MetadataBootstrapPublication) error {
		escaped = scope
		cert, err = scope.TryPublishCurrent(func() {})
		return err
	}))
	_, err = escaped.TryPublishCurrent(func() { t.Fatal("escaped publication") })
	require.ErrorIs(t, err, ErrBootstrapTransferStaleFence)
	r.PriorCertificateID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	r.SuffixStart = 2
	s.anchorErr = errors.New("read failure")
	_, err = f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, cert)
	require.ErrorIs(t, err, s.anchorErr)
	require.NotErrorIs(t, err, ErrBootstrapContinuityMiss)
	s.anchorErr = nil
	other, _, _, _ := metadataSourceFSM(t)
	_, err = other.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, cert)
	require.ErrorIs(t, err, ErrBootstrapContinuityMiss)
	require.NoError(t, s.TruncateTo(0))
	_, err = f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, cert)
	require.ErrorIs(t, err, ErrBootstrapContinuityMiss)
}
func TestMetadataBootstrapExportDoesNotHoldFSMGates(t *testing.T) {
	f, _, s, r := metadataSourceFSM(t)
	ticket, err := f.PrepareMetadataBootstrapSource(context.Background(), r.Source, r, nil)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	s.export = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		e, err := ticket.ExportContext(ctx)
		if e != nil {
			err = errors.Join(err, e.Close())
		}
		done <- err
	}()
	<-entered
	applied := make(chan interface{}, 1)
	go func() {
		applied <- f.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"other","addr":"localhost:9000","status":"active","lifecycle_protocol":6}`)})
	}()
	select {
	case result := <-applied:
		require.Nil(t, result)
	case <-ctx.Done():
		t.Fatal("export blocks Apply")
	}
	close(release)
	require.NoError(t, <-done)
}
