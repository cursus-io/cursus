package fsm

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReplicaBootstrapReceiverFenceAndMinimum(t *testing.T) {
	for _, change := range []string{"growth", "source identity", "requester identity", "leader", "lifecycle", "definition", "local definition", "local partition", "unknown HWM", "low HWM", "requester ISR"} {
		t.Run(change, func(t *testing.T) {
			f, p, _, r := replicaBootstrapFSM(t)
			ticket, err := f.PrepareReplicaBootstrapReceiver(context.Background(), r.Requester, r)
			require.NoError(t, err)
			require.Same(t, p, ticket.Partition())
			m, d := f.partitionMetadata[r.Topic+"-0"], f.topicState[r.Topic]
			switch change {
			case "growth":
				m.CommittedHWM = 20
			case "source identity":
				f.brokers[r.Source.BrokerID].IncarnationEpoch++
			case "requester identity":
				f.brokers[r.Requester.BrokerID].IncarnationEpoch++
			case "leader":
				m.LeaderEpoch++
			case "lifecycle":
				m.LifecycleEpoch++
			case "definition":
				d.Policy.ReadACL[0] = "new"
			case "local definition":
				def := f.tm.GetTopic(r.Topic).Definition()
				def.Policy.WriteACL = []string{"new"}
				require.NoError(t, f.tm.ApplyDefinition(def))
			case "local partition":
				f.tm.GetTopic(r.Topic).Partitions[0] = nil
				defer func() { f.tm.GetTopic(r.Topic).Partitions[0] = p }()
			case "unknown HWM":
				m.CommittedHWMKnown = false
			case "low HWM":
				m.CommittedHWM = 0
			case "requester ISR":
				m.ISR = append(m.ISR, r.Requester.BrokerID)
			}
			called := false
			err = f.WithReplicaBootstrapInstallFence(context.Background(), func() error {
				if err := f.ValidateReplicaBootstrapReceiverPrelocked(ticket, 1); err != nil {
					return err
				}
				// This also checks that callback work is outside f.mu.
				require.NotNil(t, f.GetBroker(r.Requester.BrokerID))
				called = true
				return nil
			})
			if change == "growth" {
				require.NoError(t, err)
				require.True(t, called)
				require.EqualValues(t, 1, ticket.request.MinimumEnd)
			} else {
				require.Error(t, err)
				require.False(t, called)
			}
		})
	}
}

func TestReplicaBootstrapResumeIgnoresHistoricalAuthority(t *testing.T) {
	f, p, d, r, originalDiskManager := replicaBootstrapFSMWithManager(t)
	source, err := f.PrepareReplicaBootstrapSource(context.Background(), r.Source, r)
	require.NoError(t, err)
	capture := replicaCapture(t, source)
	stage, err := p.StageBootstrap(capture.Manifest())
	require.NoError(t, err)
	for _, file := range capture.Manifest().Files {
		data, err := capture.ReadChunk(file.ID, 0, 256<<10)
		require.NoError(t, err)
		require.NoError(t, stage.WriteChunk(file.ID, 0, data))
	}
	require.NoError(t, stage.PrepareContext(context.Background()))
	session, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, session.Install(stage, nil))
	session.Close()
	manifest, _ := p.BootstrapStatus()
	require.NotNil(t, manifest)
	// Reopen real durable intent with lower admission. Disk replay precedes the
	// new partition, whose pending boundary remains authoritative for resume.
	cfg := config.DefaultConfig()
	cfg.LogDir = filepath.Dir(filepath.Dir(d.BaseName))
	cfg.BootstrapMaxTransferBytes = 1
	cfg.BootstrapMaxTotalBytes = 1
	f.tm.Stop()
	originalDiskManager.CloseAllHandlers()
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { tm.Stop(); dm.CloseAllHandlers() })
	require.NoError(t, tm.RestoreTopics())
	reopened := NewBrokerFSM(tm, nil)
	reopened.topicState = f.topicState
	reopened.partitionMetadata = f.partitionMetadata
	reopened.brokers = f.brokers
	f = reopened
	p, err = tm.GetTopic(r.Topic).GetPartition(r.Partition)
	require.NoError(t, err)
	require.True(t, p.BootstrapPending())
	require.False(t, p.HWMKnown())
	handler, err := dm.GetHandler(r.Topic, r.Partition)
	require.NoError(t, err)
	d = handler.(*disk.DiskHandler)
	require.EqualValues(t, 1, d.BootstrapLimits().MaxTransferBytes)
	manifest, err = p.BootstrapStatus()
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.EqualValues(t, 1, manifest.EndOffset)
	delete(f.brokers, r.Source.BrokerID)
	f.brokers[r.Requester.BrokerID].IncarnationEpoch++
	f.brokers[r.Requester.BrokerID].RecoveryProtocol = 0
	m := f.partitionMetadata[r.Topic+"-0"]
	m.Leader = "replacement"
	m.LeaderEpoch++
	def := f.tm.GetTopic(r.Topic).Definition()
	def.Revision++
	def.Partitions++
	def.Policy.ReadACL = []string{"new-reader"}
	def.Policy.WriteACL = []string{"new-writer"}
	minimum := 2
	def.Policy.MinInSyncReplicas = &minimum
	require.NoError(t, f.tm.ApplyDefinition(def))
	f.topicState[r.Topic] = &def
	m.PartitionCount = def.Partitions
	limits := d.BootstrapLimits()
	limits.MaxTransferBytes = 1
	limits.MaxTotalBytes = 1
	require.NoError(t, d.SetBootstrapLimits(limits))
	require.Empty(t, f.BuildReplicaCatchupRequests(r.Requester.BrokerID))
	for _, change := range []string{"unknown", "low", "unassigned", "ISR", "lifecycle"} {
		old := *m
		switch change {
		case "unknown":
			m.CommittedHWMKnown = false
		case "low":
			m.CommittedHWM = 0
		case "unassigned":
			m.Replicas = nil
		case "ISR":
			m.ISR = append([]string(nil), r.Requester.BrokerID)
		case "lifecycle":
			m.LifecycleEpoch++
		}
		require.Error(t, f.ValidateReplicaBootstrapResumePrelocked(r.Requester.BrokerID, p, *manifest))
		*m = old
	}
	p.MarkReconciliationPending(errors.New("unrelated reconciliation"))
	require.NoError(t, f.WithReplicaBootstrapInstallFence(context.Background(), func() error {
		if err := f.ValidateReplicaBootstrapResumePrelocked(r.Requester.BrokerID, p, *manifest); err != nil {
			return err
		}
		s, err := p.BeginBootstrapSession()
		if err != nil {
			return err
		}
		defer s.Close()
		return s.Complete(func(*topic.PartitionReadView, []byte) error { return nil })
	}))
	require.False(t, p.BootstrapPending())
	require.ErrorContains(t, p.RecoveryError(), "unrelated reconciliation")
	require.NotContains(t, m.ISR, r.Requester.BrokerID)
}

func TestReplicaBootstrapReceiverExclusionPrecedesTailMutation(t *testing.T) {
	f, p, _, r := replicaBootstrapFSM(t)
	f.brokers[r.Source.BrokerID].Addr = "127.0.0.1:9000"
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 1, Payload: "uncommitted"}}, true))
	requests := f.BuildReplicaCatchupRequestsExcluding(r.Requester.BrokerID, func(name string, id int) bool { return name == r.Topic && id == r.Partition })
	require.Empty(t, requests)
	require.EqualValues(t, 2, p.NextOffset())
	_ = f.BuildReplicaCatchupRequests(r.Requester.BrokerID)
	require.EqualValues(t, 1, p.NextOffset())
}
