package fsm

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type epochBoundaryStorage struct {
	types.StorageHandler
	read func(uint64, int) ([]types.Message, error)
}

func (s *epochBoundaryStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	if s.read != nil {
		return s.read(offset, max)
	}
	return s.StorageHandler.ReadMessages(offset, max)
}

type epochBoundaryProvider struct {
	*disk.DiskManager
	storage *epochBoundaryStorage
}

func (p *epochBoundaryProvider) GetHandler(name string, partition int) (types.StorageHandler, error) {
	return p.GetHandlerWithPolicy(name, partition, config.CleanupPolicyDelete, 0, 0)
}

func (p *epochBoundaryProvider) GetHandlerWithPolicy(name string, partition int, policy string, hours int, bytes int64) (types.StorageHandler, error) {
	handler, err := p.DiskManager.GetHandlerWithPolicy(name, partition, policy, hours, bytes)
	if err != nil {
		return nil, err
	}
	p.storage = &epochBoundaryStorage{StorageHandler: handler}
	return p.storage, nil
}

func TestFetchReplicaCatchupDoesNotFallbackOnReadOrFenceFailure(t *testing.T) {
	readFailure := errors.New("injected storage checksum failure")
	for _, tc := range []struct {
		name string
		want string
	}{
		{"scan read error", readFailure.Error()},
		{"scan empty range", "leader epoch scan stopped"},
		{"prefix read error", readFailure.Error()},
		{"prefix removed", "read source record before catch-up"},
		{"leader fence", "stale leader fence"},
		{"lifecycle fence", "stale topic lifecycle epoch"},
		{"committed HWM", "committed HWM regressed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.DiskMinFreeBytes = 1
			cfg.DiskMinFreePercent = 0.1
			provider := &epochBoundaryProvider{DiskManager: disk.NewDiskManager(cfg)}
			t.Cleanup(provider.CloseAllHandlers)
			tm := topic.NewTopicManager(cfg, provider, nil)
			require.NoError(t, tm.CreateTopic("orders", 1, false, false))
			partition, err := tm.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			t.Cleanup(partition.Close)
			for _, payload := range []string{"common", "source-one", "source-two"} {
				require.NoError(t, partition.EnqueueSync(types.Message{Payload: payload, LeaderEpoch: 4}))
			}
			partition.FlushDisk()
			before, err := partition.ReadMessages(0, 3)
			require.NoError(t, err)
			brokerFSM := NewBrokerFSM(tm, nil)
			registerActiveBroker(t, brokerFSM, "node-1")
			registerActiveBroker(t, brokerFSM, "node-2")
			definition := tm.GetTopic("orders").Definition()
			brokerFSM.topicState["orders"] = &definition
			brokerFSM.partitionMetadata["orders-0"] = &PartitionMetadata{
				Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch,
				CommittedHWM: 3, CommittedHWMKnown: true, PartitionCount: 1,
				Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1"},
			}
			digest, err := replicaRecordDigest(types.Message{Offset: 2, Payload: "divergent", LeaderEpoch: 4})
			require.NoError(t, err)
			request := ReplicaCatchupRequest{
				Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 3, CommittedHWM: 3,
				Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4,
				LifecycleEpoch: definition.LifecycleEpoch, MaxRecords: MaxReplicaCatchupRecords,
				PreviousLeaderEpoch: 4, PreviousRecordDigest: digest,
			}
			provider.storage.read = func(offset uint64, max int) ([]types.Message, error) {
				if tc.name == "scan read error" && offset == 0 || tc.name == "prefix read error" && offset == 2 {
					return nil, readFailure
				}
				if tc.name == "scan empty range" && offset == 0 || tc.name == "prefix removed" && offset == 2 {
					return nil, nil
				}
				return provider.storage.StorageHandler.ReadMessages(offset, max)
			}
			switch tc.name {
			case "leader fence":
				request.LeaderEpoch++
			case "lifecycle fence":
				request.LifecycleEpoch++
			case "committed HWM":
				request.CommittedHWM++
			}
			batch, err := brokerFSM.FetchReplicaCatchup(request)
			require.ErrorContains(t, err, tc.want)
			require.NotErrorIs(t, err, errReplicaEpochBoundaryUnavailable)
			if tc.name == "scan read error" {
				require.ErrorIs(t, err, readFailure)
			}
			require.Nil(t, batch.TruncateTo)
			require.Empty(t, batch.Messages)
			provider.storage.read = nil
			after, err := partition.ReadMessages(0, 3)
			require.NoError(t, err)
			require.Equal(t, before, after, "source committed records must remain unchanged")
			require.Equal(t, uint64(3), partition.GetHWM())
		})
	}
}
