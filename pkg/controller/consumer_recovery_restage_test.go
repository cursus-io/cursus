package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type measuredRecoveryReader struct {
	*consumerRecoveryReader
	records, bytes int
}

func (r *measuredRecoveryReader) ReadCommittedConsumerMetadata(name string, p int, offset uint64, max int) ([]types.Message, error) {
	messages, err := r.consumerRecoveryReader.ReadCommittedConsumerMetadata(name, p, offset, max)
	for _, message := range messages {
		r.records++
		r.bytes += len(message.Payload)
	}
	return messages, err
}

func TestConsumerRecoveryRestagesLocalCommitsUnderExactFence(t *testing.T) {
	for _, mutation := range []string{"commit", "uncovered", "regression", "leader"} {
		t.Run(mutation, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			cfg.LogDir = t.TempDir()
			dm := disk.NewDiskManager(cfg)
			t.Cleanup(dm.CloseAllHandlers)
			tm := topic.NewTopicManager(cfg, dm, nil)
			t.Cleanup(tm.Stop)
			c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
			require.NoError(t, err)
			t.Cleanup(c.Stop)
			state := fsm.NewBrokerFSM(tm, c)
			registerRoutingBroker(t, state, "remote")
			installRoutingOffsetsTopology(t, state, "remote")
			h := newCoordinatorRoutingHandler("local", state, c)
			h.TopicManager = tm
			p, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
			require.NoError(t, err)
			records := stagedConsumerRecords(t, "workers")
			records[1].Offset = 1
			require.NoError(t, p.ReplicaAppend(records[:1]))
			require.NoError(t, p.ApplyReplicaHWM(1))
			p.FlushDisk()
			setMetadata := func(end uint64, changeLeader bool) {
				m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
				m.CommittedHWM = end
				if changeLeader {
					m.LeaderEpoch++
				}
				data, err := json.Marshal(m)
				require.NoError(t, err)
				require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-0:"), data...)}))
			}
			setMetadata(1, false)
			metadata, err := state.ConsumerMetadataRecoverySnapshot()
			require.NoError(t, err)
			plan, err := h.prepareConsumerRecoveryPlan(context.Background(), metadata, -1, false)
			require.NoError(t, err)
			defer plan.close()
			// A remote broker commits after preflight but before the final fence.
			require.NoError(t, p.ReplicaAppend(records[1:]))
			require.NoError(t, p.ApplyReplicaHWM(2))
			p.FlushDisk()
			end := uint64(2)
			if mutation == "uncovered" {
				end = 3
			}
			setMetadata(end, mutation == "leader")
			release, err := c.BeginConsumerMetadataRecovery(context.Background())
			require.NoError(t, err)
			defer release()
			var measured *measuredRecoveryReader
			err = state.WithConsumerMetadataRecoveryFence(func(current []fsm.PartitionMetadata) error {
				if mutation == "regression" {
					// Ordinary FSM apply rejects regressions earlier. Exercise
					// the final validator's independent defense as well.
					current[0].CommittedHWM = 0
				}
				if err := h.validateConsumerRecoveryPlan(plan, current); err != nil {
					return err
				}
				require.Equal(t, current, plan.metadata, "the final vector remains exact")
				return h.withConsumerRecoveryPlan(context.Background(), plan, -1, nil, func(reader *consumerRecoveryReader) error {
					measured = &measuredRecoveryReader{consumerRecoveryReader: reader}
					prepared, err := c.PrepareFullConsumerMetadataReplay(measured)
					if err != nil {
						return err
					}
					if err := c.ApplyFullConsumerMetadataReplay(prepared); err != nil {
						return err
					}
					return c.CompleteConsumerRecovery(c.ConsumerRecoveryRequirement().RequiredGeneration)
				})
			})
			release()
			if mutation != "commit" {
				require.ErrorIs(t, err, ErrConsumerRecoveryPending)
				require.True(t, c.ConsumerRecoveryRequirement().Pending())
				require.Empty(t, c.ExportState())
				return
			}
			require.NoError(t, err)
			require.NoError(t, c.RecoveryReadinessError())
			offset, found := c.GetOffset("workers", "orders", 0)
			require.True(t, found)
			require.Equal(t, uint64(37), offset, "the commit after preflight must not be omitted")
			require.Equal(t, 2, measured.records)
			require.Equal(t, len(records[0].Payload)+len(records[1].Payload), measured.bytes)
			t.Logf("replayed records=%d payload_bytes=%d final_end=%d", measured.records, measured.bytes, plan.ends[0])
		})
	}
}
