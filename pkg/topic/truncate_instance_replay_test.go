package topic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestHistoricalTruncateCannotResetRecreatedInstance(t *testing.T) {
	for _, operation := range []string{"truncate", "restore"} {
		t.Run(operation, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			dm := disk.NewDiskManager(cfg)
			m := NewTopicManager(cfg, dm, nil)
			t.Cleanup(func() { m.Stop(); dm.CloseAllHandlers() })
			d := DefaultDefinition("orders", cfg)
			d.Partitions, d.Idempotent, d.InstanceID = 1, true, "instance-recreated-5678"
			_, err := m.CreateTopicWithPatch(d, DefinitionPatch{})
			require.NoError(t, err)
			require.NoError(t, m.PublishToPartitionWithAck(d.Name, 0, &types.Message{Payload: "fresh", ProducerID: "p", Epoch: 1, SeqNum: 1}))
			before := m.GetTopic(d.Name).Definition()
			manifestPath := filepath.Join(cfg.LogDir, TopicMetadataFileName)
			manifest, err := os.ReadFile(manifestPath)
			require.NoError(t, err)
			old := before
			old.InstanceID = "instance-original-1234"
			old.Revision++
			old.LifecycleEpoch++
			if operation == "truncate" {
				_, err = m.ApplyTruncateDefinition(old)
			} else {
				err = m.RestoreDefinitions([]Definition{old})
			}
			require.Error(t, err, "historical identity must be rejected before mutation")
			require.False(t, m.IsTruncationPending(d.Name))
			require.Equal(t, before, m.GetTopic(d.Name).Definition())
			after, err := os.ReadFile(manifestPath)
			require.NoError(t, err)
			require.Equal(t, manifest, after)
			messages, err := m.ReadTopicPartition(d.Name, 0, 0, 10)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, "fresh", messages[0].Payload)
			// Producer dedup state also belongs to the new instance.
			require.NoError(t, m.PublishToPartitionWithAck(d.Name, 0, &types.Message{Payload: "fresh", ProducerID: "p", Epoch: 1, SeqNum: 1}))
			messages, err = m.ReadTopicPartition(d.Name, 0, 0, 10)
			require.NoError(t, err)
			require.Len(t, messages, 1)
		})
	}
}
