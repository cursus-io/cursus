package topic

import (
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestPartitionRecoveryStaleHWMCheckpoint distinguishes standalone durable-tail
// recovery from distributed commit authority and keeps open transactions hidden.
func TestPartitionRecoveryStaleHWMCheckpoint(t *testing.T) {
	for _, distributed := range []bool{false, true} {
		mode := "standalone"
		if distributed {
			mode = "distributed"
		}
		for _, checkpoint := range []string{`{"version":2,"hwm":1}`, "1"} {
			t.Run(mode+"/"+checkpoint, func(t *testing.T) {
				d, cfg := bootstrapPartitionDisk(t, 0)
				cfg.EnabledDistribution = distributed
				require.NoError(t, d.WriteBatchSync([]types.DiskMessage{
					{Topic: "orders", Offset: 0, Payload: "first"},
					{Topic: "orders", Offset: 1, Payload: "second"},
					{Topic: "orders", Offset: 2, Payload: "open", TransactionalID: "tx", TransactionState: types.TransactionStateOpen, ProducerID: "producer", Epoch: 1, SeqNum: 1},
				}))
				path := hwmCheckpointPath(d, 0)
				require.NoError(t, d.Close())
				// Model the last checkpoint surviving while newer synced records
				// survive a crash. No live partition can race this publication.
				require.NoError(t, os.WriteFile(path, []byte(checkpoint), 0600))
				reopened, err := disk.NewDiskHandler(cfg, "orders", 0)
				require.NoError(t, err)
				p := NewPartition(0, "orders", reopened, nil, cfg)
				t.Cleanup(func() { p.Close(); require.NoError(t, reopened.Close()) })
				require.Equal(t, uint64(3), p.NextOffset())
				if distributed {
					require.Equal(t, uint64(1), p.GetHWM(), "local durability cannot prove replication commit")
					require.Equal(t, checkpoint != "1", p.HWMKnown())
					return
				}
				require.Equal(t, uint64(3), p.GetHWM())
				require.True(t, p.HWMKnown())
				messages, err := p.ReadCommitted(0, 10)
				require.NoError(t, err)
				require.Len(t, messages, 2, "recovered HWM must not make an open transaction visible")
				require.Equal(t, "second", messages[1].Payload)
			})
		}
	}
}
