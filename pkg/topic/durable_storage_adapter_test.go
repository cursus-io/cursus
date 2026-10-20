package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type durableStorageAdapter struct{ *disk.DiskHandler }

func TestDurableStorageAdapterPreservesCheckpointAndTransactionRecovery(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	d, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "open", TransactionalID: "transaction", TransactionState: types.TransactionStateOpen, Epoch: 1, ProducerID: "producer", SeqNum: 1}}))
	p := NewPartition(0, "orders", &durableStorageAdapter{d}, nil, cfg)
	t.Cleanup(func() { p.Close(); require.NoError(t, d.Close()) })
	require.Equal(t, hwmCheckpointPath(d, 0), p.hwmCheckpointPath)
	require.Equal(t, producerStateCheckpointPath(d, 0), p.producerStatePath)
	require.NotNil(t, p.hwmCheckpointCh)
	require.NotNil(t, p.producerStateCh)
	require.Len(t, p.txnOpenOffsets, 1, "wrapped storage must recover raw transaction visibility")
	require.NoError(t, p.RecoveryError())
}
