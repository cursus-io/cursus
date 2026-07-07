package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPartition_RestoresPersistedHWMCheckpoint(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "committed"}))
	require.Equal(t, uint64(1), p.GetHWM())

	leaderBatch := []types.Message{{Payload: "uncommitted"}}
	require.NoError(t, p.EnqueueBatchLeader(leaderBatch))
	p.FlushDisk()
	require.Equal(t, uint64(1), p.GetHWM(), "leader append must not advance HWM before replication commit")
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)

	require.Equal(t, uint64(1), restarted.GetHWM())
	msgs, err := restarted.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "committed", msgs[0].Payload)
}
