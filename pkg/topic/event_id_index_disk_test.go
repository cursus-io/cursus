package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// Prove the cache can be discarded on a real close/reopen: only the existing
// committed log/checkpoint survives, not a new identity database or sidecar.
func TestCommittedEventIDIndexDiskRestart(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	dh, err := disk.NewDiskHandler(cfg, "identity", 0)
	require.NoError(t, err)
	p := NewPartition(0, "identity", dh, nil, cfg)
	p.SetHWM(0)
	require.NoError(t, p.EnqueueBatchLeader([]types.Message{
		{EventID: "committed-1", Payload: "one"},
		{EventID: "committed-2", Payload: "two"},
	}))
	p.FlushDisk()
	require.NoError(t, p.ApplyReplicaHWM(2))
	requireEventID(t, p, "committed-1", true)
	p.Close()
	require.NoError(t, dh.Close())
	dh2, err := disk.NewDiskHandler(cfg, "identity", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, dh2.Close()) }()
	p2 := NewPartition(0, "identity", dh2, nil, cfg)
	defer p2.Close()
	require.Nil(t, p2.eventIDs.ids)
	requireEventID(t, p2, "committed-1", true)
	requireEventID(t, p2, "committed-2", true)
	requireEventID(t, p2, "absent", false)
	require.Equal(t, uint64(2), p2.NextOffset())
}
