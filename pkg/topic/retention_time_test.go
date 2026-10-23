package topic

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func retentionPartition(t *testing.T, enabled bool) (*Partition, *disk.DiskHandler, *config.Config) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionTimeEnabled = enabled
	cfg.DiskMinFreeBytes = 0
	cfg.DiskMinFreePercent = 0
	cfg.SegmentRollTimeMS = 0
	cfg.RetentionCheckIntervalMS = 3600000
	cfg.CompactionCheckIntervalMS = 3600000
	d, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(func() { p.Close(); require.NoError(t, d.Close()) })
	return p, d, cfg
}

func TestRetentionTimeLeaderFollowerClockAndClientInput(t *testing.T) {
	p, d, _ := retentionPartition(t, true)
	now := time.Unix(1700000000, 0)
	p.retentionClock = func() time.Time { return now }
	messages := []types.Message{{Payload: "one", Timestamp: 1, AppendTimeMS: 1}, {Payload: "two", Timestamp: 1 << 62, AppendTimeMS: 1 << 62}}
	require.NoError(t, p.EnqueueBatchSync(messages))
	stored, err := d.ReadMessages(0, 2)
	require.NoError(t, err)
	for _, m := range stored {
		require.Equal(t, now.UnixMilli(), m.AppendTimeMS)
	}
	follower, fd, _ := retentionPartition(t, true)
	follower.retentionClock = func() time.Time { return now.Add(500 * time.Hour) }
	require.NoError(t, follower.ReplicaAppend(stored))
	recovered, err := fd.ReadMessages(0, 2)
	require.NoError(t, err)
	require.Equal(t, stored, recovered)
	// Promotion with bounded backward skew clamps to persisted predecessor.
	follower.retentionClock = func() time.Time { return now.Add(-30 * time.Second) }
	require.NoError(t, follower.EnqueueBatchLeader([]types.Message{{Payload: "promoted"}}))
	recovered, err = fd.ReadMessages(2, 1)
	require.NoError(t, err)
	require.Equal(t, now.UnixMilli(), recovered[0].AppendTimeMS)
	follower.retentionClock = func() time.Time { return now.Add(-2 * time.Minute) }
	require.ErrorContains(t, follower.EnqueueBatchLeader([]types.Message{{Payload: "reject"}}), "clock tolerance")
	legacy, ld, _ := retentionPartition(t, false)
	require.NoError(t, legacy.EnqueueBatchSync([]types.Message{{AppendTimeMS: now.UnixMilli()}}))
	read, err := ld.ReadMessages(0, 1)
	require.NoError(t, err)
	require.Zero(t, read[0].AppendTimeMS)
}

func TestRetentionTimeHWMAndTransactionGuard(t *testing.T) {
	p, d, cfg := retentionPartition(t, true)
	require.NoError(t, p.EnqueueBatchSync([]types.Message{{Payload: "committed"}}))
	require.NoError(t, d.RollSegmentAt(1))
	require.NoError(t, p.EnqueueBatchLeader([]types.Message{{Payload: "uncommitted"}}))
	require.NoError(t, d.RollSegmentAt(2))
	d.SetRetentionPolicy(-1, 1)
	d.EnforceRetention(cfg)
	require.Equal(t, uint64(1), d.GetFirstOffset())
	require.FileExists(t, d.GetSegmentPath(1))
	p.SetHWM(2)
	d.EnforceRetention(cfg)
	require.Equal(t, uint64(2), d.GetFirstOffset())
	require.NoError(t, p.EnqueueBatchSync([]types.Message{{Payload: "open", TransactionalID: "tx", TransactionState: types.TransactionStateOpen}}))
	require.NoError(t, d.RollSegmentAt(3))
	d.EnforceRetention(cfg)
	require.FileExists(t, d.GetSegmentPath(2))
}

func TestRetentionTimeCapabilityGate(t *testing.T) {
	p, d, _ := retentionPartition(t, true)
	p.distributed = true
	require.ErrorContains(t, p.EnqueueBatchLeader([]types.Message{{Payload: "blocked"}}), "capability")
	require.Zero(t, d.GetAbsoluteOffset())
	p.retentionTimeGate = func() bool { return true }
	require.NoError(t, p.EnqueueBatchLeader([]types.Message{{Payload: "enabled"}}))
	read, err := d.ReadMessages(0, 1)
	require.NoError(t, err)
	require.Positive(t, read[0].AppendTimeMS)
}

func TestRetentionTimeManifestBlocksDisableAndOldReader(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionTimeEnabled = true
	dm := disk.NewDiskManager(cfg)
	defer dm.CloseAllHandlers()
	store := newTopicMetadataStore(cfg, dm)
	require.NoError(t, store.Save(nil))
	old := *store
	old.retentionTime = false
	_, err := old.Load()
	require.ErrorContains(t, err, "must remain enabled")
	definitions, err := store.Load()
	require.NoError(t, err)
	require.Empty(t, definitions)
	data, err := os.ReadFile(store.path)
	require.NoError(t, err)
	// The previous release uses this strict shape and refuses unknown fields.
	var previous struct {
		Version int          `json:"version"`
		Topics  []Definition `json:"topics"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.ErrorContains(t, decoder.Decode(&previous), "retention_time")
}

func TestRetentionTimeTailRepairReseedsClock(t *testing.T) {
	p, d, _ := retentionPartition(t, true)
	now := time.Unix(1700000000, 0)
	p.retentionClock = func() time.Time { return now }
	require.NoError(t, p.EnqueueSync(types.Message{Payload: "committed"}))
	now = now.Add(time.Hour)
	require.NoError(t, p.EnqueueBatchLeader([]types.Message{{Payload: "uncommitted"}}))
	require.NoError(t, p.TruncateReplicaTail(1))
	now = now.Add(-time.Hour)
	require.NoError(t, p.EnqueueBatchLeader([]types.Message{{Payload: "replacement"}}))
	read, err := d.ReadMessages(0, 2)
	require.NoError(t, err)
	require.Len(t, read, 2)
	require.Equal(t, now.UnixMilli(), read[0].AppendTimeMS)
	require.Equal(t, now.UnixMilli(), read[1].AppendTimeMS)
}
