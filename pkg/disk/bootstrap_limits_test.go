package disk

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBootstrapLimitsFromBrokerConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.BootstrapMaxTransferBytes = 3 << 20
	cfg.BootstrapMaxTotalBytes = 9 << 20
	d, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	require.Equal(t, BootstrapLimits{MaxTransferBytes: 3 << 20, MaxTotalBytes: 9 << 20}, d.BootstrapLimits())
	cfg.BootstrapMaxTransferBytes = 0
	_, err = NewDiskHandler(cfg, "invalid", 0)
	require.Error(t, err)
}

func TestBootstrapLimitsAdmitNormalSegmentMetadataWithoutLargeBuffers(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	// Isolate byte accounting from this test host's physical free capacity. Stage
	// creates only two empty files; the declared 1 GiB is never allocated or read.
	d.headroom = nil
	m := BootstrapMetadata{Version: BootstrapVersion, TransferID: strings.Repeat("a", 32), StartOffset: 0, EndOffset: 1, Fence: bootstrapTestFence(1, 1), Files: []BootstrapFile{
		{ID: "records", Role: "canonical-log", Size: 1 << 30, SHA256: strings.Repeat("0", 64)},
		{ID: "catalog", Role: "snapshot-catalog", Size: 0, SHA256: strings.Repeat("0", 64)},
	}}
	size, err := validateBootstrapMetadata(m)
	require.NoError(t, err)
	require.Equal(t, int64(1<<30), size)
	r, err := d.StageBootstrap(m)
	require.NoError(t, err)
	for _, f := range m.Files {
		info, err := os.Stat(filepath.Join(r.transfer.dir, f.ID))
		require.NoError(t, err)
		require.Zero(t, info.Size())
	}
	require.Greater(t, r.transfer.byteOwner.reserved, int64(512<<20))
	require.NoError(t, r.Abort())
	require.NoError(t, d.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: 512 << 20, MaxTotalBytes: 1 << 30}))
	_, err = d.StageBootstrap(m)
	var limit *BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, int64(512<<20), limit.Limit)
	// The lower operational setting must not change on-disk format validity.
	_, err = validateBootstrapMetadata(m)
	require.NoError(t, err)
	require.NoError(t, d.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: 2 << 30, MaxTotalBytes: 4 << 30}))
	r, err = d.StageBootstrap(m)
	require.NoError(t, err)
	require.NoError(t, r.Abort())
}

func TestBootstrapAggregateLimitsAccountLargeReservationsWithoutAllocation(t *testing.T) {
	base := filepath.Join(t.TempDir(), "partition_1")
	limits := BootstrapLimits{MaxTransferBytes: 2 << 30, MaxTotalBytes: 3 << 30}
	first, err := newBootstrapTransfer(base, BootstrapMetadata{}, 2<<30, limits)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.close()) }()
	_, err = newBootstrapTransfer(base, BootstrapMetadata{}, 2<<30, limits)
	var limit *BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "total staging bytes", limit.Resource)
	require.Equal(t, int64(4<<30), limit.Requested)
	require.Equal(t, int64(3<<30), limit.Limit)
	require.NoError(t, first.close())
	second, err := newBootstrapTransfer(base, BootstrapMetadata{}, 2<<30, limits)
	require.NoError(t, err)
	require.NoError(t, second.close())
}

func TestBootstrapLimitsRejectInvalidAndOverflowingValues(t *testing.T) {
	d := &DiskHandler{}
	require.Equal(t, DefaultBootstrapLimits(), d.BootstrapLimits())
	for _, limits := range []BootstrapLimits{
		{}, {MaxTransferBytes: -1, MaxTotalBytes: 1}, {MaxTransferBytes: 1, MaxTotalBytes: -1},
		{MaxTransferBytes: 2, MaxTotalBytes: 1}, {MaxTransferBytes: math.MaxInt64, MaxTotalBytes: math.MaxInt64},
	} {
		require.Error(t, d.SetBootstrapLimits(limits))
		require.Equal(t, DefaultBootstrapLimits(), d.BootstrapLimits())
	}
	_, err := addBootstrapBytes(math.MaxInt64, 1)
	require.Error(t, err)
	_, err = addBootstrapBytes(-1, 1)
	require.Error(t, err)
	_, err = addBootstrapBytes(1, -1)
	require.Error(t, err)
	value, err := addBootstrapBytes(math.MaxInt64-1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), value)
	require.NoError(t, d.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: bootstrapStructuralMaxBytes, MaxTotalBytes: math.MaxInt64}))
	_, err = newBootstrapTransfer(filepath.Join(t.TempDir(), "partition_1"), BootstrapMetadata{}, math.MaxInt64, d.BootstrapLimits())
	require.Error(t, err)
}

func TestBootstrapLowerAdmissionLimitsDoNotStrandPendingRecovery(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: strings.Repeat("x", 1024)})
	require.NoError(t, err)
	e := bootstrapTestExport(t, source, 0, 1)
	target, cfg := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, e)
	// Admission already succeeded; a live limit reduction cannot revoke it.
	require.NoError(t, target.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: 1, MaxTotalBytes: 1}))
	require.NoError(t, r.Install())
	require.NoError(t, target.Close())
	var tx bootstrapTransaction
	require.NoError(t, readBootstrapEnvelope(bootstrapPendingPath(target.BaseName), &tx))
	require.NoError(t, validateBootstrapTransaction(target.BaseName, tx))
	target, err = NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, target.Close()) }()
	require.NoError(t, target.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: 1, MaxTotalBytes: 1}))
	retry, err := target.StageBootstrap(e.Manifest())
	require.NoError(t, err)
	require.NoError(t, retry.Install())
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
	retry, err = target.StageBootstrap(e.Manifest())
	require.NoError(t, err)
	require.NoError(t, retry.Install())
}
