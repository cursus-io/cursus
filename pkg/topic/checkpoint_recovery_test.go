package topic

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpointStrictPublicationSharesSavedImagesAndRetries(t *testing.T) {
	for _, kind := range []string{"hwm", "producer"} {
		for _, stage := range []string{"open", "write", "sync", "close", "rename", "dir"} {
			t.Run(kind+"/"+stage, func(t *testing.T) {
				p, probe := checkpointFixture(t)
				p.dh = &checkpointStorage{}
				p.checkpointDirectorySync = probe.syncDir
				change := func(n uint64) { p.HWM = n }
				save := func() error { return p.persistHWMCheckpointLocked(p.HWM, true) }
				ordinary := p.persistHWMCheckpoint
				path := p.hwmCheckpointPath
				if kind == "producer" {
					change = func(n uint64) { p.producerStateCoveredOffset = n }
					save = func() error { return p.persistProducerStateCheckpointLocked(true) }
					ordinary = p.persistProducerStateCheckpoint
					path = p.producerStatePath
				}
				change(1)
				require.NoError(t, save())
				original, err := os.ReadFile(path)
				require.NoError(t, err)
				before := probe.counts
				for i := 0; i < 100; i++ {
					require.NoError(t, save())
					require.NoError(t, ordinary())
				}
				require.Equal(t, before, probe.counts)
				change(2)
				probe.fail = stage
				require.ErrorContains(t, save(), "injected "+stage)
				// A failed directory barrier may have replaced the file already.
				// Returning to the previously saved value must still rewrite it.
				probe.fail = ""
				change(1)
				beforeRetry := probe.counts.writes
				require.NoError(t, save())
				require.Equal(t, beforeRetry+1, probe.counts.writes)
				restored, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, original, restored)
			})
		}
	}
}

func TestBootstrapRewritesRemovedCheckpointsThenIdleFlushIsClean(t *testing.T) {
	d, _ := bootstrapPartitionDisk(t, 0)
	p, probe := checkpointFixture(t)
	p.topic, p.id, p.dh = "orders", 0, d
	p.hwmCheckpointPath = hwmCheckpointPath(d, 0)
	p.producerStatePath = producerStateCheckpointPath(d, 0)
	p.checkpointDirectorySync = probe.syncDir
	p.FlushDisk()
	before := probe.counts
	receiver := partitionTransfer(t, d, 0, 0, nil)
	session, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	defer session.Close()
	require.NoError(t, session.Install(receiver, nil))
	require.Nil(t, p.hwmCheckpointSaved)
	require.Nil(t, p.producerCheckpointSaved)
	require.NoError(t, session.Complete(nil))
	session.Close()
	require.Equal(t, before.writes+2, probe.counts.writes)
	require.Equal(t, before.syncs+2, probe.counts.syncs)
	require.Equal(t, before.renames+2, probe.counts.renames)
	require.Equal(t, before.dirs+2, probe.counts.dirs)
	require.FileExists(t, p.hwmCheckpointPath)
	require.FileExists(t, p.producerStatePath)
	before = probe.counts
	for i := 0; i < 100; i++ {
		p.FlushDisk()
	}
	require.Equal(t, before, probe.counts, "completed recovery must share ordinary checkpoint caches")
	reopened := NewPartition(0, "orders", d, nil, nil)
	defer reopened.Close()
	require.NoError(t, reopened.RecoveryError())
	require.Equal(t, uint64(0), reopened.GetHWM())
}
