package topic

import (
	"errors"
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBootstrapInstallCommittedFloor(t *testing.T) {
	tests := []struct {
		name, checkpoint             string
		hwm, snapshot                uint64
		end                          uint64
		directory, temporary, accept bool
	}{
		{name: "memory", hwm: 4, end: 3},
		{name: "snapshot", snapshot: 4, end: 3},
		{name: "disk", checkpoint: `{"version":2,"hwm":4}`, end: 3},
		{name: "legacy floor", checkpoint: "4", end: 3},
		{name: "equal", hwm: 3, snapshot: 3, checkpoint: `{"version":2,"hwm":3}`, end: 3, accept: true},
		{name: "higher", hwm: 2, snapshot: 3, checkpoint: `{"version":2,"hwm":4}`, end: 5, accept: true},
		{name: "explicit zero", checkpoint: `{"version":2,"hwm":0}`, accept: true},
		{name: "legacy zero", checkpoint: "0", accept: true},
		{name: "legacy accepted", checkpoint: "2", end: 3, accept: true},
		{name: "absent", end: 3, accept: true},
		{name: "temporary ignored", temporary: true, end: 3, accept: true},
		{name: "missing hwm", checkpoint: `{"version":2}`, end: 3},
		{name: "null hwm", checkpoint: `{"version":2,"hwm":null}`, end: 3},
		{name: "missing version", checkpoint: `{"hwm":0}`, end: 3},
		{name: "null version", checkpoint: `{"version":null,"hwm":0}`, end: 3},
		{name: "unsupported version", checkpoint: `{"version":3,"hwm":0}`, end: 3},
		{name: "malformed version", checkpoint: `{"version":"2","hwm":0}`, end: 3},
		{name: "negative", checkpoint: `{"version":2,"hwm":-1}`, end: 3},
		{name: "overflow", checkpoint: `{"version":2,"hwm":18446744073709551616}`, end: 3},
		{name: "trailing", checkpoint: `{"version":2,"hwm":0} 1`, end: 3},
		{name: "malformed", checkpoint: `{`, end: 3},
		{name: "duplicate hwm", checkpoint: `{"version":2,"hwm":4,"hwm":0}`, end: 3},
		{name: "duplicate version", checkpoint: `{"version":3,"version":2,"hwm":0}`, end: 3},
		{name: "case alias", checkpoint: `{"version":2,"hwm":0,"HWM":4}`, end: 3},
		{name: "unknown field", checkpoint: `{"version":2,"hwm":0,"other":4}`, end: 3},
		{name: "malformed hwm", checkpoint: `{"version":2,"hwm":"0"}`, end: 3},
		{name: "negative legacy", checkpoint: "-1", end: 3},
		{name: "overflow legacy", checkpoint: "18446744073709551616", end: 3},
		{name: "empty checkpoint", checkpoint: " ", end: 3},
		{name: "directory read error", directory: true, end: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, cfg := bootstrapPartitionDisk(t, 0)
			require.NoError(t, d.WriteCompactedReplicaRange(0, 8, []types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "old"}}))
			cfg.EnabledDistribution = true
			p := NewPartition(0, "orders", d, nil, cfg)
			t.Cleanup(p.Close)
			p.mu.Lock()
			p.HWM = tc.hwm
			p.snapshotRecovery = tc.snapshot != 0
			p.recoveryCheckpointHWM = tc.snapshot
			p.mu.Unlock()
			if tc.directory {
				require.NoError(t, os.Mkdir(p.hwmCheckpointPath, 0700))
			} else if tc.checkpoint != "" {
				require.NoError(t, os.WriteFile(p.hwmCheckpointPath, []byte(tc.checkpoint), 0600))
			}
			if tc.temporary {
				require.NoError(t, os.WriteFile(p.hwmCheckpointPath+".tmp", []byte(`{"version":2,"hwm":99}`), 0600))
			}
			receiver := partitionTransfer(t, d, 0, tc.end, nil)
			originalLog, err := os.ReadFile(d.GetSegmentPath(0))
			require.NoError(t, err)
			t.Cleanup(func() {
				err := receiver.Abort()
				require.True(t, err == nil || errors.Is(err, disk.ErrBootstrapPending), "%v", err)
			})
			s, err := p.BeginBootstrapSession()
			require.NoError(t, err)
			t.Cleanup(s.Close)
			detached := false
			err = s.Install(receiver, func() error { detached = true; return nil })
			if tc.accept {
				require.NoError(t, err)
				require.True(t, detached)
				require.NoError(t, s.Complete(nil))
				s.Close()
				require.Equal(t, tc.end, p.GetHWM())
				require.True(t, p.HWMKnown())
				return
			}
			require.Error(t, err)
			require.False(t, detached)
			s.Close()
			require.NoError(t, receiver.Abort())
			require.False(t, p.BootstrapPending())
			require.Equal(t, BootstrapOutcomePreIntentAbort, s.ClosedOutcome())
			require.NoError(t, p.RecoveryError())
			require.Equal(t, tc.hwm, p.GetHWM())
			require.Equal(t, uint64(8), p.NextOffset())
			require.Equal(t, uint64(8), d.GetAbsoluteOffset())
			pending, err := d.PendingBootstrap()
			require.NoError(t, err)
			require.Nil(t, pending)
			records, err := p.ReadMessages(0, 10)
			require.NoError(t, err)
			require.Len(t, records, 1)
			require.Equal(t, "old", records[0].Payload)
			retainedLog, err := os.ReadFile(d.GetSegmentPath(0))
			require.NoError(t, err)
			require.Equal(t, originalLog, retainedLog)
			if tc.directory {
				info, err := os.Stat(p.hwmCheckpointPath)
				require.NoError(t, err)
				require.True(t, info.IsDir())
			} else if tc.checkpoint != "" {
				data, err := os.ReadFile(p.hwmCheckpointPath)
				require.NoError(t, err)
				require.Equal(t, tc.checkpoint, string(data))
			} else {
				_, err := os.Stat(p.hwmCheckpointPath)
				require.True(t, os.IsNotExist(err))
			}
		})
	}
}

func TestBootstrapResumeRetainsCommittedFloor(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, snapshot := range []bool{false, true} {
			t.Run(fmtBootstrapFloorCase(prepared, snapshot), func(t *testing.T) {
				d, cfg := bootstrapPartitionDisk(t, 0)
				p := NewPartition(0, "orders", d, nil, cfg)
				t.Cleanup(p.Close)
				receiver := partitionTransfer(t, d, 0, 3, nil)
				t.Cleanup(func() {
					err := receiver.Abort()
					require.True(t, err == nil || errors.Is(err, disk.ErrBootstrapPending), "%v", err)
				})
				s, err := p.BeginBootstrapSession()
				require.NoError(t, err)
				require.NoError(t, s.Install(receiver, nil))
				if prepared {
					// Fail receipt publication after rebuilding and checkpointing.
					receipt := d.BaseName + ".bootstrap.completed"
					require.NoError(t, os.Mkdir(receipt, 0700))
					require.Error(t, s.Complete(nil))
					require.True(t, s.Prepared())
					require.NoError(t, os.Remove(receipt))
				}
				s.Close()
				p.mu.Lock()
				if snapshot {
					p.snapshotRecovery = true
					p.recoveryCheckpointHWM = 4
				} else {
					p.HWM = 4
				}
				p.mu.Unlock()
				s, err = p.BeginBootstrapSession()
				require.NoError(t, err)
				t.Cleanup(s.Close)
				require.False(t, s.Prepared())
				called := false
				require.Error(t, s.WithPreparedReadView(func(*PartitionReadView) error { called = true; return nil }))
				require.Error(t, s.Complete(func(*PartitionReadView, []byte) error { called = true; return nil }))
				require.False(t, called)
				s.Close()
				require.Equal(t, uint64(4), p.RecoveryCommittedHWMFloor())
				require.True(t, p.BootstrapPending())
				pending, err := d.PendingBootstrap()
				require.NoError(t, err)
				require.NotNil(t, pending)
			})
		}
	}
}

func fmtBootstrapFloorCase(prepared, snapshot bool) string {
	name := "raw"
	if prepared {
		name = "prepared"
	}
	if snapshot {
		return name + "/snapshot"
	}
	return name + "/memory"
}

func TestBootstrapPendingReplayRecreatesCheckpoint(t *testing.T) {
	for _, checkpointWritten := range []bool{false, true} {
		t.Run(fmtBootstrapFloorCase(checkpointWritten, false), func(t *testing.T) {
			d, cfg := bootstrapPartitionDisk(t, 0)
			p := NewPartition(0, "orders", d, nil, cfg)
			path := p.hwmCheckpointPath
			require.NoError(t, os.WriteFile(path, []byte(`{"version":2,"hwm":2}`), 0600))
			receiver := partitionTransfer(t, d, 0, 5, nil)
			s, err := p.BeginBootstrapSession()
			require.NoError(t, err)
			require.NoError(t, s.Install(receiver, nil))
			_, err = os.Stat(path)
			require.True(t, os.IsNotExist(err))
			if checkpointWritten {
				calls := 0
				failure := errors.New("checkpoint directory sync failure")
				p.checkpointDirectorySync = func(path string) error {
					calls++
					if calls == 2 {
						return failure
					}
					return syncPartitionDirectory(path)
				}
				require.ErrorIs(t, s.Complete(nil), failure)
				hwm, ok, strict := loadHWMCheckpoint(path)
				require.True(t, ok && strict)
				require.Equal(t, uint64(5), hwm)
			}
			s.Close()
			p.Close()
			require.NoError(t, d.Close())
			reopened, err := disk.NewDiskHandler(cfg, "orders", 0)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			_, err = os.Stat(path)
			require.True(t, os.IsNotExist(err), "replay deletes even a newly written checkpoint")
			recovered := NewPartition(0, "orders", reopened, nil, cfg)
			t.Cleanup(recovered.Close)
			require.True(t, recovered.BootstrapPending())
			require.Zero(t, recovered.GetHWM())
			require.False(t, recovered.HWMKnown())
			s, err = recovered.BeginBootstrapSession()
			require.NoError(t, err)
			t.Cleanup(s.Close)
			require.NoError(t, s.Complete(nil))
			s.Close()
			require.Equal(t, uint64(5), recovered.GetHWM())
			require.True(t, recovered.HWMKnown())
			hwm, ok, strict := loadHWMCheckpoint(path)
			require.True(t, ok && strict)
			require.Equal(t, uint64(5), hwm)
			err = receiver.Abort()
			require.True(t, err == nil || errors.Is(err, disk.ErrBootstrapPending), "%v", err)
		})
	}
}
