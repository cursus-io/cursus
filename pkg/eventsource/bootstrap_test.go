package eventsource

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func newBootstrapHandler(t *testing.T) (*Handler, *topic.Partition) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 0, 0
	cfg.SegmentSize, cfg.IndexSize, cfg.IndexIntervalBytes = 1<<20, 4096, 64
	cfg.SegmentRollTimeMS = 0
	cfg.RetentionCheckIntervalMS, cfg.CompactionCheckIntervalMS = 60000, 60000
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, &fakeStreamManager{})
	require.NoError(t, tm.CreateTopic("orders", 1, false, true))
	require.NoError(t, tm.CreateTopic("neighbor", 1, false, true))
	h := NewHandler(tm)
	t.Cleanup(func() { require.NoError(t, h.Close()); tm.Stop(); dm.CloseAllHandlers() })
	p, err := tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	return h, p
}
func catalogBytes(t *testing.T, snapshots ...bootstrapSnapshot) []byte {
	t.Helper()
	partition, lifecycle := 0, uint64(1)
	if snapshots == nil {
		snapshots = []bootstrapSnapshot{}
	}
	data, err := json.Marshal(bootstrapCatalog{Version: 1, Topic: "orders", Partition: &partition, LifecycleEpoch: &lifecycle, Snapshots: snapshots})
	require.NoError(t, err)
	return data
}
func stageEventBootstrap(t *testing.T, p *topic.Partition, start, end uint64, data []byte, records ...types.DiskMessage) *disk.BootstrapReceiver {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 0, 0
	cfg.SegmentSize, cfg.IndexSize, cfg.IndexIntervalBytes = 1<<20, 4096, 64
	cfg.SegmentRollTimeMS = 0
	cfg.RetentionCheckIntervalMS, cfg.CompactionCheckIntervalMS = 60000, 60000
	d, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	if end > 0 {
		require.NoError(t, d.WriteCompactedReplicaRange(0, end, records))
	}
	export, err := d.ExportBootstrap(start, end, disk.BootstrapFence{Topic: "orders", Partition: 0, SourceID: "source", LeaderID: "source", ReceiverID: "target", LeaderEpoch: 1, LifecycleEpoch: 1, RecoveryProtocol: 3, CommittedHWM: end}, data)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, export.Close()) })
	receiver, err := p.StageBootstrap(export.Manifest())
	require.NoError(t, err)
	for _, f := range export.Manifest().Files {
		for off := int64(0); off < f.Size; {
			chunk, err := export.ReadChunk(f.ID, off, disk.BootstrapChunkBytes)
			require.NoError(t, err)
			require.NoError(t, receiver.WriteChunk(f.ID, off, chunk))
			off += int64(len(chunk))
		}
	}
	return receiver
}
func installEventBootstrap(t *testing.T, h *Handler, p *topic.Partition, receiver *disk.BootstrapReceiver) *topic.BootstrapSession {
	t.Helper()
	session, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	err = session.Install(receiver, func() error { return h.DetachPartitionForBootstrap("orders", 0) })
	if err != nil {
		session.Close()
	}
	require.NoError(t, err)
	t.Cleanup(session.Close)
	return session
}
func rebuildEventBootstrap(h *Handler) func(*topic.PartitionReadView, []byte) error {
	return func(v *topic.PartitionReadView, data []byte) error {
		return h.RebuildBootstrapState("orders", 0, v, data, 1)
	}
}

func TestBootstrapCatalogStrictCodec(t *testing.T) {
	valid := string(catalogBytes(t, bootstrapSnapshot{Key: "a", Version: 1, Payload: "payload"}))
	cases := []string{
		valid + " {}",
		strings.Replace(valid, `"version":1`, `"version":1,"Version":1`, 1),
		strings.Replace(valid, `"topic":"orders"`, `"Topic":"orders"`, 1), strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(valid, `"version":1`, `"version":2`, 1),
		strings.Replace(valid, `"topic":"orders"`, `"topic":"other"`, 1),
		strings.Replace(valid, `"partition":0,`, ``, 1), strings.Replace(valid, `"lifecycle_epoch":1,`, ``, 1),
		strings.Replace(valid, `"lifecycle_epoch":1`, `"lifecycle_epoch":2`, 1),
		strings.Replace(valid, `"key_base64":"YQ=="`, `"key_base64":""`, 1), strings.Replace(valid, `"payload_base64":"cGF5bG9hZA=="`, `"unknown":0,"payload_base64":"cGF5bG9hZA=="`, 1),
		strings.Replace(valid, `"snapshots":[`, `"snapshots":[{"key_base64":"YQ==","version":1,"payload_base64":"eA=="},`, 1),
		strings.Replace(valid, `,"payload_base64":"cGF5bG9hZA=="`, ``, 1),
		strings.Replace(valid, `"payload_base64":"cGF5bG9hZA=="`, `"payload_base64":null`, 1),
		string(catalogBytes(t, bootstrapSnapshot{Key: "a", Version: 0})), strings.Repeat(" ", disk.BootstrapCatalogBytes+1),
	}
	for i, data := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			_, err := decodeBootstrapCatalog([]byte(data), "orders", 0, 1)
			require.Error(t, err)
		})
	}
	_, err := decodeBootstrapCatalog([]byte(valid), "orders", 0, 1)
	require.NoError(t, err)
	_, err = decodeBootstrapCatalog([]byte(strings.Replace(valid, `"lifecycle_epoch":1`, `"lifecycle_epoch":0`, 1)), "orders", 0, 0)
	require.NoError(t, err)
}

func TestBootstrapExactCatalogReplacementAndReopen(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			h, p := newBootstrapHandler(t)
			dir := h.tm.GetLogDir("orders", 0)
			require.NoError(t, os.MkdirAll(dir, 0o750))
			legacy := filepath.Join(dir, "partition_0_snapshots.dat")
			record := binary.BigEndian.AppendUint16(nil, uint16(len("legacy-only")))
			record = append(record, []byte("legacy-only")...)
			record = binary.BigEndian.AppendUint64(record, 88)
			record = binary.BigEndian.AppendUint32(record, uint32(len("legacy")))
			record = append(record, []byte("legacy")...)
			require.NoError(t, os.WriteFile(legacy, record, 0o600))
			require.NoError(t, seedStaleSnapshotFixture(h, SnapshotResult{Topic: "orders", Partition: 0, Key: "stale", Version: 99, Payload: "old"}))
			require.NotNil(t, h.snapshots["orders:0"].legacyFile)

			require.NoError(t, seedStaleSnapshotFixture(h, SnapshotResult{Topic: "orders", Partition: 0, Key: "receiver-only", Version: 9, Payload: "old"}))
			require.NoError(t, h.PrepareCommittedIndex("orders", 0))
			appendSnapshotFixtureHistory(t, h, SnapshotResult{Topic: "neighbor", Partition: 0, Key: "keep", Version: 9, Payload: "keep"})
			require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "neighbor", Partition: 0, Key: "keep", Version: 9, Payload: "keep"}))
			require.NoError(t, h.PrepareCommittedIndex("neighbor", 0))
			neighborIndex, neighborStore := h.indexes["neighbor:0"], h.snapshots["neighbor:0"]
			data := catalogBytes(t)
			if !empty {
				data = catalogBytes(t, bootstrapSnapshot{Key: "stale", Version: 1, Payload: "source"})
			}
			receiver := stageEventBootstrap(t, p, 0, 1, data, types.DiskMessage{Topic: "orders", Partition: 0, Offset: 0, Key: "stale", AggregateVersion: 1, Payload: "event"})
			session := installEventBootstrap(t, h, p, receiver)
			require.Same(t, neighborIndex, h.indexes["neighbor:0"])
			require.Same(t, neighborStore, h.snapshots["neighbor:0"])
			require.NoError(t, session.Complete(rebuildEventBootstrap(h)))
			session.Close()
			require.NoFileExists(t, legacy)
			snaps, errResp := h.ListSnapshots("orders", 0)
			require.Empty(t, errResp)
			if empty {
				require.Empty(t, snaps)
			} else {
				require.Len(t, snaps, 1)
				require.Equal(t, uint64(1), snaps[0].Version)
			}
			require.NoError(t, h.DetachPartitionForBootstrap("orders", 0))
			snaps, errResp = h.ListSnapshots("orders", 0)
			require.Empty(t, errResp)
			if empty {
				require.Empty(t, snaps)
			} else {
				require.Len(t, snaps, 1)
				require.Equal(t, "source", snaps[0].Payload)
			}
			version, _, err := h.CurrentVersion("orders", "stale")
			require.NoError(t, err)
			require.Equal(t, uint64(1), version)
		})
	}
}

func TestBootstrapRetainedReplaySyncFailureAndRetry(t *testing.T) {
	for _, fail := range []string{"index", "catalog", "directory"} {
		t.Run(fail, func(t *testing.T) {
			h, p := newBootstrapHandler(t)
			data := catalogBytes(t, bootstrapSnapshot{Key: "a", Version: 2, Payload: "checkpoint"})
			receiver := stageEventBootstrap(t, p, 2, 3, data, types.DiskMessage{Topic: "orders", Partition: 0, Offset: 2, Key: "a", AggregateVersion: 3, Payload: "retained"})
			session := installEventBootstrap(t, h, p, receiver)
			originalIndex, originalSnapshots := syncBootstrapIndex, syncBootstrapSnapshots
			originalDirectory := syncSnapshotCreationDirectory
			t.Cleanup(func() {
				syncBootstrapIndex, syncBootstrapSnapshots = originalIndex, originalSnapshots
				syncSnapshotCreationDirectory = originalDirectory
			})
			switch fail {
			case "index":
				syncBootstrapIndex = func(idx *StreamIndex) error {
					require.Equal(t, uint64(3), idx.GetVersion("a"))
					return errors.New("index sync injected")
				}
			case "directory":
				syncSnapshotCreationDirectory = func(dir string) error {
					info, err := os.Stat(filepath.Join(dir, "partition_0_stream.idx"))
					if err == nil && info.Size() > 0 {
						return errors.New("directory sync injected")
					}
					return originalDirectory(dir)
				}
			default:
				syncBootstrapSnapshots = func(ss *SnapshotStore) error {
					snap, err := ss.Read("a")
					require.NoError(t, err)
					require.NotNil(t, snap)
					return errors.New("catalog sync injected")
				}
			}
			require.Error(t, session.Complete(rebuildEventBootstrap(h)))
			session.Close()
			require.True(t, p.BootstrapPending())
			require.Nil(t, h.indexes["orders:0"])
			require.Nil(t, h.snapshots["orders:0"])
			require.NotContains(t, h.indexedHWM, "orders:0")
			syncBootstrapIndex, syncBootstrapSnapshots = originalIndex, originalSnapshots
			syncSnapshotCreationDirectory = originalDirectory
			session, err := p.BeginBootstrapSession()
			require.NoError(t, err)
			require.NoError(t, session.Complete(rebuildEventBootstrap(h)))
			session.Close()
			version, _, err := h.CurrentVersion("orders", "a")
			require.NoError(t, err)
			require.Equal(t, uint64(3), version)
			require.NoError(t, h.DetachPartitionForBootstrap("orders", 0))
			version, _, err = h.CurrentVersion("orders", "a")
			require.NoError(t, err)
			require.Equal(t, uint64(3), version)
		})
	}
}

func TestBootstrapPendingRejectsCachedPublicRootsBeforeReset(t *testing.T) {
	h, p := newBootstrapHandler(t)
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=old"), "OK")
	require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "old"}))
	receiver := stageEventBootstrap(t, p, 0, 0, catalogBytes(t))
	session := installEventBootstrap(t, h, p, receiver)
	session.Close()
	// Model an accidentally retained cache entry: every public root must still
	// consult pending state before it observes or mutates derived state.
	idx, err := NewStreamIndex(t.TempDir(), 0)
	require.NoError(t, err)
	require.NoError(t, idx.SeedVersion("a", 7))
	h.indexes["orders:0"] = idx
	ss, err := NewSnapshotStore(t.TempDir(), 0)
	require.NoError(t, err)
	require.NoError(t, ss.Save("a", 7, "sentinel"))
	h.snapshots["orders:0"] = ss
	require.ErrorIs(t, h.PrepareCommittedIndex("orders", 0), topic.ErrBootstrapPending)
	_, _, err = h.CurrentVersion("orders", "a")
	require.ErrorIs(t, err, topic.ErrBootstrapPending)
	require.ErrorIs(t, h.IndexCommittedToHWM("orders", 0, 8), topic.ErrBootstrapPending)
	require.ErrorIs(t, h.RecoverIndexFromLog("orders", 0, idx), topic.ErrBootstrapPending)
	require.Equal(t, uint64(7), idx.GetVersion("a"))
	_, resp := h.ListSnapshots("orders", 0)
	require.Contains(t, resp, "bootstrap pending")
	_, resp = h.ListSnapshotsPage("orders", 0, "", 1)
	require.Contains(t, resp, "bootstrap pending")
	_, resp = h.ListSnapshotsPageAtRevision("orders", 0, "", 1, 0)
	require.Contains(t, resp, "bootstrap pending")
	_, resp = h.FetchSnapshot("orders", 0, "a")
	require.Contains(t, resp, "bootstrap pending")
	require.Contains(t, h.HandleReadSnapshot("READ_SNAPSHOT topic=orders key=a"), "bootstrap pending")
	require.Contains(t, h.HandleStreamVersion("STREAM_VERSION topic=orders key=a"), "bootstrap pending")
	require.Contains(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 8, Payload: "bad"}), "bootstrap pending")
	_, resp = h.SaveSnapshot("SAVE_SNAPSHOT topic=orders key=a version=8 message=bad", nil)
	require.Contains(t, resp, "bootstrap pending")
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=8 message=bad"), "bootstrap pending")
	envelope, batch := readStreamHistoryForTest(t, h, "READ_STREAM_HISTORY topic=orders key=a")
	require.Equal(t, "ERROR", envelope["status"])
	require.Contains(t, envelope["error"], "bootstrap pending")
	require.Nil(t, batch)
	client, server := net.Pipe()
	defer func() { _ = client.Close(); _ = server.Close() }()
	done := make(chan struct{})
	go func() { defer close(done); h.HandleReadStream("READ_STREAM topic=orders key=a", server) }()
	response, err := util.ReadWithLength(client)
	require.NoError(t, err)
	require.Contains(t, string(response), "bootstrap pending")
	<-done

}

func TestSnapshotCallbackBootstrapGenerationFence(t *testing.T) {
	h, p := newBootstrapHandler(t)
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=old"), "OK")
	receiver := stageEventBootstrap(t, p, 0, 0, catalogBytes(t))
	_, resp := h.SaveSnapshot("SAVE_SNAPSHOT topic=orders key=a version=1 message=stale", func(SnapshotResult) error {
		session := installEventBootstrap(t, h, p, receiver)
		defer session.Close()
		return session.Complete(rebuildEventBootstrap(h))
	})
	require.Contains(t, resp, "snapshot_generation_changed")
	snapshots, resp := h.ListSnapshots("orders", 0)
	require.Empty(t, resp)
	require.Empty(t, snapshots)
}

func TestBlockedStreamNetworkDoesNotHoldReadView(t *testing.T) {
	for _, history := range []bool{false, true} {
		for _, invalid := range []bool{false, true} {
			t.Run(fmt.Sprintf("history=%t/error=%t", history, invalid), func(t *testing.T) {
				h, p := newBootstrapHandler(t)
				require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=event"), "OK")
				if invalid {
					idx := h.indexes["orders:0"]
					require.NoError(t, idx.Append("a", 2, 99, 0))
				}
				pipe, client := net.Pipe()
				server := &observedWriteConn{Conn: pipe, started: make(chan struct{})}
				defer func() { _ = client.Close() }()
				defer func() { _ = server.Close() }()
				done := make(chan struct{})
				go func() {
					defer close(done)
					if history {
						cmd := "READ_STREAM_HISTORY topic=orders key=a"
						if invalid {
							cmd += " max_bytes=1"
						}
						h.HandleReadStreamHistory(cmd, server)
					} else {
						h.HandleReadStream("READ_STREAM topic=orders key=a", server)
					}
				}()
				// Wait for an actual blocked success or error write.
				select {
				case <-server.started:
				case <-time.After(3 * time.Second):
					t.Fatal("response never reached network")
				}
				acquired := make(chan error, 1)
				go func() {
					session, err := p.BeginBootstrapSession()
					if err == nil {
						session.Close()
					}
					acquired <- err
				}()
				select {
				case err := <-acquired:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Fatal("network write retained read view")
				}
				require.NoError(t, client.Close())
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("reader did not stop")
				}
			})
		}
	}
}

func TestAppendCallbackRequiresControllerFence(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()
	called := false
	_, resp := h.AppendStream("APPEND_STREAM topic=orders key=a version=1 message=event", AppendOptions{AfterAppend: func(string, int, types.Message) error { called = true; return nil }})
	require.Contains(t, resp, "require_controller_fence")
	require.False(t, called)
}

func TestBootstrapViewIdentityAndLifetime(t *testing.T) {
	h, p := newBootstrapHandler(t)
	appendSnapshotFixtureHistory(t, h, SnapshotResult{Topic: "neighbor", Key: "keep", Version: 1, Payload: "keep"})
	require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "neighbor", Key: "keep", Version: 1, Payload: "keep"}))
	var escaped *topic.PartitionReadView
	var escapedSource *topic.BootstrapSource
	require.NoError(t, p.WithReadView(func(view *topic.PartitionReadView) error {
		require.ErrorContains(t, h.RebuildBootstrapState("orders", 0, view, catalogBytes(t), 1), "exclusive callback")
		_, err := h.CaptureBootstrapCatalog(context.Background(), "orders", 0, nil, 1)
		require.ErrorContains(t, err, "exclusive callback")
		return nil
	}))
	require.NoError(t, p.WithBootstrapSource(func(source *topic.BootstrapSource) error {
		escaped = source.View()
		escapedSource = source
		_, wrongEpoch := h.CaptureBootstrapCatalog(context.Background(), "orders", 0, source, 2)
		require.ErrorContains(t, wrongEpoch, "lifecycle mismatch")
		_, err := h.CaptureBootstrapCatalog(context.Background(), "neighbor", 0, source, 1)
		require.ErrorContains(t, err, "identity mismatch")
		partition, lifecycle := 0, uint64(1)
		data, err := json.Marshal(bootstrapCatalog{Version: 1, Topic: "neighbor", Partition: &partition, LifecycleEpoch: &lifecycle, Snapshots: []bootstrapSnapshot{}})
		require.NoError(t, err)
		require.ErrorContains(t, h.RebuildBootstrapState("neighbor", 0, escaped, data, 1), "identity mismatch")
		data, err = h.CaptureBootstrapCatalog(context.Background(), "orders", 0, escapedSource, 1)
		require.NoError(t, err)
		_, err = decodeBootstrapCatalog(data, "orders", 0, 1)
		return err
	}))
	_, err := h.CaptureBootstrapCatalog(context.Background(), "orders", 0, escapedSource, 1)
	require.ErrorContains(t, err, "expired")
	snaps, resp := h.ListSnapshots("neighbor", 0)
	require.Empty(t, resp)
	require.Len(t, snaps, 1)
}

func TestBootstrapCaptureRejectsBase64OversizeCatalog(t *testing.T) {
	h, p := newBootstrapHandler(t)
	// Base64 expansion makes the encoded inventory exceed 16 MiB even though
	// its raw payload fits, and capture must never silently omit it.
	appendSnapshotFixtureHistory(t, h, SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: strings.Repeat("<", disk.BootstrapCatalogBytes*3/4)})
	require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: strings.Repeat("<", disk.BootstrapCatalogBytes*3/4)}))
	require.NoError(t, p.WithBootstrapSource(func(source *topic.BootstrapSource) error {
		data, err := h.CaptureBootstrapCatalog(context.Background(), "orders", 0, source, 1)
		require.ErrorContains(t, err, "byte limit")
		require.Nil(t, data)
		return nil
	}))
}

func TestStandaloneAppendExcludesBootstrapThroughIndexUpdate(t *testing.T) {
	h, p := newBootstrapHandler(t)
	require.NoError(t, h.PrepareCommittedIndex("orders", 0))
	idx := h.indexes["orders:0"]
	idx.mu.Lock()
	appended := make(chan string, 1)
	go func() { appended <- h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=event") }()
	time.Sleep(30 * time.Millisecond)
	acquired := make(chan error, 1)
	go func() {
		session, err := p.BeginBootstrapSession()
		if err == nil {
			session.Close()
		}
		acquired <- err
	}()
	select {
	case err := <-acquired:
		idx.mu.Unlock()
		t.Fatalf("bootstrap overtook append: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	idx.mu.Unlock()
	select {
	case resp := <-appended:
		require.Contains(t, resp, "OK")
	case <-time.After(3 * time.Second):
		t.Fatal("append blocked")
	}
	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("bootstrap blocked")
	}
}

func TestControllerFencedAppendDoesNotReacquireReconcile(t *testing.T) {
	h, p := newBootstrapHandler(t)
	require.NoError(t, h.PrepareCommittedIndex("orders", 0))
	release := p.BeginReplicationMutation()
	acquired := make(chan error, 1)
	go func() {
		session, err := p.BeginBootstrapSession()
		if err == nil {
			session.Close()
		}
		acquired <- err
	}()
	// A queued exclusive waiter makes any recursive RLock deadlock.
	time.Sleep(30 * time.Millisecond)
	completed := make(chan string, 1)
	go func() {
		_, resp := h.AppendStreamWithControllerFence("APPEND_STREAM topic=orders key=a version=1 message=event", AppendOptions{LeaderAppend: true, AfterAppend: func(string, int, types.Message) error {
			// Local read callbacks are safe because append's preflight view is gone.
			return p.WithReadView(func(view *topic.PartitionReadView) error { return view.ValidateIdentity("orders", 0) })
		}, AfterCommit: func(_ string, _ int, hwm uint64) error { return p.ApplyReplicaHWM(hwm) }})
		completed <- resp
	}()
	select {
	case resp := <-completed:
		require.Empty(t, resp)
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("fenced append recursively acquired reconcile")
	}
	release()
	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("bootstrap blocked after append")
	}
}

type observedWriteConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func TestAppendErrorRecoveryWaitsForIndexSynchronization(t *testing.T) {
	h, p := newBootstrapHandler(t)
	require.NoError(t, h.PrepareCommittedIndex("orders", 0))
	h.indexSyncMu.Lock()
	locked := true
	defer func() {
		if locked {
			h.indexSyncMu.Unlock()
		}
	}()
	committed := make(chan struct{})
	completed := make(chan string, 1)
	go func() {
		_, response := h.AppendStreamWithControllerFence("APPEND_STREAM topic=orders key=a version=1 message=event", AppendOptions{
			LeaderAppend: true,
			AfterCommit: func(_ string, _ int, hwm uint64) error {
				err := p.ApplyReplicaHWM(hwm)
				close(committed)
				return errors.Join(err, errors.New("commit acknowledgement lost"))
			},
		})
		completed <- response
	}()
	select {
	case <-committed:
	case <-time.After(3 * time.Second):
		t.Fatal("append did not reach commit callback")
	}
	select {
	case response := <-completed:
		t.Fatalf("recovery bypassed index synchronization: %s", response)
	case <-time.After(30 * time.Millisecond):
	}
	h.indexSyncMu.Unlock()
	locked = false
	select {
	case response := <-completed:
		require.Empty(t, response)
	case <-time.After(3 * time.Second):
		t.Fatal("append recovery did not finish")
	}
	version, partition, err := h.CurrentVersion("orders", "a")
	require.NoError(t, err)
	require.Zero(t, partition)
	require.Equal(t, uint64(1), version)
}

func (c *observedWriteConn) Write(data []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(data)
}

func TestBootstrapCaptureExactEncodedLimit(t *testing.T) {
	h, p := newBootstrapHandler(t)
	version := uint64(1)
	base := catalogBytes(t, bootstrapSnapshot{Key: "a", Version: version, Payload: ""})
	// Base64 grows in groups of four. Adjust version digits so the available
	// payload budget is exactly divisible by four and exercises the exact cap.
	for i := 0; i < (disk.BootstrapCatalogBytes-len(base))%4; i++ {
		version *= 10
	}
	base = catalogBytes(t, bootstrapSnapshot{Key: "a", Version: version, Payload: ""})
	payloadBytes := (disk.BootstrapCatalogBytes - len(base)) / 4 * 3
	appendSnapshotFixtureHistory(t, h, SnapshotResult{Topic: "orders", Key: "a", Version: version, Payload: strings.Repeat("x", payloadBytes)})
	require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: version, Payload: strings.Repeat("x", payloadBytes)}))
	require.NoError(t, p.WithBootstrapSource(func(source *topic.BootstrapSource) error {
		data, err := h.CaptureBootstrapCatalog(context.Background(), "orders", 0, source, 1)
		require.NoError(t, err)
		require.Len(t, data, disk.BootstrapCatalogBytes)
		return nil
	}))
}

func TestHistoryBoundsDetachedReplayMetadata(t *testing.T) {
	h, hp := newTestHandlerWithProvider(t)
	defer func() { _ = h.Close() }()
	require.Contains(t, h.HandleAppendStream("APPEND_STREAM topic=orders key=a version=1 message=event"), "OK")
	storage := hp.handlers["orders:0"]
	storage.mu.Lock()
	storage.msgs[0].EventID = strings.Repeat("x", maxHistoryBytes)
	storage.mu.Unlock()
	envelope, batch := readStreamHistoryForTest(t, h, "READ_STREAM_HISTORY topic=orders key=a")
	require.Equal(t, "ERROR", envelope["status"])
	require.Contains(t, envelope["error"], "record_too_large")
	require.Nil(t, batch)
}

func TestDetachPartitionPreservesSameTopicNeighbors(t *testing.T) {
	h := newTestHandler(t)
	defer func() { _ = h.Close() }()
	require.NoError(t, h.tm.CreateTopic("many", 11, false, true))
	for _, id := range []int{1, 10} {
		require.NoError(t, h.PrepareCommittedIndex("many", id))
		require.NoError(t, seedStaleSnapshotFixture(h, SnapshotResult{Topic: "many", Partition: id, Key: "keep", Version: 1, Payload: "value"}))
	}
	neighborIndex, neighborSnapshots := h.indexes["many:10"], h.snapshots["many:10"]
	require.NoError(t, h.DetachPartitionForBootstrap("many", 1))
	require.NotContains(t, h.indexes, "many:1")
	require.NotContains(t, h.snapshots, "many:1")
	require.NotContains(t, h.indexedHWM, "many:1")
	require.Same(t, neighborIndex, h.indexes["many:10"])
	require.Same(t, neighborSnapshots, h.snapshots["many:10"])
	record, err := neighborSnapshots.Read("keep")
	require.NoError(t, err)
	require.Equal(t, "value", record.Payload)
}

func TestBootstrapSnapshotBase64Validation(t *testing.T) {
	valid := string(catalogBytes(t, bootstrapSnapshot{Key: "a", Version: 1, Payload: "payload"}))
	for _, replacement := range []string{`"key_base64":"!"`, `"key_base64":"YR=="`, `"key_base64":"YQ"`, `"key_base64":"YQ==\n"`, `"key_base64":[]`, `"Key_Base64":"YQ=="`, `"key":"a"`, `"key_base64":"YQ==","key_base64":"YQ=="`} {
		t.Run(replacement, func(t *testing.T) {
			_, err := decodeBootstrapCatalog([]byte(strings.Replace(valid, `"key_base64":"YQ=="`, replacement, 1)), "orders", 0, 1)
			require.Error(t, err)
		})
	}
	for _, replacement := range []string{`"payload_base64":"!"`, `"payload_base64":null`, `"payload":"payload"`, `"Payload_Base64":"cGF5bG9hZA=="`} {
		_, err := decodeBootstrapCatalog([]byte(strings.Replace(valid, `"payload_base64":"cGF5bG9hZA=="`, replacement, 1)), "orders", 0, 1)
		require.Error(t, err)
	}
	_, err := decodeBootstrapCatalog(catalogBytes(t, bootstrapSnapshot{Key: strings.Repeat("k", 65536), Version: 1}), "orders", 0, 1)
	require.Error(t, err)
	catalog, err := decodeBootstrapCatalog(catalogBytes(t, bootstrapSnapshot{Key: strings.Repeat("k", 65535), Version: 1}), "orders", 0, 1)
	require.NoError(t, err)
	require.Len(t, catalog.Snapshots[0].Key, 65535)
	// Decode limits are applied after decoding, independent of encoded catalog limits.
	_, err = decodeBootstrapBytes(base64.StdEncoding.EncodeToString([]byte("abcd")), 3)
	require.Error(t, err)
	_, err = decodeBootstrapCatalog(catalogBytes(t, bootstrapSnapshot{Key: string([]byte{0xff}), Version: 1}, bootstrapSnapshot{Key: string([]byte{0xff}), Version: 2}), "orders", 0, 1)
	require.Error(t, err)
}

func TestBootstrapPreservesSnapshotBytesThroughRebuildAndReopen(t *testing.T) {
	source, sourcePartition := newBootstrapHandler(t)
	snapshots := []SnapshotResult{
		{Topic: "orders", Key: string([]byte{'k', 0xff, 0x00}), Version: 1, Payload: string([]byte{0xfe, 0x80, 0x00, 'p'})},
		{Topic: "orders", Key: string([]byte{'k', 0xfe, 0x00}), Version: 2, Payload: string([]byte{0xed, 0xa0, 0x80, 0xff})},
	}
	for _, snapshot := range snapshots {
		appendSnapshotFixtureHistory(t, source, snapshot)
		require.Empty(t, source.SaveSnapshotReplica(snapshot))
	}
	var data []byte
	require.NoError(t, sourcePartition.WithBootstrapSource(func(sourceView *topic.BootstrapSource) error {
		var err error
		data, err = source.CaptureBootstrapCatalog(context.Background(), "orders", 0, sourceView, 1)
		return err
	}))
	require.NotContains(t, string(data), "\ufffd")
	captured, err := decodeBootstrapCatalog(data, "orders", 0, 1)
	require.NoError(t, err)
	require.Len(t, captured.Snapshots, 2)
	target, targetPartition := newBootstrapHandler(t)
	receiver := stageEventBootstrap(t, targetPartition, 0, 0, data)
	session := installEventBootstrap(t, target, targetPartition, receiver)
	require.NoError(t, session.Complete(rebuildEventBootstrap(target)))
	session.Close()
	for pass := 0; pass < 2; pass++ {
		for _, snapshot := range snapshots {
			restored, response := target.FetchSnapshot("orders", 0, snapshot.Key)
			require.Empty(t, response)
			require.NotNil(t, restored)
			require.Equal(t, []byte(snapshot.Key), []byte(restored.Key))
			require.Equal(t, []byte(snapshot.Payload), []byte(restored.Payload))
			require.Equal(t, snapshot.Version, restored.Version)
		}
		require.NoError(t, target.DetachPartitionForBootstrap("orders", 0))
	}
}

// This fixture intentionally models stale state replaced by an exact bootstrap
// inventory. Production save authority must never be used to invent its proof.
func seedStaleSnapshotFixture(h *Handler, snapshot SnapshotResult) error {
	return h.withReadView(snapshot.Topic, snapshot.Partition, func(v *topic.PartitionReadView) error {
		ss, err := h.getSnapshotWithView(snapshot.Topic, snapshot.Partition, v)
		if err != nil {
			return err
		}
		return ss.Save(snapshot.Key, snapshot.Version, snapshot.Payload)
	})
}
func appendSnapshotFixtureHistory(t *testing.T, h *Handler, snapshot SnapshotResult) {
	t.Helper()
	p, err := h.tm.GetTopic(snapshot.Topic).GetPartition(snapshot.Partition)
	require.NoError(t, err)
	start := p.NextOffset()
	records := make([]types.Message, snapshot.Version)
	for i := range records {
		records[i] = types.Message{Offset: start + uint64(i), Key: snapshot.Key, AggregateVersion: uint64(i + 1), Payload: "event"}
	}
	require.NoError(t, p.ReplicaAppend(records))
	require.NoError(t, p.ApplyReplicaHWM(start+snapshot.Version))
}
