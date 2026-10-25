package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// A timeout scan can already be waiting when a live heartbeat arrives. The
// durable commit fence must serialize removal without starving that heartbeat.
func TestCooperativeHeartbeatRefreshesBeforeQueuedExpiration(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 2)
	m := runtimeJoin(t, c, parts, "a")
	c.mu.Lock()
	c.groups["workers"].Members["a"].LastHeartbeat = time.Now().Add(-time.Hour)
	c.mu.Unlock()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		if record.Type == ConsumerMetadataRecordOffsetSnapshot {
			close(entered)
			<-release
		}
		return nil
	})
	committed := make(chan error, 1)
	go func() { committed <- c.CommitCooperative("workers", m.RegistrationEpoch, m.Grants[0], 16) }()
	<-entered
	expired := make(chan error, 1)
	go func() { expired <- c.expireCooperative("workers", time.Second) }()
	require.Eventually(t, func() bool {
		if c.lifecycleMu.TryLock() {
			c.lifecycleMu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond, "expiration must be queued behind durable commit")
	heartbeat := make(chan error, 1)
	go func() {
		_, err := c.ReconcileCooperative("workers", m.Member, m.RegistrationEpoch, m.Session, parts, nil, false)
		heartbeat <- err
	}()
	select {
	case err := <-heartbeat:
		require.NoError(t, err)
	case <-time.After(time.Second):
		// On the old implementation expiration held lifecycleMu while waiting
		// for the append's global read lock. It removes the stale session before
		// the blocked heartbeat can refresh it: release I/O to expose fencing.
		unblock()
		require.NoError(t, <-committed)
		require.NoError(t, <-expired)
		require.NoError(t, <-heartbeat, "live heartbeat was fenced after waiting behind durable append and expiration")
		t.Fatal("heartbeat did not complete until durable append resumed")
	}
	unblock()
	require.NoError(t, <-committed)
	require.NoError(t, <-expired)
	require.NoError(t, c.ValidateCooperativeRead("workers", m.RegistrationEpoch, &m.Grants[0]))
}

// The FSM restore path must not wait for a writer that may itself be waiting
// for Raft. Such a writer must not acknowledge an update to the detached map.
func TestCooperativeCommitRestoreFencesDetachedCompletion(t *testing.T) {
	c, parts, records := runtimeCooperativeFixture(t, 2)
	m := runtimeJoin(t, c, parts, "a")
	require.NoError(t, c.CommitCooperative("workers", m.RegistrationEpoch, m.Grants[0], 8))
	snapshot := c.ExportState()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		close(entered)
		<-release
		*records = append(*records, record)
		return nil
	})
	committed := make(chan error, 1)
	go func() { committed <- c.CommitCooperative("workers", m.RegistrationEpoch, m.Grants[0], 16) }()
	<-entered
	restored := make(chan error, 1)
	go func() { restored <- c.ImportState(snapshot) }()
	select {
	case err := <-restored:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("restore waited for the in-flight durable writer")
	}
	unblock()
	require.ErrorIs(t, <-committed, ErrConsumerMetadataRecovering)
	require.Equal(t, uint64(8), c.ExportState()["workers"].Offsets["orders"][0])
	// A rejected ACK is ambiguous, not a discarded durable record. Subsequent
	// log replay must recover the completed append with the same ownership.
	var messages []types.Message
	for i, record := range *records {
		messages = append(messages, encodedMetadataMessage(t, record, uint64(i)))
	}
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	recovered, err := NewCoordinatorWithRecovery(context.Background(), cfg, &metadataReplayHandler{messages: map[int][]types.Message{0: messages}})
	require.NoError(t, err)
	t.Cleanup(recovered.Stop)
	require.Equal(t, uint64(16), recovered.ExportState()["workers"].Offsets["orders"][0])
	require.NoError(t, recovered.ValidateCooperativeRead("workers", m.RegistrationEpoch, &m.Grants[0]))
}

// Appending errors never advance the visible offset; a heartbeat is only a
// session refresh and cannot convert either a failed write or stale grant to ACK.
func TestCooperativeHeartbeatDoesNotAcknowledgeFailedCommit(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 2)
	m := runtimeJoin(t, c, parts, "a")
	require.NoError(t, c.CommitCooperative("workers", m.RegistrationEpoch, m.Grants[0], 8))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	injected := errors.New("injected append rejection")
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { close(entered); <-release; return injected })
	committed := make(chan error, 1)
	go func() { committed <- c.CommitCooperative("workers", m.RegistrationEpoch, m.Grants[0], 16) }()
	<-entered
	heartbeat := make(chan error, 1)
	go func() {
		_, err := c.ReconcileCooperative("workers", m.Member, m.RegistrationEpoch, m.Session, parts, nil, false)
		heartbeat <- err
	}()
	select {
	case err := <-heartbeat:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("heartbeat blocked behind rejected append")
	}
	stale := m.Grants[0]
	stale.Epoch++
	require.ErrorContains(t, c.CommitCooperative("workers", m.RegistrationEpoch, stale, 99), "cooperative_fenced")
	require.Equal(t, stableOffsetResult{offset: 8, found: true}, awaitStable(t, readStableAsync(c, 0)))
	unblock()
	require.ErrorIs(t, <-committed, injected)
	require.Equal(t, uint64(8), c.ExportState()["workers"].Offsets["orders"][0])
}
