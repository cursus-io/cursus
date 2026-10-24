package server

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestCooperativeWireSlowCommitKeepsStableReadsAndDurableBarrier(t *testing.T) {
	cfg, cd, tm := cooperativeWireFixture(t, true)
	require.NoError(t, cd.RegisterGroup(cfg.Topic, cfg.GroupID, 6))
	definition := tm.GetTopic(cfg.Topic).Definition()
	parts := make([]coordinator.CooperativePartition, 6)
	for p := range parts {
		parts[p] = coordinator.CooperativePartition{Topic: cfg.Topic, Partition: p, LifecycleEpoch: definition.LifecycleEpoch}
	}
	membership, err := cd.ReconcileCooperative(cfg.GroupID, "slow-writer", 0, 0, parts, nil, false)
	require.NoError(t, err)
	grant := membership.Grants[0]
	require.NoError(t, cd.CommitCooperative(cfg.GroupID, membership.RegistrationEpoch, grant, 8))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	cd.SetOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
		if record.Type == coordinator.ConsumerMetadataRecordOffsetSnapshot {
			close(entered)
			<-release
		}
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- cd.CommitCooperative(cfg.GroupID, membership.RegistrationEpoch, grant, 16) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("durable writer did not start")
	}
	dial := func() (net.Conn, *wire.Connection) {
		raw, err := net.DialTimeout("tcp", cfg.CoordinatorAddr, time.Second)
		require.NoError(t, err)
		t.Cleanup(func() { _ = raw.Close() })
		require.NoError(t, raw.SetDeadline(time.Now().Add(time.Second)))
		return raw, newWireTestClient(t, raw)
	}
	_, reader := dial()
	fetch := fmt.Sprintf("FETCH_OFFSET topic=%s group=%s partition=%d include_found=true", cfg.Topic, cfg.GroupID, grant.Partition.Partition)
	require.Contains(t, wireRequest(t, reader, wire.CommandFetchOffset, []byte(fetch)), "offset=8 found=true", "unacknowledged offset 16 must not leak into the stable view")
	select {
	case <-done:
		t.Fatal("commit acknowledged while durable writer was paused")
	default:
	}
	// Membership still uses the global mutation fence. Pausing durable append
	// deterministically reproduces a TCP response timeout through that fence;
	// it does not prove the trigger of a historical, uninstrumented stall.
	heartbeatRaw, heartbeat := dial()
	command := fmt.Sprintf("HEARTBEAT protocol=cooperative topic=%s group=%s member=%s registration_epoch=%d session=%d", cfg.Topic, cfg.GroupID, membership.Member, membership.RegistrationEpoch, membership.Session)
	writeWireCommand(t, heartbeat, wire.CommandHeartbeat, 2, command)
	_, err = heartbeat.ReadFrame()
	var timeout net.Error
	require.ErrorAs(t, err, &timeout)
	require.True(t, timeout.Timeout(), "%v", err)
	_ = heartbeatRaw.Close()
	t.Log("injected durable append pause caused heartbeat response timeout through coordinator/group locking; stable read remained available and commit remained unacknowledged")
	unblock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("commit did not finish after durable writer resumed")
	}
	_, recovered := dial()
	require.Contains(t, wireRequest(t, recovered, wire.CommandFetchOffset, []byte(fetch)), "offset=16 found=true")
	require.Contains(t, wireRequest(t, recovered, wire.CommandHeartbeat, []byte(command)), "OK cooperative=")
}
