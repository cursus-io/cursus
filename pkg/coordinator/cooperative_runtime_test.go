package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func runtimeCooperativeFixture(t *testing.T, count int) (*Coordinator, []CooperativePartition, *[]ConsumerMetadataRecord) {
	t.Helper()
	c, err := NewCoordinatorWithRecovery(context.Background(), config.DefaultConfig(), &metadataReplayHandler{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	records := new([]ConsumerMetadataRecord)
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error { *records = append(*records, record); return nil })
	require.NoError(t, c.RegisterGroup("orders", "workers", count))
	parts := make([]CooperativePartition, count)
	for i := range parts {
		parts[i] = CooperativePartition{Topic: "orders", Partition: i, LifecycleEpoch: 1}
	}
	return c, parts, records
}

func runtimeJoin(t *testing.T, c *Coordinator, parts []CooperativePartition, member string) CooperativeMembership {
	t.Helper()
	m, err := c.ReconcileCooperative("workers", member, 0, 0, parts, nil, false)
	require.NoError(t, err)
	return m
}
func runtimeHeartbeat(t *testing.T, c *Coordinator, parts []CooperativePartition, m CooperativeMembership, acks []CooperativeRevocation) CooperativeMembership {
	t.Helper()
	result, err := c.ReconcileCooperative("workers", m.Member, m.RegistrationEpoch, m.Session, parts, acks, false)
	require.NoError(t, err)
	return result
}
func membershipAcks(m CooperativeMembership) []CooperativeRevocation {
	var result []CooperativeRevocation
	for _, g := range m.Grants {
		if g.RevokeRevision > 0 {
			result = append(result, CooperativeRevocation{Partition: g.Partition, GrantEpoch: g.Epoch, RevokeRevision: g.RevokeRevision})
		}
	}
	return result
}

func TestCooperativeRuntimeHandoffRecoveryAndRecoveryWork(t *testing.T) {
	c, parts, records := runtimeCooperativeFixture(t, 120)
	a := runtimeJoin(t, c, parts, "a")
	b := runtimeJoin(t, c, parts, "b")
	require.Empty(t, b.Grants)
	a = runtimeHeartbeat(t, c, parts, a, nil)
	acks := membershipAcks(a)
	require.Len(t, acks, 60)
	writes := len(*records)
	for range 20 {
		runtimeHeartbeat(t, c, parts, a, nil)
	}
	require.Len(t, *records, writes, "unchanged heartbeats do not rewrite ownership")
	old := a.Grants[len(a.Grants)-1]
	require.NoError(t, c.CommitCooperative("workers", a.RegistrationEpoch, old, 10), "old owner may finish before ACK")
	var messages []types.Message
	bytes := 0
	for i, record := range *records {
		message := encodedMetadataMessage(t, record, uint64(i))
		bytes += len(message.Payload)
		messages = append(messages, message)
	}
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	recovered, err := NewCoordinatorWithRecovery(context.Background(), cfg, &metadataReplayHandler{messages: map[int][]types.Message{0: messages}})
	require.NoError(t, err)
	t.Cleanup(recovered.Stop)
	recovered.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
	before := c.ExportState()["workers"].Cooperative
	require.Equal(t, before, recovered.ExportState()["workers"].Cooperative)
	a = runtimeHeartbeat(t, recovered, parts, a, acks)
	b = runtimeHeartbeat(t, recovered, parts, b, nil)
	require.Len(t, a.Grants, 60)
	require.Len(t, b.Grants, 60)
	require.ErrorContains(t, recovered.CommitCooperative("workers", a.RegistrationEpoch, old, 11), "cooperative_fenced")
	require.ErrorContains(t, recovered.ValidateCooperativeRead("workers", a.RegistrationEpoch, &old), "cooperative_fenced")
	runtimeHeartbeat(t, recovered, parts, a, acks)
	require.Len(t, recovered.ExportState()["workers"].Cooperative.Current, 120)
	snapshot, err := json.Marshal(recovered.ExportState())
	require.NoError(t, err)
	t.Logf("replay_records=%d replay_payload_bytes=%d decoded_grants=%d snapshot_bytes=%d moved_partitions=60 retained_partitions=60", len(messages), bytes, 240, len(snapshot))
}

func TestCooperativeRuntimeFailedWriteDoesNotPublish(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 4)
	a := runtimeJoin(t, c, parts, "a")
	before := c.ExportState()["workers"].Cooperative
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return errors.New("append unavailable") })
	_, err := c.ReconcileCooperative("workers", "b", 0, 0, parts, nil, false)
	require.ErrorContains(t, err, "append unavailable")
	require.Equal(t, before, c.ExportState()["workers"].Cooperative)
	require.ErrorContains(t, c.ValidateCooperativeRead("workers", a.RegistrationEpoch, &a.Grants[0]), "coordinator_not_available")
	_, err = c.ReconcileCooperative("workers", "c", 0, 0, parts, nil, false)
	require.Error(t, err, "ambiguous append blocks revision reuse until recovery")
}

func TestCooperativeRuntimeExpirationAndLifecycleFence(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 4)
	a := runtimeJoin(t, c, parts, "a")
	b := runtimeJoin(t, c, parts, "b")
	c.mu.Lock()
	c.groups["workers"].Members["a"].LastHeartbeat = time.Now().Add(-time.Hour)
	c.mu.Unlock()
	require.NoError(t, c.expireCooperative("workers", time.Minute))
	b = runtimeHeartbeat(t, c, parts, b, nil)
	require.Len(t, b.Grants, 4)
	_, err := c.ReconcileCooperative("workers", "a", a.RegistrationEpoch, a.Session, parts, nil, false)
	require.ErrorContains(t, err, "cooperative_fenced")
	before := b.Grants[0]
	for i := range parts {
		parts[i].LifecycleEpoch++
	}
	b = runtimeHeartbeat(t, c, parts, b, nil)
	require.Len(t, b.Grants, 4)
	require.ErrorContains(t, c.ValidateCooperativeRead("workers", b.RegistrationEpoch, &before), "cooperative_fenced")
	a = runtimeJoin(t, c, parts, "a")
	require.Greater(t, a.Session, before.Session)
}

func TestCooperativeRuntimeTransactionBarrier(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			c, parts, _ := runtimeCooperativeFixture(t, 2)
			a := runtimeJoin(t, c, parts, "a")
			grant := a.Grants[1]
			reservation := TransactionOffsetReservation{TransactionalID: "tx", ProducerID: "p", ProducerEpoch: 1, MemberID: "a", Generation: 0, Offsets: []ReservedTransactionOffset{{Topic: "orders", Partition: 1, Offset: 7}}, Grants: []CooperativeGrant{grant}}
			require.NoError(t, c.PrepareOffsetReservation("workers", a.RegistrationEpoch, reservation))
			b := runtimeJoin(t, c, parts, "b")
			a = runtimeHeartbeat(t, c, parts, a, nil)
			a = runtimeHeartbeat(t, c, parts, a, membershipAcks(a))
			b = runtimeHeartbeat(t, c, parts, b, nil)
			require.Empty(t, b.Grants, "reservation blocks transfer after quiescence")
			require.NoError(t, c.PrepareOffsetReservation("workers", a.RegistrationEpoch, reservation), "durable retry remains valid after revocation")
			require.NoError(t, c.ResolveOffsetReservation("workers", a.RegistrationEpoch, "tx", "p", 1, commit))
			b = runtimeHeartbeat(t, c, parts, b, nil)
			require.Len(t, b.Grants, 1)
			value, found := c.GetOffset("workers", "orders", 1)
			if commit {
				require.True(t, found)
				require.Equal(t, uint64(7), value)
			} else {
				require.False(t, found)
			}
			reservation.TransactionalID = "stale"
			require.ErrorContains(t, c.PrepareOffsetReservation("workers", a.RegistrationEpoch, reservation), "cooperative_fenced")
		})
	}
}

func TestCooperativeRuntimeRetainedGrantDuringDurableHandoff(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 4)
	a := runtimeJoin(t, c, parts, "a")
	runtimeJoin(t, c, parts, "b")
	a = runtimeHeartbeat(t, c, parts, a, nil)
	retained, moving := a.Grants[0], a.Grants[3]
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(resume) })
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
		if record.Type == ConsumerMetadataRecordLifecycleSnapshot {
			close(entered)
			<-resume
		}
		return nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := c.ReconcileCooperative("workers", a.Member, a.RegistrationEpoch, a.Session, parts, membershipAcks(a), false)
		done <- err
	}()
	<-entered
	require.NoError(t, c.ValidateCooperativeRead("workers", a.RegistrationEpoch, &retained))
	require.NoError(t, c.CommitCooperative("workers", a.RegistrationEpoch, retained, 5))
	require.ErrorContains(t, c.ValidateCooperativeRead("workers", a.RegistrationEpoch, &moving), "coordinator_not_available")
	require.ErrorContains(t, c.CommitCooperative("workers", a.RegistrationEpoch, moving, 5), "coordinator_not_available")
	once.Do(func() { close(resume) })
	require.NoError(t, <-done)
	require.ErrorContains(t, c.ValidateCooperativeRead("workers", a.RegistrationEpoch, &moving), "cooperative_fenced")
	offset, found := c.GetOffset("workers", "orders", 0)
	require.True(t, found)
	require.Equal(t, uint64(5), offset)
}

func TestCooperativeRuntimeRejectsNegativeLegacyGeneration(t *testing.T) {
	c, parts, records := runtimeCooperativeFixture(t, 1)
	c.mu.Lock()
	c.groups["workers"].Generation = -1
	c.mu.Unlock()
	writes := len(*records)
	_, err := c.ReconcileCooperative("workers", "a", 0, 0, parts, nil, false)
	require.ErrorContains(t, err, "invalid_generation")
	require.Nil(t, c.ExportGroupState("workers").Cooperative)
	require.Len(t, *records, writes)
}
