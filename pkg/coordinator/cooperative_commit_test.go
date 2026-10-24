package coordinator

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/stretchr/testify/require"
)

func TestCooperativeBatchCommitAtomicFencingAndDurability(t *testing.T) {
	c, parts, records := runtimeCooperativeFixture(t, 6)
	a := runtimeJoin(t, c, parts, "a")
	tokens := make([]protocol.CooperativeToken, len(a.Grants))
	offsets := make([]OffsetItem, len(a.Grants))
	for i, grant := range a.Grants {
		tokens[i] = protocol.CooperativeToken{RegistrationEpoch: a.RegistrationEpoch, Grant: grant}
		offsets[i] = OffsetItem{Partition: grant.Partition.Partition, Offset: 8}
	}
	before := len(*records)
	require.NoError(t, c.CommitCooperativeBatch("workers", tokens, offsets))
	require.Len(t, *records, before+1, "six offsets share one durable snapshot")
	require.NoError(t, c.CommitCooperativeBatch("workers", tokens, offsets))
	require.Len(t, *records, before+1, "retry after lost success response is a no-op")
	for i := range offsets {
		offsets[i].Offset = 16
	}
	bad := append([]protocol.CooperativeToken(nil), tokens...)
	bad[len(bad)-1].Grant.Epoch++
	require.ErrorContains(t, c.CommitCooperativeBatch("workers", bad, offsets), "cooperative_fenced")
	require.Len(t, *records, before+1)
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return errors.New("append unavailable") })
	require.ErrorContains(t, c.CommitCooperativeBatch("workers", tokens, offsets), "append unavailable")
	for _, offset := range offsets {
		current, ok := c.GetOffset("workers", "orders", offset.Partition)
		require.True(t, ok)
		require.Equal(t, uint64(8), current, "failed batch must not publish any partition")
	}
	c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error { *records = append(*records, record); return nil })
	reservation := TransactionOffsetReservation{TransactionalID: "tx", ProducerID: "p", ProducerEpoch: 1, MemberID: "a", Offsets: []ReservedTransactionOffset{{Topic: "orders", Partition: 1, Offset: 9}}, Grants: []CooperativeGrant{tokens[1].Grant}}
	require.NoError(t, c.PrepareOffsetReservation("workers", a.RegistrationEpoch, reservation))
	require.ErrorContains(t, c.CommitCooperativeBatch("workers", tokens, offsets), "unstable_offset_commit")
	current, _ := c.GetOffset("workers", "orders", 0)
	require.Equal(t, uint64(8), current, "reservation rejection cannot partially publish the batch")
	require.NoError(t, c.ResolveOffsetReservation("workers", a.RegistrationEpoch, "tx", "p", 1, false))
	runtimeJoin(t, c, parts, "b")
	a = runtimeHeartbeat(t, c, parts, a, nil)
	require.NoError(t, c.CommitCooperativeBatch("workers", tokens, offsets), "pending revoke still permits finishing accepted work")
	runtimeHeartbeat(t, c, parts, a, membershipAcks(a))
	for i := range offsets {
		offsets[i].Offset = 24
	}
	require.ErrorContains(t, c.CommitCooperativeBatch("workers", tokens, offsets), "cooperative_fenced")
	current, _ = c.GetOffset("workers", "orders", 0)
	require.Equal(t, uint64(16), current, "stale moved grant rejects retained offsets in same batch")
	require.NoError(t, c.CommitCooperativeBatch("workers", tokens[:1], offsets[:1]), "retained partition can retry independently")
}

func TestCooperativeBatchCommitRejectsInvalidScope(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 4)
	a := runtimeJoin(t, c, parts, "a")
	first := protocol.CooperativeToken{RegistrationEpoch: a.RegistrationEpoch, Grant: a.Grants[0]}
	second := protocol.CooperativeToken{RegistrationEpoch: a.RegistrationEpoch, Grant: a.Grants[1]}
	for _, mutate := range []func(*protocol.CooperativeToken){
		func(t *protocol.CooperativeToken) { t.RegistrationEpoch++ },
		func(t *protocol.CooperativeToken) { t.Grant.Member = "b" },
		func(t *protocol.CooperativeToken) { t.Grant.Session++ },
		func(t *protocol.CooperativeToken) { t.Grant.Partition.Topic = "other" },
		func(t *protocol.CooperativeToken) { t.Grant.Partition.Partition = 0 },
	} {
		bad := second
		mutate(&bad)
		require.Error(t, c.CommitCooperativeBatch("workers", []protocol.CooperativeToken{first, bad}, []OffsetItem{{Partition: 0, Offset: 8}, {Partition: 1, Offset: 8}}))
	}
	require.Error(t, c.CommitCooperativeBatch("workers", nil, nil))
	require.Error(t, c.CommitCooperativeBatch("workers", []protocol.CooperativeToken{first, first}, []OffsetItem{{Partition: 0, Offset: 8}, {Partition: 0, Offset: 9}}))
}
