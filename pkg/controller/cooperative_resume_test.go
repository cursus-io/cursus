package controller

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCooperativeResumePositionPreservesStabilityAndLegacyCursor(t *testing.T) {
	ch, tm, cd, _ := newDiskBackedTransactionHandler(t)
	generation := prepareTransactionGroup(t, tm, cd, "inputs", "workers", "worker")
	for i := 0; i < 16; i++ {
		require.NoError(t, tm.PublishToPartitionWithAck("inputs", 0, &types.Message{Payload: "record"}))
	}
	require.NoError(t, cd.CommitOffset("workers", "inputs", 0, 4))
	p, err := tm.GetTopic("inputs").GetPartition(0)
	require.NoError(t, err)
	args := CommonArgs{TopicName: "inputs", GroupName: "workers", MemberID: "worker", Generation: generation, PartitionID: 0, HasOffset: true, Offset: 8, ReadIsolation: ReadIsolationCommitted}
	for _, cooperative := range []bool{false, true} {
		t.Run(fmt.Sprint(cooperative), func(t *testing.T) {
			args.Ownership = nil
			if cooperative {
				// These internal cursor helpers do not authorize delivery. The Wire
				// integration covers real tokens and the final ownership fence.
				args.Ownership = &protocol.CooperativeToken{RegistrationEpoch: 1}
			}
			want := uint64(4)
			if cooperative {
				want = 8
			}
			offset, err := ch.resolveOffset(p, "inputs", args)
			require.NoError(t, err)
			require.Equal(t, want, offset)
			ctx := NewClientContext("workers", 0)
			messages, err := ch.readFromTopic("inputs", args, ctx, 1)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, want, messages[0].Offset)
			ctx.OffsetCache[consumerOffsetCacheKey("inputs", args)] = 12
			messages, err = ch.readFromTopic("inputs", args, ctx, 1)
			require.NoError(t, err)
			require.Equal(t, uint64(12), messages[0].Offset)
		})
	}
	args.Ownership = &protocol.CooperativeToken{RegistrationEpoch: 1}
	args.Offset = 2
	offset, err := ch.resolveOffset(p, "inputs", args)
	require.NoError(t, err)
	require.Equal(t, uint64(4), offset, "explicit cursor cannot rewind a committed position")
	args.Offset = 1000
	messages, err := ch.readFromTopic("inputs", args, NewClientContext("workers", 0), 1)
	require.NoError(t, err)
	require.Empty(t, messages, "a cursor beyond log end must not fall back to old data")
	offset, _ = cd.GetOffset("workers", "inputs", 0)
	require.Equal(t, uint64(4), offset, "reading ahead must not advance a durable commit")
	require.NoError(t, cd.PrepareOffsetReservation("workers", cd.GetRegistrationEpoch("workers"), coordinator.TransactionOffsetReservation{
		TransactionalID: "tx", ProducerID: "producer", MemberID: "worker", Generation: generation,
		Offsets: []coordinator.ReservedTransactionOffset{{Topic: "inputs", Partition: 0, Offset: 5}},
	}))
	_, err = ch.resolveOffset(p, "inputs", args)
	require.ErrorContains(t, err, "unstable_offset_commit")
	_, err = ch.readFromTopic("inputs", args, NewClientContext("workers", 0), 1)
	require.ErrorContains(t, err, "unstable_offset_commit")
}

func TestCooperativeBatchCommitRetainsEveryFence(t *testing.T) {
	ch, tm, cd, _ := newDiskBackedTransactionHandler(t)
	require.NoError(t, tm.CreateTopic("inputs", 2, false, false))
	require.NoError(t, cd.RegisterGroup("inputs", "workers", 2))
	epoch := tm.GetTopic("inputs").Definition().LifecycleEpoch
	parts := []coordinator.CooperativePartition{{Topic: "inputs", Partition: 0, LifecycleEpoch: epoch}, {Topic: "inputs", Partition: 1, LifecycleEpoch: epoch}}
	membership, err := cd.ReconcileCooperative("workers", "worker", 0, 0, parts, nil, false)
	require.NoError(t, err)
	tokens := []protocol.CooperativeToken{{RegistrationEpoch: membership.RegistrationEpoch, Grant: membership.Grants[0]}, {RegistrationEpoch: membership.RegistrationEpoch, Grant: membership.Grants[1]}}
	args := map[string]string{"group": "workers", "topic": "inputs", "member": "worker", "offsets": "P0:8,P1:8"}
	for _, change := range []struct {
		name   string
		mutate func(*protocol.CooperativeToken)
	}{
		{"registration", func(t *protocol.CooperativeToken) { t.RegistrationEpoch++ }},
		{"session", func(t *protocol.CooperativeToken) { t.Grant.Session++ }},
		{"grant", func(t *protocol.CooperativeToken) { t.Grant.Epoch++ }},
		{"lifecycle", func(t *protocol.CooperativeToken) { t.Grant.Partition.LifecycleEpoch++ }},
		{"partition", func(t *protocol.CooperativeToken) { t.Grant.Partition.Partition++ }},
		{"member", func(t *protocol.CooperativeToken) { t.Grant.Member = "other" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			invalid := append([]protocol.CooperativeToken(nil), tokens...)
			change.mutate(&invalid[1])
			args["ownerships"] = protocol.EncodeCooperative(invalid)
			require.Contains(t, ch.handleCooperativeBatchCommit(args), "ERROR:")
			_, found := cd.GetOffset("workers", "inputs", 0)
			require.False(t, found, "an invalid second grant must not publish the first offset")
		})
	}
	args["ownerships"] = protocol.EncodeCooperative(tokens)
	require.Equal(t, "OK batched=2", ch.handleCooperativeBatchCommit(args))
	for p := 0; p < 2; p++ {
		offset, found := cd.GetOffset("workers", "inputs", p)
		require.True(t, found)
		require.Equal(t, uint64(8), offset)
	}
}
