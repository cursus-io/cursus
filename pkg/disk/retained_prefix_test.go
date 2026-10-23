package disk

import (
	"context"
	"encoding/binary"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func prefixRecords(count int) []types.DiskMessage {
	records := make([]types.DiskMessage, count)
	for i := range records {
		records[i] = types.DiskMessage{Topic: "orders", Partition: 1, Offset: uint64(i), Payload: strings.Repeat("x", 512)}
	}
	return records
}

func TestRetainedPrefixWarmSuffixEvictionAndRestart(t *testing.T) {
	d, cfg := bootstrapTestHandler(t, 1)
	d.SegmentSize, d.IndexSize = 1<<30, 1<<30
	records := prefixRecords(600)
	require.NoError(t, d.WriteBatchSync(records))
	proof, err := d.RetainedPrefixContext(context.Background(), 0, 600)
	require.NoError(t, err)
	info, err := os.Stat(d.GetSegmentPath(0))
	require.NoError(t, err)
	cold := d.RetainedPrefixWorkSnapshot()
	require.Equal(t, uint64(info.Size()), cold.BytesRead)
	require.Equal(t, uint64(600), cold.DecodedFrames)
	repeated, err := d.RetainedPrefixContext(context.Background(), 0, 600)
	require.NoError(t, err)
	require.Equal(t, proof, repeated)
	require.Equal(t, cold.BytesRead, d.RetainedPrefixWorkSnapshot().BytesRead)
	require.Equal(t, cold.CanonicalBytes, d.RetainedPrefixWorkSnapshot().CanonicalBytes)
	// More historical probes than cache slots must not evict the suffix anchor.
	for end := uint64(1); end <= 150; end++ {
		_, err := d.RetainedPrefixContext(context.Background(), 0, end)
		require.NoError(t, err)
	}
	require.Len(t, d.retainedPrefixes, retainedPrefixCheckpointLimit)
	require.Equal(t, retainedPrefixCheckpointLimit, cap(d.retainedPrefixes))
	before := d.RetainedPrefixWorkSnapshot()
	_, err = d.RetainedPrefixContext(context.Background(), 0, 600)
	require.NoError(t, err)
	require.Equal(t, before.BytesRead, d.RetainedPrefixWorkSnapshot().BytesRead)
	_, err = d.RetainedPrefixContext(context.Background(), 0, 1)
	require.NoError(t, err)
	require.Greater(t, d.RetainedPrefixWorkSnapshot().BytesRead, before.BytesRead, "evicted history must be read again")
	before = d.RetainedPrefixWorkSnapshot()
	last := records[0]
	last.Offset = 600
	d.SegmentSize = 1 // force rotation while retaining the original generation
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{last}))
	extended, err := d.RetainedPrefixContext(context.Background(), 0, 601)
	require.NoError(t, err)
	require.Equal(t, proof.ContentGeneration, extended.ContentGeneration)
	require.Equal(t, uint64(1), d.RetainedPrefixWorkSnapshot().DecodedFrames-before.DecodedFrames)
	encoded, err := util.SerializeDiskMessage(last)
	require.NoError(t, err)
	require.Equal(t, uint64(len(encoded)+4), d.RetainedPrefixWorkSnapshot().BytesRead-before.BytesRead)
	require.NoError(t, d.Close())
	reopened, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.Empty(t, reopened.retainedPrefixes)
	rebuilt, err := reopened.RetainedPrefixContext(context.Background(), 0, 601)
	require.NoError(t, err)
	require.Equal(t, extended.Proof, rebuilt.Proof)
	require.NotEqual(t, extended.ContentGeneration, rebuilt.ContentGeneration)
	require.Equal(t, uint64(601), reopened.RetainedPrefixWorkSnapshot().DecodedFrames)
}

func TestRetainedPrefixMarkedHolesAndExactBounds(t *testing.T) {
	for _, offsets := range [][]uint64{nil, {2, 5}} {
		d, _ := bootstrapTestHandler(t, 1)
		var records []types.DiskMessage
		for _, offset := range offsets {
			records = append(records, types.DiskMessage{Topic: "orders", Partition: 1, Offset: offset, TransactionalID: "tx", TransactionState: types.TransactionStateAborted})
		}
		require.NoError(t, d.WriteCompactedReplicaRange(0, 10, records))
		previous := ""
		for end := uint64(1); end <= 10; end++ {
			got, err := d.RetainedPrefixContext(context.Background(), 0, end)
			require.NoError(t, err)
			require.True(t, got.UsedMarkedAuthority)
			require.Equal(t, end, got.Proof.End)
			require.NotEqual(t, previous, got.Proof.Digest, "trailing holes must bind the exact end")
			previous = got.Proof.Digest
			cold, _ := bootstrapTestHandler(t, 1)
			require.NoError(t, cold.WriteCompactedReplicaRange(0, 10, records))
			want, err := cold.RetainedPrefixContext(context.Background(), 0, end)
			require.NoError(t, err)
			require.Equal(t, want.Proof, got.Proof, "extending a cut within a hole must preserve the next physical record")
		}
		for _, bounds := range [][2]uint64{{1, 10}, {0, 11}, {5, 4}} {
			_, err := d.RetainedPrefixContext(context.Background(), bounds[0], bounds[1])
			require.Error(t, err)
		}
	}
	d, _ := bootstrapTestHandler(t, 1)
	empty, err := d.RetainedPrefixContext(context.Background(), 0, 0)
	require.NoError(t, err)
	require.Equal(t, uint32(1), empty.Proof.Version)
	require.Len(t, empty.Proof.Digest, 64)
	require.Equal(t, "83b7db7fc0e082e03418938c447b922ca0aefe9a1ddb206e0aed1402473415bb", empty.Proof.Digest)
	require.False(t, empty.UsedMarkedAuthority)
}

func TestRetainedPrefixCancellationPreservesOnlyCompletedChunks(t *testing.T) {
	for _, threshold := range []uint64{5, 300, 600} {
		d, _ := bootstrapTestHandler(t, 1)
		d.SegmentSize, d.IndexSize = 1<<30, 1<<30
		require.NoError(t, d.WriteBatchSync(prefixRecords(600)))
		ctx := retainedPageTestContext{Context: context.Background(), check: func() error {
			if d.retainedPrefixWork.DecodedFrames >= threshold {
				return context.Canceled
			}
			return nil
		}}
		got, err := d.RetainedPrefixContext(ctx, 0, 600)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, got.Proof.Digest)
		var committed uint64
		for _, checkpoint := range d.retainedPrefixes {
			committed = max(committed, checkpoint.cursor.End)
		}
		require.Equal(t, (threshold-1)/256*256, committed)
		failed := d.RetainedPrefixWorkSnapshot()
		require.Greater(t, failed.BytesRead, uint64(0), "discarded reads must remain charged")
		_, err = d.RetainedPrefixContext(context.Background(), 0, 600)
		require.NoError(t, err)
		require.Equal(t, 600-committed, d.RetainedPrefixWorkSnapshot().DecodedFrames-failed.DecodedFrames)
	}
	d, _ := bootstrapTestHandler(t, 1)
	d.retainedPrefixMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := d.RetainedPrefixContext(ctx, 0, 0)
	d.retainedPrefixMu.Unlock()
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRetainedPrefixFencesMutationAndDestructiveABA(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	records := prefixRecords(2)
	require.NoError(t, d.WriteBatchSync(records))
	mutated := false
	ctx := retainedPageTestContext{Context: context.Background(), check: func() error {
		if !mutated && d.retainedPrefixWork.DecodedFrames != 0 && d.mu.TryLock() {
			mutated = true
			if err := d.advanceContentGenerationLocked(); err != nil {
				t.Fatal(err)
			}
			d.mu.Unlock()
		}
		return nil
	}}
	got, err := d.RetainedPrefixContext(ctx, 0, 2)
	require.True(t, mutated, "hashing must release storage locks")
	require.ErrorIs(t, err, ErrStaleContentGeneration)
	require.Empty(t, got.Proof.Digest)
	old, err := d.RetainedPrefixContext(context.Background(), 0, 2)
	require.NoError(t, err)
	require.NoError(t, d.TruncateTo(0))
	records[0].Payload = "replacement"
	require.NoError(t, d.WriteBatchSync(records))
	replacement, err := d.RetainedPrefixContext(context.Background(), 0, 2)
	require.NoError(t, err)
	require.NotEqual(t, old.Proof, replacement.Proof)
	require.ErrorIs(t, d.CheckContentGeneration(old.ContentGeneration), ErrStaleContentGeneration)
	require.NoError(t, d.Close())
	_, err = d.RetainedPrefixContext(context.Background(), 0, 2)
	require.Error(t, err)
}

func TestRetainedPrefixCanonicalFieldsAndLayouts(t *testing.T) {
	zero := types.DiskMessage{}
	baseline, _, err := retainedPrefixHash(retainedPrefixSeed(0), zero)
	require.NoError(t, err)
	for i := 0; i < reflect.TypeOf(zero).NumField(); i++ {
		message := zero
		value := reflect.ValueOf(&message).Elem().Field(i)
		switch value.Kind() {
		case reflect.String:
			value.SetString("changed")
		case reflect.Int16, reflect.Int32, reflect.Int64:
			value.SetInt(1)
		case reflect.Uint32, reflect.Uint64:
			value.SetUint(1)
		case reflect.Slice:
			value.SetBytes([]byte{1})
		default:
			t.Fatalf("uncovered persisted field %s", reflect.TypeOf(zero).Field(i).Name)
		}
		changed, _, err := retainedPrefixHash(retainedPrefixSeed(0), message)
		require.NoError(t, err)
		require.NotEqual(t, baseline, changed, reflect.TypeOf(zero).Field(i).Name)
	}
	var previous RetainedPrefix
	for _, size := range []uint64{1 << 30, 1} {
		d, _ := bootstrapTestHandler(t, 1)
		d.SegmentSize = size
		for _, record := range prefixRecords(5) {
			require.NoError(t, d.WriteBatchSync([]types.DiskMessage{record}))
		}
		proof, err := d.RetainedPrefixContext(context.Background(), 0, 5)
		require.NoError(t, err)
		if previous.Digest != "" {
			require.Equal(t, previous, proof.Proof)
		}
		previous = proof.Proof
	}
}

func TestRetainedPrefixStrictValidationAndFixedCut(t *testing.T) {
	for _, damage := range []string{"topic", "partition", "order", "hole", "partial-frame", "partial-header", "bad-length"} {
		t.Run(damage, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			records := prefixRecords(3)
			require.NoError(t, d.WriteBatchSync(records))
			switch damage {
			case "topic":
				records[1].Topic = "foreign"
			case "partition":
				records[1].Partition = 2
			case "order":
				records[1].Offset = 0
			case "hole":
				records = append(records[:1], records[2])
			}
			var data []byte
			for _, record := range records {
				encoded, err := util.SerializeDiskMessage(record)
				require.NoError(t, err)
				data = binary.BigEndian.AppendUint32(data, uint32(len(encoded)))
				data = append(data, encoded...)
			}
			switch damage {
			case "partial-frame":
				data = data[:len(data)-1]
			case "partial-header":
				data = append(data, 0)
			case "bad-length":
				binary.BigEndian.PutUint32(data[:4], MaxMessageSize+1)
			}
			require.NoError(t, os.WriteFile(d.GetSegmentPath(0), data, 0o600))
			proof, err := d.RetainedPrefixContext(context.Background(), 0, 3)
			require.Error(t, err)
			require.Empty(t, proof.Proof.Digest)
			if damage == "partial-header" || damage == "partial-frame" {
				_, err := d.RetainedPrefixContext(context.Background(), 0, 1)
				require.NoError(t, err, "a fixed earlier cut must not depend on later damaged bytes")
			}
		})
	}
}
