package disk

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func openTransactionHistoryDisks(t *testing.T, cfg *config.Config, populate bool) *consumerHistoryDiskReader {
	r := openConsumerHistoryDisks(t, cfg, 41, false)
	if !populate {
		return r
	}
	for p, d := range r.handlers {
		group, id := fmt.Sprintf("g%d", p), fmt.Sprintf("tx%d", p)
		var batch []types.DiskMessage
		appendRecord := func(record coordinator.ConsumerMetadataRecord) {
			payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
			require.NoError(t, err)
			m := types.DiskMessage{Topic: config.ConsumerOffsetsTopicName, Partition: int32(p), Offset: uint64(len(batch)), Payload: string(payload), Key: key, ProducerID: "ordinary"}
			if record.TransactionalID != "" {
				m.ProducerID = record.ProducerID
				m.Epoch = record.ProducerEpoch
				m.SeqNum = 1
				m.TransactionalID = id
				m.TransactionState = types.TransactionStateOpen
			}
			batch = append(batch, m)
		}
		appendRecord(coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordRegistration, Group: group, Topic: "events", Epoch: 1, PartitionCount: 1})
		appendRecord(coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: group, Topic: "events", Epoch: 1, Revision: 1, Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 1}}})
		for epoch := int64(1); epoch <= 20; epoch++ {
			appendRecord(coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersionTransactionHistory, Type: coordinator.ConsumerMetadataRecordTransactionalOffsetSnapshot, Group: group, Topic: "events", Epoch: 1, Revision: uint64(epoch), Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 1}}, TransactionalID: id, ProducerID: "producer", ProducerEpoch: epoch, CoordinatorEpoch: 7})
			key, value, err := types.TransactionMarkerControlBytes(types.TransactionMarkerAbort, 7)
			require.NoError(t, err)
			batch = append(batch, types.DiskMessage{Topic: config.ConsumerOffsetsTopicName, Partition: int32(p), Offset: uint64(len(batch)), Payload: types.TransactionControlMarkerPayload, ProducerID: fmt.Sprintf("txn-history:%s:abort", id), Epoch: epoch, SeqNum: 7, TransactionalID: id, TransactionState: types.TransactionStateAborted, TransactionMarker: types.TransactionMarkerAbort, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 7, ControlBatchKey: key, ControlBatchValue: value})
		}
		require.NoError(t, d.WriteBatchSync(batch))
		rollCompactionSegment(t, d)
		require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: config.ConsumerOffsetsTopicName, Partition: int32(p), Offset: r.end, Payload: "uncommitted tail"}}))
	}
	return r
}

func TestConsumerHistoryTransactionCrashStages(t *testing.T) {
	if stage := os.Getenv("CURSUS_TRANSACTION_HISTORY_CRASH_STAGE"); stage != "" {
		cfg := consumerHistoryDiskConfig(t, os.Getenv("CURSUS_TRANSACTION_HISTORY_CRASH_DIR"))
		r := openTransactionHistoryDisks(t, cfg, false)
		c := historyDiskCoordinator(t, cfg, r)
		_, _, _, err := certifyAndRewriteHistorySegment(t, c, r, 0, func(current string) error {
			if current == stage {
				os.Exit(77)
			}
			return nil
		})
		t.Fatalf("crash stage not reached: %v", err)
	}
	for _, stage := range []string{"written", "log_synced", "index_synced", "marker_synced", "generation_invalidated", "marker_published", "log_published", "index_published", "directory_synced"} {
		t.Run(stage, func(t *testing.T) {
			cfg := consumerHistoryDiskConfig(t, t.TempDir())
			r := openTransactionHistoryDisks(t, cfg, true)
			closeConsumerHistoryDisks(t, r)
			command := exec.Command(os.Args[0], "-test.run=^TestConsumerHistoryTransactionCrashStages$") // #nosec G204 -- reexecutes this test binary with a test-owned directory.
			command.Env = append(os.Environ(), "CURSUS_TRANSACTION_HISTORY_CRASH_STAGE="+stage, "CURSUS_TRANSACTION_HISTORY_CRASH_DIR="+cfg.LogDir)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(output))
			require.Equal(t, 77, exit.ExitCode(), string(output))
			// Reopen twice, proving interrupted recovery does not require a cache.
			for attempt := 0; attempt < 2; attempt++ {
				r = openTransactionHistoryDisks(t, cfg, false)
				c := historyDiskCoordinator(t, cfg, r)
				replay, err := c.PrepareFullConsumerMetadataReplay(r)
				require.NoError(t, err)
				require.NoError(t, c.ApplyFullConsumerMetadataReplayAndComplete(replay))
				require.Equal(t, uint64(1), c.ExportState()["g0"].Offsets["events"][0])
				records, err := r.handlers[0].ReadMessages(0, 100)
				require.NoError(t, err)
				require.Contains(t, []int{5, 43}, len(records), "publication must expose the old or new complete segment")
				require.Equal(t, uint64(43), r.handlers[0].GetAbsoluteOffset())
				require.Equal(t, "uncommitted tail", records[len(records)-1].Payload)
				// Complete another certified pass after crash; exactly four committed
				// witnesses and the untouched uncommitted tail must remain.
				plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, r.end)
				require.NoError(t, err)
				segments, err := r.handlers[0].ConsumerHistorySegments(context.Background(), r.end)
				require.NoError(t, err)
				_, err = r.handlers[0].RewriteCertifiedConsumerHistory(context.Background(), segments[0], plan)
				require.NoError(t, err)
				records, err = r.handlers[0].ReadMessages(0, 100)
				require.NoError(t, err)
				require.Len(t, records, 5)
				closeConsumerHistoryDisks(t, r)
			}
		})
	}
}
