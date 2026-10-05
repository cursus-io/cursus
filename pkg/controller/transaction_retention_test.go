package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

func TestTransactionalIDReuseAfterRetentionAndCompleteDiskRestart(t *testing.T) {
	testTransactionRetentionRestart(t, false)
}

func TestLegacyEmptyJournalUpgradePreservesRetainedTransactionIdentity(t *testing.T) {
	testTransactionRetentionRestart(t, true)
}

func testTransactionRetentionRestart(t *testing.T, legacyEmpty bool) {
	for _, firstDecision := range []string{"commit", "abort"} {
		t.Run(firstDecision, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir, cfg.IndexSize, cfg.DiskFlushIntervalMS = t.TempDir(), 1024, 1
			var ch *CommandHandler
			var tm *topic.TopicManager
			var cd *coordinator.Coordinator
			var dm *disk.DiskManager
			open := func(restore bool) {
				dm = disk.NewDiskManager(cfg)
				tm = topic.NewTopicManager(cfg, dm, nil)
				if restore {
					require.NoError(t, tm.RestoreTopics())
				}
				var err error
				cd, err = coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, tm)
				require.NoError(t, err)
				ch = NewCommandHandler(tm, cfg, cd, nil, nil)
				require.NoError(t, ch.ConfigureTransactionJournal(filepath.Join(cfg.LogDir, "transactions.journal")))
			}
			closeAll := func() {
				require.NoError(t, ch.Close())
				cd.Stop()
				tm.Stop()
				dm.CloseAllHandlers()
			}
			open(false)
			t.Cleanup(closeAll)
			generation := prepareTransactionGroup(t, tm, cd, "input", "workers", "worker")
			require.NoError(t, tm.CreateTopic("output", 1, false, false))
			require.NoError(t, cd.CommitOffset("workers", "input", 0, 0))
			ctx := NewClientContext("", 0)
			command := func(cmd string) string {
				response := ch.HandleCommand(cmd, ctx)
				require.True(t, strings.HasPrefix(response, "OK "), "%s: %s", cmd, response)
				return response
			}
			previousEpoch := int64(-1)
			previousProducer := ""
			want := make([]string, 0)
			for cycle := range 3 {
				fields := parseKeyValueArgs(strings.TrimPrefix(command("INIT_PRODUCER_ID transactional_id=retained-id"), "OK "))
				epoch, err := strconv.ParseInt(fields["epoch"], 10, 64)
				require.NoError(t, err)
				require.Greater(t, epoch, previousEpoch)
				if cycle > 0 {
					require.NotEqual(t, previousProducer, fields["producerId"], "fully retired producer identity must not be reused")
					stale := ch.HandleCommand(fmt.Sprintf("BEGIN_TXN transactional_id=retained-id producerId=%s epoch=%d", fields["producerId"], previousEpoch), ctx)
					require.False(t, strings.HasPrefix(stale, "OK "), "retired producer must remain fenced")
				}
				identity := fmt.Sprintf("transactional_id=retained-id producerId=%s epoch=%d", fields["producerId"], epoch)
				command("BEGIN_TXN " + identity)
				payload := fmt.Sprintf("after-retention-%d", cycle)
				command("TXN_PUBLISH " + identity + " topic=output partition=0 seqNum=1 message=" + payload)
				command(fmt.Sprintf("SEND_OFFSETS_TO_TXN %s topic=input group=workers member=worker generation=%d offsets=P0:%d", identity, generation, cycle+1))
				decision := "commit"
				if cycle == 0 {
					decision = firstDecision
				}
				end := "END_TXN " + identity + " result=" + decision
				command(end)
				command(end) // A lost acknowledgement must not duplicate output.
				if decision == "commit" {
					want = append(want, payload)
				}
				require.Equal(t, want, readCommittedPayloads(t, tm, "output"))
				wantOffset := cycle + 1
				if decision == "abort" {
					wantOffset = 0
				}
				require.Equal(t, fmt.Sprintf("OK offset=%d", wantOffset), ch.handleFetchOffset("FETCH_OFFSET topic=input partition=0 group=workers"))
				previousEpoch = epoch
				previousProducer = fields["producerId"]
				if cycle == 2 {
					break
				}
				now := time.Now()
				require.Equal(t, 1, ch.TxnManager.PruneExpired(now.Add(30*24*time.Hour)))
				require.Equal(t, 1, ch.TxnManager.PruneExpired(now.Add(60*24*time.Hour)))
				require.Empty(t, ch.TxnManager.ExportState())
				require.NoError(t, ch.txnJournal.Rewrite(ch.TxnManager.ExportState()))
				closeAll()
				if legacyEmpty {
					// Version one compacted away the last transaction tombstone
					// without retaining an allocator watermark.
					require.NoError(t, os.WriteFile(filepath.Join(cfg.LogDir, "transactions.journal"), nil, 0o600))
				}
				open(true)
				require.Equal(t, want, readCommittedPayloads(t, tm, "output"))
				generation = cd.GetGeneration("workers")
			}
		})
	}
}
