package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type replayIndexTestReader struct {
	*strictReplayReader
	hidePending            bool
	identity               string
	reads, rawReads, syncs int
	syncErr                error
	onRaw                  func()
}

func writeTestReplayIndex(t *testing.T, path string, saved consumerReplayIndex) {
	t.Helper()
	payload, err := json.Marshal(saved)
	require.NoError(t, err)
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(cleanupCheckpointEnvelope{payload, hex.EncodeToString(sum[:])})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
}

func (r *replayIndexTestReader) ConsumerMetadataIndexIdentity(int) (string, error) {
	return r.identity, nil
}
func (r *replayIndexTestReader) ReadCommittedConsumerMetadata(topic string, p int, next uint64, max int) ([]types.Message, error) {
	messages, err := r.ReadTopicPartition(topic, p, next, max)
	if r.hidePending {
		visible := make([]types.Message, 0, len(messages))
		for _, m := range messages {
			if m.TransactionState != types.TransactionStateOpen {
				visible = append(visible, m)
			}
		}
		messages = visible
	}
	r.reads += len(messages)
	return messages, err
}
func (r *replayIndexTestReader) ReadRawConsumerMetadataIndex(p int, next uint64, max int) ([]types.Message, error) {
	if r.onRaw != nil {
		r.onRaw()
	}
	messages, err := r.ReadTopicPartition("", p, next, max)
	end, _ := r.EndTopicOffset("", p)
	for i, m := range messages {
		if m.Offset >= end {
			messages = messages[:i]
			break
		}
	}
	r.rawReads += len(messages)
	return messages, err
}
func (r *replayIndexTestReader) SyncConsumerMetadataIndex() error { r.syncs++; return r.syncErr }

func newIndexFixture(t *testing.T, history int) (*Coordinator, *replayIndexTestReader) {
	t.Helper()
	c := newAuthorityTestCoordinator(t)
	c.cfg.LogDir = t.TempDir()
	c.cfg.ConsumerRecoveryIndexEnabled = true
	r := &replayIndexTestReader{strictReplayReader: strictReader(c), identity: "instance:lifecycle:durable-generation"}
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayRegistration("g", 1), 0))
	for i := 1; i <= history; i++ {
		r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayOffset("g", 1, uint64(i), uint64(i)), uint64(i)))
	}
	acquireReplay(t, c)
	return c, r
}

func TestConsumerReplayIndexSuffixAndBounds(t *testing.T) {
	c, r := newIndexFixture(t, 100)
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.False(t, p.status.IndexHit)
	require.Empty(t, p.status.IndexFailure)
	require.Positive(t, p.status.IndexWriteBytes)
	require.Equal(t, 101, r.reads)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
	r.reads, r.rawReads = 0, 0
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayOffset("g", 1, 101, 101), 101))
	// A physically present, uncommitted tail must never enter the projection.
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayOffset("g", 1, 102, 999), 102))
	r.end = map[int]uint64{0: 102}
	// The committed adapter must itself respect its end, as production does.
	r.read = func(p int, next uint64, max int) ([]types.Message, error) {
		var out []types.Message
		end, _ := r.EndTopicOffset("", p)
		for _, m := range r.messages[p] {
			if m.Offset >= next && m.Offset < end && len(out) < max {
				out = append(out, m)
			}
		}
		return out, nil
	}
	p, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.True(t, p.status.IndexHit)
	require.Empty(t, p.status.IndexFailure)
	require.Equal(t, 1, r.reads)
	require.Equal(t, 1, r.rawReads)
	require.Equal(t, uint64(101), p.scratch.groups["g"].Offsets["events"][0])
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
	// Restart a coordinator with the same compatibility authority and log.
	c2 := newAuthorityTestCoordinator(t)
	c2.cfg.LogDir = c.cfg.LogDir
	c2.cfg.ConsumerRecoveryIndexEnabled = true
	acquireReplay(t, c2)
	r.reads, r.rawReads = 0, 0
	p, err = c2.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.True(t, p.status.IndexHit)
	require.Zero(t, r.reads)
	require.Zero(t, r.rawReads)
	require.Equal(t, uint64(101), p.scratch.groups["g"].Offsets["events"][0])
}

func TestConsumerReplayIndexInvalidation(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "version", "generation", "rollback", "baseline", "sync failure"} {
		t.Run(kind, func(t *testing.T) {
			c, r := newIndexFixture(t, 10)
			p, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.Empty(t, p.status.IndexFailure)
			path := filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")
			switch kind {
			case "missing":
				require.NoError(t, os.Remove(path))
			case "corrupt":
				require.NoError(t, os.WriteFile(path, []byte("torn"), 0600))
			case "version":
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				var envelope cleanupCheckpointEnvelope
				require.NoError(t, json.Unmarshal(data, &envelope))
				var saved consumerReplayIndex
				require.NoError(t, json.Unmarshal(envelope.Payload, &saved))
				saved.Version++
				writeTestReplayIndex(t, path, saved)
			case "generation":
				r.identity = "replacement"
			case "rollback":
				r.messages[0] = r.messages[0][:5]
			case "baseline":
				require.NoError(t, c.RegisterGroupFromFSM("events", "compatibility", 1))
			case "sync failure":
				require.NoError(t, os.Remove(path))
				r.syncErr = errors.New("injected source fsync failure")
			}
			r.reads = 0
			p, err = c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.False(t, p.status.IndexHit)
			require.Positive(t, r.reads)
			if kind == "sync failure" {
				require.Contains(t, p.status.IndexFailure, "fsync")
				_, err = os.Stat(path)
				require.True(t, os.IsNotExist(err))
			}
		})
	}
}

func TestConsumerReplayIndexOldSuffixCannotHideCorruption(t *testing.T) {
	for _, record := range []ConsumerMetadataRecord{replayOffset("g", 1, 2, 99), replayOffset("g", 1, 11, 2)} {
		c, r := newIndexFixture(t, 10)
		_, err := c.PrepareFullConsumerMetadataReplay(r)
		require.NoError(t, err)
		r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, record, 11))
		_, err = c.PrepareFullConsumerMetadataReplay(r)
		require.Error(t, err)
	}
}

func TestConsumerReplayIndexCrossPartitionSuffixFallsBack(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	// The latest revision lives on a partition visited AFTER the new suffix.
	r.messages[1] = []types.Message{r.messages[0][10]}
	r.messages[1][0].Offset = 0
	r.messages[0] = r.messages[0][:10]
	_, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayOffset("g", 1, 2, 999), 10))
	_, err = c.PrepareFullConsumerMetadataReplay(r)
	require.ErrorContains(t, err, "conflicting offset snapshots")
}

func TestConsumerReplayIndexTombstoneAndReRegistration(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "g", Topic: "events", Epoch: 2}, 11))
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Empty(t, p.status.IndexFailure)
	require.Nil(t, p.scratch.groups["g"])
	require.Equal(t, uint64(2), p.scratch.groupEpochs["g"])
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayRegistration("g", 3), 12), encodedMetadataMessage(t, replayOffset("g", 3, 1, 0), 13))
	p, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.True(t, p.status.IndexHit)
	require.Equal(t, uint64(3), p.scratch.groupEpochs["g"])
	require.Zero(t, p.scratch.groups["g"].Offsets["events"][0])
}

func TestConsumerReplayIndexDeclinesTransactionsAndLegacy(t *testing.T) {
	for _, kind := range []string{"transaction", "legacy"} {
		t.Run(kind, func(t *testing.T) {
			c, r := newIndexFixture(t, 10)
			if kind == "transaction" {
				r.messages[0][1].TransactionalID = "pending"
				r.messages[0][1].TransactionState = types.TransactionStateOpen
			} else {
				r.messages[0] = append(r.messages[0], types.Message{Offset: 11, Payload: `{"group":"old","topic":"events","partition":0,"offset":9}`})
			}
			p, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.False(t, p.status.IndexHit)
			require.Zero(t, p.status.IndexWriteBytes)
		})
	}
}

func TestConsumerReplayIndexTransactionalSuffixInvalidatesReuse(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Positive(t, p.status.IndexWriteBytes)
	// A cached ordinary prefix must not hide a newly pending transaction. Even
	// before its decision becomes visible, raw certification must decline reuse.
	pending := encodedMetadataMessage(t, replayOffset("g", 1, 11, 11), 11)
	pending.TransactionalID = "pending"
	pending.TransactionState = types.TransactionStateOpen
	r.messages[0] = append(r.messages[0], pending)
	r.hidePending = true
	p, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.False(t, p.status.IndexHit)
	require.Zero(t, p.status.IndexWriteBytes)
	require.Equal(t, uint64(10), p.scratch.groups["g"].Offsets["events"][0])
	// Repeated recovery still checks the suffix; an older published index cannot
	// accidentally certify a transaction-bearing prefix on the following retry.
	p, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.False(t, p.status.IndexHit)
	require.Zero(t, p.status.IndexWriteBytes)
}

func TestConsumerReplayIndexReservationsAndProvenance(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	record := reservationFixture()
	record.Group = "g"
	record.Epoch = 1
	record.Reservations[0].Offsets = []ReservedTransactionOffset{{Topic: "events", Partition: 0, Offset: 20}}
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, record, 11))
	// Superseded records on an old physical partition still supply provenance.
	r.messages[1] = []types.Message{encodedMetadataMessage(t, replayOffset("g", 1, 1, 1), 0)}
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Empty(t, p.status.IndexFailure)
	require.Len(t, p.scratch.groups["g"].OffsetReservations, 1)
	p, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.True(t, p.status.IndexHit)
	require.Len(t, p.scratch.groups["g"].OffsetReservations, 1)
	require.Contains(t, p.scratch.metadataSources["g"], 1)
}

func TestConsumerReplayIndexRejectsChangingGeneration(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	r.onRaw = func() { r.identity = "replaced-during-replay" }
	_, err := c.PrepareFullConsumerMetadataReplay(r)
	require.ErrorContains(t, err, "source changed")
	require.Empty(t, c.groups)
}

func TestConsumerReplayIndexPublicationFailurePreservesPrevious(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Empty(t, p.status.IndexFailure)
	path := filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayOffset("g", 1, 11, 11), 11))
	r.syncErr = errors.New("durable stage failed")
	p, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Equal(t, uint64(11), p.scratch.groups["g"].Offsets["events"][0])
	require.Contains(t, p.status.IndexFailure, "durable stage")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	r.syncErr = nil
	p, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.True(t, p.status.IndexHit)
	require.Equal(t, uint64(11), p.scratch.groups["g"].Offsets["events"][0])
}

func TestConsumerReplayIndexCrashPublication(t *testing.T) {
	if stage := os.Getenv("CURSUS_INDEX_CRASH_STAGE"); stage != "" {
		path := os.Getenv("CURSUS_INDEX_CRASH_PATH")
		data, err := os.ReadFile(path + ".next")
		require.NoError(t, err)
		_, err = writeConsumerReplayIndex(path, data, func(current string) error {
			if current == stage {
				os.Exit(77)
			}
			return nil
		})
		t.Fatalf("crash point not reached: %v", err)
	}
	for _, stage := range []string{"written", "synced", "published"} {
		t.Run(stage, func(t *testing.T) {
			c, r := newIndexFixture(t, 10)
			_, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			path := filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")
			old, err := os.ReadFile(path)
			require.NoError(t, err)
			r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayOffset("g", 1, 11, 11), 11))
			_, err = c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			next, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path+".next", next, 0600))
			require.NoError(t, os.WriteFile(path, old, 0600))
			for attempt := 0; attempt < 2; attempt++ {
				command := exec.Command(os.Args[0], "-test.run=^TestConsumerReplayIndexCrashPublication$") // #nosec G204 -- reexecute this test binary only.
				command.Env = append(os.Environ(), "CURSUS_INDEX_CRASH_STAGE="+stage, "CURSUS_INDEX_CRASH_PATH="+path)
				err := command.Run()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit)
				require.Equal(t, 77, exit.ExitCode())
			}
			p, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.True(t, p.status.IndexHit)
			require.Equal(t, uint64(11), p.scratch.groups["g"].Offsets["events"][0])
			leftovers, err := filepath.Glob(path + ".tmp*")
			require.NoError(t, err)
			require.LessOrEqual(t, len(leftovers), 1)
		})
	}
}

func TestConsumerReplayPreservesOnlyUnchangedOwnerSession(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprint(cached), func(t *testing.T) {
			c, r := newIndexFixture(t, 10)
			lifecycle := ConsumerMetadataRecord{Version: 4, Type: ConsumerMetadataRecordLifecycleSnapshot, Group: "g", Epoch: 1, Revision: 1, Lifecycle: &GroupLifecycleSnapshot{TopicName: "events", Generation: 1, Partitions: []int{0}, Members: []GroupLifecycleMember{{ID: "m", Assignments: []int{0}}}}}
			r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, lifecycle, 11))
			p, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
			clock := time.Now().Add(-time.Minute)
			c.groups["g"].Members["m"].LastHeartbeat = clock
			c.groups["g"].LastActivity = clock
			c.ownershipSince["g"] = clock
			if !cached {
				require.NoError(t, os.Remove(filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")))
			}
			p, err = c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.NoError(t, c.PreserveConsumerReplayClocks(p, func(string) (int, error) { return 0, nil }, map[int]bool{0: true}))
			require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
			require.Equal(t, clock, c.groups["g"].Members["m"].LastHeartbeat)
			require.Equal(t, clock, c.groups["g"].LastActivity)
			require.Equal(t, clock, c.ownershipSince["g"])
			p, err = c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.NoError(t, c.PreserveConsumerReplayClocks(p, func(string) (int, error) { return 0, nil }, nil))
			require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
			require.NotEqual(t, clock, c.groups["g"].Members["m"].LastHeartbeat)
			require.NotContains(t, c.ownershipSince, "g")
		})
	}
}

func BenchmarkConsumerReplayIndex(b *testing.B) {
	for _, history := range []int{100, 1000, 10000} {
		for _, mode := range []string{"full", "build", "index", "suffix", "corrupt"} {
			b.Run(fmt.Sprintf("%s/history=%d", mode, history), func(b *testing.B) {
				cfg := config.DefaultConfig()
				cfg.EnabledDistribution = true
				cfg.ConsumerRecoveryIndexEnabled = true
				cfg.LogDir = b.TempDir()
				c := NewCoordinator(context.Background(), cfg, &countingPublisher{})
				defer c.Stop()
				r := &replayIndexTestReader{strictReplayReader: &strictReplayReader{count: c.offsetTopicPartitionCount, messages: make(map[int][]types.Message)}, identity: "benchmark-generation"}
				for i := 0; i <= history; i++ {
					record := replayOffset("g", 1, uint64(i), uint64(i))
					if i == 0 {
						record = replayRegistration("g", 1)
					}
					payload, key, err := EncodeConsumerMetadataRecord(record)
					if err != nil {
						b.Fatal(err)
					}
					r.messages[0] = append(r.messages[0], types.Message{Offset: uint64(i), Key: key, Payload: string(payload)})
				}
				release, err := c.BeginConsumerMetadataRecovery(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				defer release()
				if mode == "index" || mode == "suffix" || mode == "corrupt" {
					if _, err = c.PrepareFullConsumerMetadataReplay(r); err != nil {
						b.Fatal(err)
					}
				}
				r.reads, r.rawReads = 0, 0
				var cacheReads, cacheWrites int
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					path := filepath.Join(cfg.LogDir, ".consumer-replay-index.json")
					if mode == "build" {
						if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
							b.Fatal(err)
						}
					}
					if mode == "corrupt" {
						if err := os.WriteFile(path, []byte("torn"), 0600); err != nil {
							b.Fatal(err)
						}
					}
					if mode == "suffix" {
						next := uint64(len(r.messages[0]))
						payload, key, err := EncodeConsumerMetadataRecord(replayOffset("g", 1, next, next))
						if err != nil {
							b.Fatal(err)
						}
						r.messages[0] = append(r.messages[0], types.Message{Offset: next, Key: key, Payload: string(payload)})
					}
					var reader ConsumerMetadataCommittedReader = r
					if mode == "full" {
						reader = struct {
							ConsumerMetadataCommittedReader
						}{r}
					}
					prepared, err := c.PrepareFullConsumerMetadataReplay(reader)
					if err != nil {
						b.Fatal(err)
					}
					if prepared.status.IndexFailure != "" {
						b.Fatal(prepared.status.IndexFailure)
					}
					cacheReads += prepared.status.IndexReadBytes
					cacheWrites += prepared.status.IndexWriteBytes
				}
				b.StopTimer()
				b.ReportMetric(float64(r.reads+r.rawReads)/float64(b.N), "source-records/op")
				b.ReportMetric(float64(cacheReads)/float64(b.N), "index-read-B/op")
				b.ReportMetric(float64(cacheWrites)/float64(b.N), "index-write-B/op")
				b.ReportMetric(float64(c.offsetTopicPartitionCount), "partitions")
			})
		}
	}
}
