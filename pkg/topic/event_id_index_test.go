package topic

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type eventIDTestStorage struct {
	types.StorageHandler
	messages []types.Message
	first    uint64
	reads    []uint64
	readErr  error
}

func (s *eventIDTestStorage) GetFirstOffset() uint64    { return s.first }
func (s *eventIDTestStorage) GetAbsoluteOffset() uint64 { return uint64(len(s.messages)) }
func (s *eventIDTestStorage) GetFlushedOffset() uint64  { return s.GetAbsoluteOffset() }
func (s *eventIDTestStorage) ReadMessages(offset uint64, count int) ([]types.Message, error) {
	s.reads = append(s.reads, offset)
	if s.readErr != nil {
		return nil, s.readErr
	}
	end := min(len(s.messages), int(offset)+count)
	return s.messages[offset:end], nil
}
func (s *eventIDTestStorage) TruncateTo(offset uint64) error {
	s.messages = s.messages[:offset]
	return nil
}
func (s *eventIDTestStorage) append(id string) {
	s.messages = append(s.messages, types.Message{Offset: uint64(len(s.messages)), EventID: id, Payload: "payload"})
}
func eventIDTestPartition(n int) (*Partition, *eventIDTestStorage) {
	s := &eventIDTestStorage{}
	for i := 0; i < n; i++ {
		s.append(fmt.Sprint(i))
	}
	p := &Partition{dh: s, HWM: uint64(n), hwmAuthoritative: true, messageNotifyCh: make(chan struct{})}
	p.LEO.Store(uint64(n))
	return p, s
}
func requireEventID(t *testing.T, p *Partition, id string, want bool) {
	t.Helper()
	got, err := p.CommittedEventIDExists(id)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestCommittedEventIDIndexIncrementalAndRetention(t *testing.T) {
	p, s := eventIDTestPartition(2050)
	requireEventID(t, p, "missing", false)
	require.Equal(t, []uint64{0, 1024, 2048}, s.reads)
	s.reads = nil
	requireEventID(t, p, "0", true)
	requireEventID(t, p, "missing", false)
	require.Empty(t, s.reads)
	s.append("next")
	p.HWM++
	requireEventID(t, p, "next", true)
	require.Equal(t, []uint64{2050}, s.reads)
	s.first = 1024
	requireEventID(t, p, "0", false)
	requireEventID(t, p, "1024", true)
	require.Len(t, p.eventIDs.ids, 1027)
}

func TestCommittedEventIDIndexRollbackRefillSameTail(t *testing.T) {
	p, s := eventIDTestPartition(3)
	requireEventID(t, p, "2", true)
	require.NoError(t, p.ApplyReplicaRollback(1))
	s.append("replacement-1")
	s.append("replacement-2")
	p.LEO.Store(3)
	p.SetHWM(3)
	requireEventID(t, p, "2", false)
	requireEventID(t, p, "replacement-2", true)
	requireEventID(t, p, "0", true)
}

func TestCommittedEventIDIndexReadFailureIsNotAbsence(t *testing.T) {
	p, s := eventIDTestPartition(1)
	requireEventID(t, p, "0", true)
	s.append("new")
	p.HWM++
	s.readErr = errors.New("unreadable committed log")
	_, err := p.CommittedEventIDExists("new")
	require.ErrorContains(t, err, "unreadable committed log")
	require.Nil(t, p.eventIDs.ids)
	s.readErr = nil
	requireEventID(t, p, "new", true)
	p.setRecoveryError(errors.New("recovery pending"))
	_, err = p.CommittedEventIDExists("absent")
	require.Error(t, err)
	require.Nil(t, p.eventIDs.ids)
}

func TestCommittedEventIDIndexIgnoresUncommittedAndAbortedRecords(t *testing.T) {
	p, s := eventIDTestPartition(3)
	p.HWM = 2
	s.messages[1].TransactionalID = "aborted"
	s.messages[1].TransactionState = types.TransactionStateAborted
	requireEventID(t, p, "0", true)
	requireEventID(t, p, "1", false)
	requireEventID(t, p, "2", false)
	p.SetHWM(3)
	requireEventID(t, p, "2", true)
}

func TestCommittedEventIDIndexRebuildsOnNewPartition(t *testing.T) {
	p, s := eventIDTestPartition(3)
	requireEventID(t, p, "2", true)
	restarted := &Partition{dh: s, HWM: 3, hwmAuthoritative: true}
	requireEventID(t, restarted, "0", true)
	requireEventID(t, restarted, "2", true)
	requireEventID(t, restarted, "absent", false)
}

func TestCommittedEventIDIndexSnapshotVisibilityAndUnavailableHWM(t *testing.T) {
	p, _ := eventIDTestPartition(3)
	requireEventID(t, p, "2", true)
	previous := p.eventIDs.generation
	require.NoError(t, p.ReconcileSnapshotHWM(1))
	requireEventID(t, p, "2", false)
	require.NotEqual(t, previous, p.eventIDs.generation)
	require.NoError(t, p.FinalizeSnapshotRecovery(3))
	requireEventID(t, p, "2", true)
	p.distributed, p.hwmAuthoritative = true, false
	_, err := p.CommittedEventIDExists("absent")
	require.ErrorIs(t, err, types.ErrCommittedHWMUnavailable)
	require.Nil(t, p.eventIDs.ids)
}

func BenchmarkCommittedEventIDLookup(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			p, _ := eventIDTestPartition(size)
			if _, err := p.CommittedEventIDExists("absent"); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := p.CommittedEventIDExists("absent"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
