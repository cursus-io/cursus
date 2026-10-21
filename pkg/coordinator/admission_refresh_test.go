package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func admissionClockFixture(t *testing.T) (*Coordinator, *metadataReplayHandler, time.Time) {
	t.Helper()
	old := time.Now().Add(-2 * time.Minute).UTC()
	registration := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordRegistration,
		Group: "live", Topic: "payments", PartitionCount: 2, Epoch: 1, Timestamp: old}
	lifecycle := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersionLifecycle, Type: ConsumerMetadataRecordLifecycleSnapshot,
		Group: "live", Epoch: 1, Revision: 1, Timestamp: old,
		Lifecycle: &GroupLifecycleSnapshot{TopicName: "payments", Generation: 1, Partitions: []int{0, 1},
			Members: []GroupLifecycleMember{{ID: "member", Assignments: []int{0, 1}}}, LastRebalance: old, LastActivity: old}}
	handler := &metadataReplayHandler{messages: map[int][]types.Message{0: {
		encodedMetadataMessage(t, registration, 0), encodedMetadataMessage(t, lifecycle, 1),
	}}}
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := NewCoordinatorWithRecovery(context.Background(), cfg, handler)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.NoError(t, recoverDeferredMetadataForTest(t, c))
	c.mu.Lock()
	c.groups["live"].Members["member"].LastHeartbeat = old
	c.groups["live"].LastActivity = old
	c.ownershipSince["live"] = old
	c.mu.Unlock()
	return c, handler, old
}

func TestAdmissionRefreshPreservesClocksAndDoesNotExtendTimeout(t *testing.T) {
	c, handler, old := admissionClockFixture(t)
	offset := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersion, Type: ConsumerMetadataRecordOffsetSnapshot,
		Group: "live", Topic: "payments", Epoch: 1, Revision: 1, Offsets: []OffsetItem{{Partition: 0, Offset: 42}}}
	handler.messages[0] = append(handler.messages[0], encodedMetadataMessage(t, offset, 2))
	for range 3 {
		require.NoError(t, c.RefreshConsumerMetadataForAdmission())
		require.Equal(t, old, c.groups["live"].Members["member"].LastHeartbeat)
		require.Equal(t, old, c.groups["live"].LastActivity)
		require.Equal(t, old, c.ownershipSince["live"])
		value, found := c.GetOffset("live", "payments", 0)
		require.True(t, found)
		require.Equal(t, uint64(42), value)
	}
	var expired []string
	c.groupOwnerChecker = func(string) bool { return true }
	c.expirationHandler = func(_ string, _ int, members []string) error { expired = members; return nil }
	c.checkSingleGroupTimeout("live", 30*time.Second)
	require.Equal(t, []string{"member"}, expired, "admin requests must not grant new ownership/session grace")
}

func TestAdmissionRefreshRetainsHeartbeatReceivedDuringScan(t *testing.T) {
	c, handler, old := admissionClockFixture(t)
	paused := &pausedMetadataReplayHandler{metadataReplayHandler: *handler, scanned: make(chan struct{}), resume: make(chan struct{})}
	c.topicHandler = paused
	done := make(chan error, 1)
	go func() { done <- c.RefreshConsumerMetadataForAdmission() }()
	select {
	case <-paused.scanned:
	case <-time.After(5 * time.Second):
		close(paused.resume)
		t.Fatal("replay did not reach pause")
	}
	err := c.RecordHeartbeatForGeneration("live", "member", 1)
	c.mu.RLock()
	heartbeat := c.groups["live"].Members["member"].LastHeartbeat
	c.mu.RUnlock()
	close(paused.resume)
	require.NoError(t, err)
	require.NoError(t, <-done)
	require.True(t, heartbeat.After(old))
	require.Equal(t, heartbeat, c.groups["live"].Members["member"].LastHeartbeat)
	require.Equal(t, heartbeat, c.groups["live"].LastActivity)
	require.Equal(t, old, c.ownershipSince["live"])
}

func TestAdmissionRefreshDoesNotCarryClocksAcrossLifecycleChanges(t *testing.T) {
	for _, change := range []string{"epoch", "generation", "assignment", "member", "departure", "rebalance", "tombstone", "ownership recovery"} {
		t.Run(change, func(t *testing.T) {
			c, handler, old := admissionClockFixture(t)
			registration, _, err := decodeConsumerMetadataRecord(handler.messages[0][0].Payload)
			require.NoError(t, err)
			lifecycle, _, err := decodeConsumerMetadataRecord(handler.messages[0][1].Payload)
			require.NoError(t, err)
			switch change {
			case "epoch":
				registration.Epoch, lifecycle.Epoch = 2, 2
			case "generation":
				lifecycle.Lifecycle.Generation = 2
				lifecycle.Revision = 2
			case "assignment":
				lifecycle.Lifecycle.Members[0].Assignments = []int{1}
			case "member":
				lifecycle.Lifecycle.Members[0].ID = "replacement"
			case "departure":
				lifecycle.Lifecycle.Members = nil
			case "rebalance":
				lifecycle.Lifecycle.LastRebalance = old.Add(time.Second)
			case "tombstone":
				registration.Type, registration.Epoch = ConsumerMetadataRecordTombstone, 2
				registration.PartitionCount = 0
			}
			handler.messages[0] = []types.Message{encodedMetadataMessage(t, registration, 0), encodedMetadataMessage(t, lifecycle, 1)}
			if change == "ownership recovery" {
				require.NoError(t, c.ReloadDistributedConsumerMetadata())
			} else {
				require.NoError(t, c.RefreshConsumerMetadataForAdmission())
			}
			require.NotContains(t, c.ownershipSince, "live")
			if change == "tombstone" {
				require.Nil(t, c.groups["live"])
			} else {
				for _, member := range c.groups["live"].Members {
					require.True(t, member.LastHeartbeat.After(old))
				}
			}
		})
	}
}

func TestAdmissionRefreshRetainsAcknowledgedReservationProgress(t *testing.T) {
	c, handler, old := admissionClockFixture(t)
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
	reservation := TransactionOffsetReservation{TransactionalID: "tx", ProducerID: "producer", MemberID: "member", Generation: 1,
		Offsets: []ReservedTransactionOffset{{Topic: "payments", Partition: 0, Offset: 42}}}
	require.NoError(t, c.PrepareOffsetReservation("live", 1, reservation))
	require.NoError(t, c.RefreshConsumerMetadataForAdmission())
	_, _, err := c.GetStableOffset("live", "payments", 0)
	require.ErrorContains(t, err, "unstable_offset_commit")
	prepared := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersionReservations, Type: ConsumerMetadataRecordOffsetReservations,
		Group: "live", Epoch: 1, Revision: 1, Reservations: []TransactionOffsetReservation{reservation}}
	handler.messages[0] = append(handler.messages[0], encodedMetadataMessage(t, prepared, 2))
	require.NoError(t, c.ResolveOffsetReservation("live", 1, "tx", "producer", 0, false))
	require.NoError(t, c.RefreshConsumerMetadataForAdmission())
	_, _, err = c.GetStableOffset("live", "payments", 0)
	require.NoError(t, err, "stale replay must not resurrect an acknowledged release")
	require.Equal(t, uint64(2), c.groups["live"].ReservationRevision)
	require.Equal(t, old, c.groups["live"].Members["member"].LastHeartbeat)
	require.Equal(t, old, c.ownershipSince["live"])
}

type admissionFailedReader struct{ *metadataReplayHandler }

func (*admissionFailedReader) ReadTopicPartition(string, int, uint64, int) ([]types.Message, error) {
	return nil, fmt.Errorf("injected admission read failure")
}

func TestAdmissionRefreshReadFailureDoesNotChangeLiveState(t *testing.T) {
	c, handler, old := admissionClockFixture(t)
	live := c.groups["live"]
	c.topicHandler = &admissionFailedReader{handler}
	require.ErrorContains(t, c.RefreshConsumerMetadataForAdmission(), "injected admission read failure")
	require.Same(t, live, c.groups["live"])
	require.Equal(t, old, live.Members["member"].LastHeartbeat)
	require.Equal(t, old, c.ownershipSince["live"])
}
