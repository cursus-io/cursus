package coordinator

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Expiring a crashed peer must not block a live member's volatile heartbeat
// behind durable lifecycle I/O or overwrite that heartbeat at publication.
func TestCooperativeHeartbeatDuringPeerExpiration(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "published"
		if fail {
			name = "ambiguous"
		}
		t.Run(name, func(t *testing.T) {
			c, parts, records := runtimeCooperativeFixture(t, 4)
			a := runtimeJoin(t, c, parts, "a")
			b := runtimeJoin(t, c, parts, "b")
			a = runtimeHeartbeat(t, c, parts, a, nil)
			a = runtimeHeartbeat(t, c, parts, a, membershipAcks(a))
			b = runtimeHeartbeat(t, c, parts, b, nil)
			oldClock := time.Now().Add(-10 * time.Second)
			c.mu.Lock()
			c.groups["workers"].Members["a"].LastHeartbeat = oldClock
			c.groups["workers"].Members["b"].LastHeartbeat = time.Now().Add(-time.Hour)
			before := c.groups["workers"].Cooperative.Clone()
			c.mu.Unlock()
			writesBefore := len(*records)
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(resume) }) }
			defer unblock()
			injected := errors.New("lifecycle append outcome unknown")
			c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
				close(entered)
				<-resume
				*records = append(*records, record)
				if fail {
					return injected
				}
				return nil
			})
			expired := make(chan error, 1)
			go func() { expired <- c.expireCooperative("workers", time.Minute) }()
			<-entered
			heartbeat := make(chan error, 1)
			go func() {
				_, err := c.ReconcileCooperative("workers", a.Member, a.RegistrationEpoch, a.Session, parts, nil, false)
				heartbeat <- err
			}()
			select {
			case err := <-heartbeat:
				require.ErrorContains(t, err, "coordinator_not_available", "pending grants must not be acknowledged")
			case <-time.After(time.Second):
				unblock()
				<-expired
				<-heartbeat
				t.Fatal("live heartbeat waited behind another member's expiration append")
			}
			c.mu.RLock()
			refreshed := c.groups["workers"].Members["a"].LastHeartbeat
			current := c.groups["workers"].Cooperative.Clone()
			c.mu.RUnlock()
			require.True(t, refreshed.After(oldClock))
			require.Equal(t, before, current, "heartbeat must not publish the pending assignment")
			for _, invalid := range [][2]uint64{{a.RegistrationEpoch + 1, a.Session}, {a.RegistrationEpoch, a.Session + 1}} {
				_, err := c.ReconcileCooperative("workers", a.Member, invalid[0], invalid[1], parts, nil, false)
				require.ErrorContains(t, err, "cooperative_fenced")
			}
			_, late := c.ReconcileCooperative("workers", b.Member, b.RegistrationEpoch, b.Session, parts, nil, false)
			require.ErrorContains(t, late, "coordinator_not_available", "selected removal must not revive")
			c.mu.RLock()
			currentClock := c.groups["workers"].Members["a"].LastHeartbeat
			removedClock := c.groups["workers"].Members["b"].LastHeartbeat
			c.mu.RUnlock()
			require.Equal(t, refreshed, currentClock, "stale sessions must not refresh the live member")
			require.Greater(t, time.Since(removedClock), time.Minute)
			require.NoError(t, c.ValidateCooperativeRead("workers", a.RegistrationEpoch, &a.Grants[0]))
			require.ErrorContains(t, c.ValidateCooperativeRead("workers", b.RegistrationEpoch, &b.Grants[0]), "coordinator_not_available")
			unblock()
			err := <-expired
			require.Len(t, *records, writesBefore+1, "heartbeats must not append lifecycle records")
			payload, marshalErr := json.Marshal((*records)[writesBefore])
			require.NoError(t, marshalErr)
			t.Logf("expiration_records=1 expiration_payload_bytes=%d heartbeat_records=0 heartbeat_payload_bytes=0", len(payload))
			if fail {
				require.ErrorIs(t, err, injected)
				require.ErrorContains(t, c.ValidateCooperativeRead("workers", a.RegistrationEpoch, &a.Grants[0]), "coordinator_not_available")
			} else {
				require.NoError(t, err)
				c.mu.RLock()
				published := c.groups["workers"].Members["a"].LastHeartbeat
				c.mu.RUnlock()
				require.False(t, published.Before(refreshed), "publication replaced a fresh heartbeat with its older candidate clock")
				runtimeHeartbeat(t, c, parts, a, nil)
				require.ErrorContains(t, c.ValidateCooperativeRead("workers", b.RegistrationEpoch, &b.Grants[0]), "cooperative_fenced")
			}
		})
	}
}

// Retrying an actual revocation ACK during its own append may refresh the
// session, but must not acknowledge the handoff or grant the successor early.
func TestCooperativeHeartbeatDoesNotPublishPendingRevocation(t *testing.T) {
	c, parts, _ := runtimeCooperativeFixture(t, 4)
	a := runtimeJoin(t, c, parts, "a")
	b := runtimeJoin(t, c, parts, "b")
	a = runtimeHeartbeat(t, c, parts, a, nil)
	acks := membershipAcks(a)
	require.NotEmpty(t, acks)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { close(entered); <-resume; return nil })
	done := make(chan error, 1)
	go func() {
		_, err := c.ReconcileCooperative("workers", a.Member, a.RegistrationEpoch, a.Session, parts, acks, false)
		done <- err
	}()
	<-entered
	for _, request := range []struct {
		member CooperativeMembership
		acks   []CooperativeRevocation
	}{{a, acks}, {b, nil}} {
		result := make(chan error, 1)
		go func() {
			_, err := c.ReconcileCooperative("workers", request.member.Member, request.member.RegistrationEpoch, request.member.Session, parts, request.acks, false)
			result <- err
		}()
		select {
		case err := <-result:
			require.ErrorContains(t, err, "coordinator_not_available")
		case <-time.After(time.Second):
			unblock()
			<-done
			<-result
			t.Fatal("pending revocation blocked a session heartbeat")
		}
	}
	c.mu.RLock()
	grants := cooperativeMembership(c.groups["workers"].Cooperative, b.Member).Grants
	c.mu.RUnlock()
	require.Empty(t, grants)
	unblock()
	require.NoError(t, <-done)
	b = runtimeHeartbeat(t, c, parts, b, nil)
	require.Len(t, b.Grants, 2)
}
