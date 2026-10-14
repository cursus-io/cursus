package coordinator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapTicketsSurviveLeaseReleaseRestoreAndStaleCleanup(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	release := acquireReplay(t, c)
	identity := ConsumerBootstrapIdentity{Partition: 0, Lifecycle: 1, Attempt: "a"}
	ticket, err := c.RetainConsumerBootstrapTicket(identity, nil)
	require.NoError(t, err)
	other, err := c.RetainConsumerBootstrapTicket(ConsumerBootstrapIdentity{Partition: 1, Lifecycle: 1, Attempt: "b"}, nil)
	require.NoError(t, err)
	release()
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	generation := c.BeginConsumerRestore()
	c.FinishConsumerRestore(generation, nil)
	release, err = c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	defer release()
	same, err := c.RetainConsumerBootstrapTicket(identity, nil)
	require.NoError(t, err)
	require.Same(t, ticket, same)
	require.Error(t, c.FinishConsumerBootstrapTicket(ticket, ConsumerBootstrapUncertain, false, 0))
	require.NoError(t, c.FinishConsumerBootstrapTicket(ticket, ConsumerBootstrapPreIntentAbort, false, 0))
	next, err := c.RetainConsumerBootstrapTicket(ConsumerBootstrapIdentity{Partition: 0, Lifecycle: 1, Attempt: "next"}, nil)
	require.NoError(t, err)
	require.Error(t, c.FinishConsumerBootstrapTicket(ticket, ConsumerBootstrapPreIntentAbort, false, 0))
	require.NoError(t, c.FinishConsumerBootstrapTicket(next, ConsumerBootstrapPreIntentAbort, false, 0))
	require.NoError(t, c.FinishConsumerBootstrapTicket(other, ConsumerBootstrapPreIntentAbort, false, 0))
}

func TestPreparedBootstrapStampInvalidatedByRestore(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	acquireReplay(t, c)
	ticket, err := c.RetainConsumerBootstrapTicket(ConsumerBootstrapIdentity{Partition: 0, Lifecycle: 1, Attempt: "a"}, nil)
	require.NoError(t, err)
	applyBootstrapTicketForTest(t, c, ticket, 7)
	require.True(t, c.ConsumerBootstrapPreparedCurrent(ticket, 7))
	generation := c.BeginConsumerRestore()
	c.FinishConsumerRestore(generation, nil)
	require.False(t, c.ConsumerBootstrapPreparedCurrent(ticket, 7))
	require.Error(t, c.FinishConsumerBootstrapTicket(ticket, ConsumerBootstrapCompleted, false, 7))
	require.Error(t, c.FinishConsumerBootstrapTicket(ticket, ConsumerBootstrapPreIntentAbort, false, 7))
	applyBootstrapTicketForTest(t, c, ticket, 8)
	require.Error(t, c.FinishConsumerBootstrapTicket(ticket, ConsumerBootstrapCompleted, true, 8))
	require.NoError(t, c.FinishConsumerBootstrapTicket(ticket, ConsumerBootstrapCompleted, false, 8))
}

func applyBootstrapTicketForTest(t *testing.T, c *Coordinator, ticket *ConsumerBootstrapTicket, view uint64) {
	t.Helper()
	scope, err := c.ConsumerBootstrapScope(ticket)
	require.NoError(t, err)
	reader := strictReader(c)
	prepared, err := c.PrepareScopedConsumerMetadataReplay(reader, reader, scope, func(string) (int, error) { return 0, nil })
	require.NoError(t, err)
	require.NoError(t, c.ApplyConsumerBootstrapReplay(ticket, prepared, view))
}

func TestConsumerBootstrapTicketAcceptsExplicitZeroLifecycle(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	acquireReplay(t, c)
	_, err := c.RetainConsumerBootstrapTicket(ConsumerBootstrapIdentity{Partition: 0, Lifecycle: 0, Attempt: "a"}, nil)
	require.NoError(t, err)
}

func TestBootstrapCannotPublishPreparedStampAcrossRestore(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	acquireReplay(t, c)
	ticket, err := c.RetainConsumerBootstrapTicket(ConsumerBootstrapIdentity{Partition: 0, Attempt: "a"}, nil)
	require.NoError(t, err)
	scope, err := c.ConsumerBootstrapScope(ticket)
	require.NoError(t, err)
	reader := strictReader(c)
	prepared, err := c.PrepareScopedConsumerMetadataReplay(reader, reader, scope, func(string) (int, error) { return 0, nil })
	require.NoError(t, err)
	require.NoError(t, c.ApplyScopedConsumerMetadataReplay(prepared))
	generation := c.BeginConsumerRestore()
	c.FinishConsumerRestore(generation, nil)
	require.Error(t, c.markConsumerBootstrapPrepared(ticket, 7, prepared.stamp))
	require.False(t, c.ConsumerBootstrapPreparedCurrent(ticket, 7))
}

func TestObservedBootstrapPendingClosesAdmissionWithoutLease(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	identity := ConsumerBootstrapIdentity{Partition: 0, Lifecycle: 1, Attempt: "a"}
	require.NoError(t, c.ObservePendingConsumerBootstrap(identity, c.CaptureConsumerBootstrapObservation()))
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	release := acquireReplay(t, c)
	ticket, err := c.RetainConsumerBootstrapTicket(identity, nil)
	require.NoError(t, err)
	require.NoError(t, c.FinishConsumerBootstrapTicket(ticket, ConsumerBootstrapPreIntentAbort, false, 0))
	release()
	require.NoError(t, c.ConsumerOperationsReadinessError())
}

func TestBootstrapObservationRevisionPreservesNewAttemptsAndIndependentTickets(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	observation := c.CaptureConsumerBootstrapObservation()
	identity := ConsumerBootstrapIdentity{Partition: 0, Lifecycle: 1, Attempt: "old"}
	placeholderIdentity := identity
	placeholderIdentity.Attempt = ""
	require.NoError(t, c.ObservePendingConsumerBootstrap(placeholderIdentity, observation))
	placeholder := c.consumerAdmission.tickets[0]
	require.NoError(t, c.ObservePendingConsumerBootstrap(identity, observation))
	release := acquireReplay(t, c)
	old, err := c.RetainConsumerBootstrapTicket(identity, nil)
	require.NoError(t, err)
	require.NotSame(t, placeholder, old)
	require.Error(t, c.FinishConsumerBootstrapTicket(placeholder, ConsumerBootstrapPreIntentAbort, false, 0))
	otherIdentity := ConsumerBootstrapIdentity{Partition: 1, Lifecycle: 1, Attempt: "independent"}
	other, err := c.RetainConsumerBootstrapTicket(otherIdentity, nil)
	require.NoError(t, err)
	require.NoError(t, c.FinishConsumerBootstrapTicket(old, ConsumerBootstrapPreIntentAbort, false, 0))
	nextIdentity := identity
	nextIdentity.Attempt = "new"
	next, err := c.RetainConsumerBootstrapTicket(nextIdentity, nil)
	require.NoError(t, err)
	require.ErrorIs(t, c.ObservePendingConsumerBootstrap(identity, observation), ErrConsumerBootstrapObservationStale)
	require.ErrorIs(t, c.ObservePendingConsumerBootstrap(placeholderIdentity, observation), ErrConsumerBootstrapObservationStale)
	require.Error(t, c.FinishConsumerBootstrapTicket(old, ConsumerBootstrapPreIntentAbort, false, 0))
	same, err := c.RetainConsumerBootstrapTicket(nextIdentity, nil)
	require.NoError(t, err)
	require.Same(t, next, same)
	// An unrelated completion invalidates an in-flight observation, but a
	// fresh storage observation can still retain the independent pending ticket.
	require.ErrorIs(t, c.ObservePendingConsumerBootstrap(otherIdentity, observation), ErrConsumerBootstrapObservationStale)
	fresh := c.CaptureConsumerBootstrapObservation()
	require.Error(t, c.FinishConsumerBootstrapTicket(other, ConsumerBootstrapUncertain, false, 0))
	require.NoError(t, c.ObservePendingConsumerBootstrap(otherIdentity, fresh), "failed completion must not invalidate observations")
	require.NoError(t, c.FinishConsumerBootstrapTicket(next, ConsumerBootstrapPreIntentAbort, false, 0))
	release()
	require.ErrorIs(t, c.ConsumerOperationsReadinessError(), ErrConsumerMetadataRecovering)
	release = acquireReplay(t, c)
	require.NoError(t, c.FinishConsumerBootstrapTicket(other, ConsumerBootstrapPreIntentAbort, false, 0))
	release()
	require.NoError(t, c.ConsumerOperationsReadinessError())
}

func TestBootstrapObservationTokenBelongsToCoordinator(t *testing.T) {
	owner := newAuthorityTestCoordinator(t)
	other := newAuthorityTestCoordinator(t)
	identity := ConsumerBootstrapIdentity{Partition: 0, Attempt: "pending"}
	require.ErrorIs(t, other.ObservePendingConsumerBootstrap(identity, owner.CaptureConsumerBootstrapObservation()), ErrConsumerBootstrapObservationStale)
	require.ErrorIs(t, other.ObservePendingConsumerBootstrap(identity, ConsumerBootstrapObservation{}), ErrConsumerBootstrapObservationStale)
	require.NoError(t, other.ConsumerOperationsReadinessError())
}
