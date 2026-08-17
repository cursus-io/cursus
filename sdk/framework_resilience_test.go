package sdk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryPolicyUsesBoundedExponentialDelay(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, InitialDelay: 10 * time.Millisecond, MaxDelay: 25 * time.Millisecond, Multiplier: 2}
	require.True(t, policy.ShouldRetry(1))
	require.False(t, policy.ShouldRetry(3))
	require.Equal(t, 10*time.Millisecond, policy.Delay(1))
	require.Equal(t, 20*time.Millisecond, policy.Delay(2))
	require.Equal(t, 25*time.Millisecond, policy.Delay(3))
}

func TestUpcasterRegistryAdvancesEventSchema(t *testing.T) {
	registry := NewUpcasterRegistry()
	require.NoError(t, registry.Register("GameFinished", 1, func(event EventEnvelope) (EventEnvelope, error) {
		event.SchemaVersion = 2
		event.Payload = []byte(`{"winner":"p1","rating_delta":10}`)
		return event, nil
	}))
	event := sagaTestEvent()
	updated, err := registry.Upcast(event)
	require.NoError(t, err)
	require.Equal(t, uint32(2), updated.SchemaVersion)
	require.JSONEq(t, `{"winner":"p1","rating_delta":10}`, string(updated.Payload))
}

func TestUpcasterRegistryRejectsNonAdvancingMigration(t *testing.T) {
	registry := NewUpcasterRegistry()
	require.NoError(t, registry.Register("GameFinished", 1, func(event EventEnvelope) (EventEnvelope, error) { return event, nil }))
	_, err := registry.Upcast(sagaTestEvent())
	require.Error(t, err)
	require.Contains(t, err.Error(), "did not advance")
}

func TestDeadlineManagerRunsDueCallbacksOnce(t *testing.T) {
	manager := NewDeadlineManager()
	now := time.Unix(100, 0)
	fired := 0
	require.NoError(t, manager.Schedule("deadline-1", now, func() { fired++ }))
	require.Equal(t, 1, manager.RunDue(now))
	require.Equal(t, 1, fired)
	require.Equal(t, 0, manager.RunDue(now.Add(time.Second)))
}

func TestCompensationCommandPreservesSagaCorrelation(t *testing.T) {
	command := CompensationCommand("RollbackElo", SagaState{ID: "saga-1", CorrelationID: "corr-1"}, "event-1", `{}`)
	require.Equal(t, "saga-1", command.SagaID)
	require.Equal(t, "corr-1", command.CorrelationID)
	require.Equal(t, "event-1", command.CausationID)
}
