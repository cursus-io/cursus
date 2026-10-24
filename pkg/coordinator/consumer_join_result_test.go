package coordinator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumerJoinResultSurvivesQueuedRecovery(t *testing.T) {
	c := newAuthorityTestCoordinator(t)
	require.NoError(t, c.RegisterGroupFromFSM("events", "g", 1))
	entered, persist := make(chan struct{}), make(chan struct{})
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { close(entered); <-persist; return nil })
	type outcome struct {
		result ConsumerJoinResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := c.AddConsumerWithResult("g", "m")
		done <- outcome{result, err}
	}()
	awaitAdmission(t, entered)
	recovery := startAdmissionRecovery(t, c)
	waitRecoveryClaims(t, c, 1)
	close(persist)
	joined := awaitAdmission(t, done)
	require.NoError(t, joined.err)
	release := awaitAdmission(t, recovery)
	defer release()
	require.Equal(t, 1, joined.result.Generation)
	require.Equal(t, []int{0}, joined.result.Assignments)
	require.Zero(t, c.GetGeneration("g"), "a separate admitted getter cannot supply the JOIN response during recovery")
	state := c.ExportState()["g"]
	require.Equal(t, joined.result.Generation, state.Generation)
}
