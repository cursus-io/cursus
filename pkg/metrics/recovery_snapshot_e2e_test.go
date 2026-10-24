//go:build e2e_faults && linux

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryWorkSnapshotForFault(t *testing.T) {
	before, err := RecoveryWorkSnapshotForFault()
	require.NoError(t, err)
	RecordRecoveryWork(RecoveryConsumerReplay, RecoveryDecodedFrames, 7)
	RecordRecoveryWork(RecoveryConsumerReplay, RecoveryDecodedBytes, 901)
	after, err := RecoveryWorkSnapshotForFault()
	require.NoError(t, err)
	frames := `cursus_recovery_work_total{stage="consumer_replay",unit="decoded_frames"}`
	bytes := `cursus_recovery_work_total{stage="consumer_replay",unit="decoded_bytes"}`
	require.Equal(t, float64(7), after[frames]-before[frames])
	require.Equal(t, float64(901), after[bytes]-before[bytes])
}
