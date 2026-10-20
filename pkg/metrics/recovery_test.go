package metrics

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecoveryExporterIncludesWorkFailuresAndBoundedLabels(t *testing.T) {
	recoveryWork.Reset()
	recoveryAttempts.Reset()
	RecordRecoveryBytes(RecoveryChunkRead, RecoveryReadBytes, 7)
	RecordRecoveryBytes(RecoveryChunkRead, RecoveryReadBytes, -1)
	RecordRecoveryWork(RecoveryStage(255), RecoveryUnit(255), 42)
	RecordRecoveryAttempt(RecoveryChunkRead, nil)
	RecordRecoveryAttempt(RecoveryChunkRead, errors.New("private/path/topic/attempt"))
	RecordRecoveryAttempt(RecoveryChunkRead, context.Canceled)
	RecordRecoveryAttempt(RecoveryStage(255), nil)
	handler, err := Handler()
	require.NoError(t, err)
	snapshot := handler.(*snapshotHandler)
	t.Cleanup(func() {
		require.NoError(t, snapshot.Close())
		select {
		case <-snapshot.process.worker.Done():
		case <-time.After(5 * time.Second):
			t.Error("metrics observation worker did not stop")
		}
	})
	require.Eventually(t, func() bool {
		return snapshot.process.worker.Snapshot().Valid()
	}, 5*time.Second, time.Millisecond, "initial metrics observation did not complete")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	body := response.Body.String()
	require.Contains(t, body, `cursus_recovery_work_total{stage="chunk_read",unit="read_bytes"} 7`)
	for _, outcome := range []string{"success", "failure", "canceled"} {
		require.Contains(t, body, `cursus_recovery_attempts_total{outcome="`+outcome+`",stage="chunk_read"} 1`)
	}
	require.NotContains(t, body, "private/path/topic/attempt")
}
