package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestRecordCommandTracksResultLatencyAndErrorCode(t *testing.T) {
	command := "TEST_COMMAND"
	okCounter := CommandsTotal.WithLabelValues(command, "ok")
	errorCounter := CommandsTotal.WithLabelValues(command, "error")
	codeCounter := CommandErrors.WithLabelValues(command, "invalid_request")
	okBefore := counterMetricValue(t, okCounter)
	errorBefore := counterMetricValue(t, errorCounter)
	codeBefore := counterMetricValue(t, codeCounter)
	duration := CommandDuration.WithLabelValues(command)
	durationBefore := histogramMetricCount(t, duration)

	RecordCommand(command, "OK", 5*time.Millisecond)
	RecordCommand(command, "ERROR: invalid_request reason=bad", 10*time.Millisecond)

	if got := counterMetricValue(t, okCounter); got != okBefore+1 {
		t.Fatalf("ok command count = %v, want %v", got, okBefore+1)
	}
	if got := counterMetricValue(t, errorCounter); got != errorBefore+1 {
		t.Fatalf("error command count = %v, want %v", got, errorBefore+1)
	}
	if got := counterMetricValue(t, codeCounter); got != codeBefore+1 {
		t.Fatalf("error code count = %v, want %v", got, codeBefore+1)
	}
	if got := histogramMetricCount(t, duration); got != durationBefore+2 {
		t.Fatalf("duration sample count = %v, want %v", got, durationBefore+2)
	}
}

func counterMetricValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

func histogramMetricCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metricWriter, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatal("histogram observer does not implement prometheus.Metric")
	}
	metric := &dto.Metric{}
	if err := metricWriter.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetHistogram().GetSampleCount()
}
