//go:build e2e_faults && linux

package metrics

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// RecoveryWorkSnapshotForFault records work before an injected process exit.
// Collect only this counter: gathering the global registry while recovery owns
// its state fences could call collectors that need those same fences.
func RecoveryWorkSnapshotForFault() (map[string]float64, error) {
	values := make(map[string]float64)
	stream := make(chan prometheus.Metric)
	go func() {
		recoveryWork.Collect(stream)
		close(stream)
	}()
	var firstErr error
	for metric := range stream {
		var value dto.Metric
		if err := metric.Write(&value); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		var labels []string
		for _, label := range value.Label {
			labels = append(labels, fmt.Sprintf("%s=%q", label.GetName(), label.GetValue()))
		}
		values["cursus_recovery_work_total{"+strings.Join(labels, ",")+"}"] = value.GetCounter().GetValue()
	}
	return values, firstErr
}
