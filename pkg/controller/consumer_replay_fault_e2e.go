//go:build e2e_faults && linux

package controller

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/metrics"
	"golang.org/x/sys/unix"
)

const replayFaultArm = "/app/cluster-logs/.consumer-replay-fault-arm"
const replayFaultResult = "/app/cluster-logs/.consumer-replay-fault-result.json"

var replayFaultOnce sync.Once
var replayFaultThreshold uint64
var replayFaultRecords atomic.Uint64
var replayFaultFired atomic.Bool

// A dedicated fault image and an offline-created sentinel are both required.
// Exit after a real source page was consumed, before its detached projection can
// publish an index. This path never exists in a production broker binary.
func consumerReplayMeasurementFault(records int) {
	replayFaultOnce.Do(func() {
		if os.Getenv("CURSUS_E2E_FAULTS") != "1" {
			return
		}
		arm, err := os.ReadFile(replayFaultArm)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			replayFaultFatal(err)
		}
		replayFaultThreshold, err = strconv.ParseUint(strings.TrimSpace(string(arm)), 10, 64)
		if err != nil || replayFaultThreshold == 0 {
			replayFaultFatal(fmt.Errorf("invalid replay fault threshold: %q", arm))
		}
	})
	if replayFaultThreshold == 0 || records <= 0 {
		return
	}
	consumed := replayFaultRecords.Add(uint64(records))
	if consumed < replayFaultThreshold || !replayFaultFired.CompareAndSwap(false, true) {
		return
	}
	work, err := metrics.RecoveryWorkSnapshotForFault()
	if err != nil {
		replayFaultFatal(err)
	}
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		replayFaultFatal(err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	resources := map[string]float64{
		"cpu_seconds":           float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6,
		"peak_rss_bytes":        float64(usage.Maxrss) * 1024,
		"heap_bytes":            float64(memory.HeapAlloc),
		"allocated_bytes_total": float64(memory.TotalAlloc),
		"allocations_total":     float64(memory.Mallocs),
	}
	io, err := os.ReadFile("/proc/self/io")
	if err != nil {
		replayFaultFatal(err)
	}
	for _, line := range strings.Split(string(io), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			replayFaultFatal(err)
		}
		resources["process_"+strings.TrimSuffix(fields[0], ":")] = float64(value)
	}
	result := map[string]any{"threshold_records": replayFaultThreshold, "consumed_source_records": consumed,
		"resources_before_diagnostic_write": resources, "metrics": work}
	file, err := os.OpenFile(replayFaultResult+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		replayFaultFatal(err)
	}
	if err := json.NewEncoder(file).Encode(result); err != nil {
		replayFaultFatal(err)
	}
	if err := file.Sync(); err != nil {
		replayFaultFatal(err)
	}
	if err := file.Close(); err != nil {
		replayFaultFatal(err)
	}
	if err := os.Rename(replayFaultResult+".tmp", replayFaultResult); err != nil {
		replayFaultFatal(err)
	}
	os.Exit(77)
}

func replayFaultFatal(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "replay measurement fault failed: %v\n", err)
	os.Exit(78)
}
