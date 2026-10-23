package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/pkg/wire"
)

type RecoveryConfig struct {
	BytesPerSecond       int64 `yaml:"bytes_per_second" json:"bytes_per_second"`
	BurstBytes           int64 `yaml:"burst_bytes" json:"burst_bytes"`
	SourceBytesPerSecond int64 `yaml:"source_bytes_per_second" json:"source_bytes_per_second"`
	SourceBurstBytes     int64 `yaml:"source_burst_bytes" json:"source_burst_bytes"`
	QuantumBytes         int64 `yaml:"quantum_bytes" json:"quantum_bytes"`
	Workers              int   `yaml:"workers" json:"workers"`
	SourceConcurrency    int   `yaml:"source_concurrency" json:"source_concurrency"`
	QueueEntries         int   `yaml:"queue_entries" json:"queue_entries"`
	TurnTimeoutMS        int   `yaml:"turn_timeout_ms" json:"turn_timeout_ms"`
}

func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{BytesPerSecond: 1 << 20, BurstBytes: 2 << 20, SourceBytesPerSecond: 512 << 10, SourceBurstBytes: 1 << 20, QuantumBytes: 1 << 20, Workers: 4, SourceConcurrency: 1, QueueEntries: 100000, TurnTimeoutMS: 5000}
}

func (r RecoveryConfig) Limits() (recovery.Limits, error) {
	l := recovery.Limits{BytesPerSecond: r.BytesPerSecond, BurstBytes: r.BurstBytes, SourceBytesPerSecond: r.SourceBytesPerSecond, SourceBurstBytes: r.SourceBurstBytes, MaxUnitBytes: wire.MaxFramePayload + 65536, Concurrency: r.Workers, SourceConcurrency: r.SourceConcurrency, MaxSources: 4096, MaxWaiters: 32}
	if r.QuantumBytes < 4096 || r.QuantumBytes > 16<<20 || r.QuantumBytes > r.SourceBurstBytes || r.QueueEntries < r.Workers || r.QueueEntries > 1000000 || r.TurnTimeoutMS < 100 || r.TurnTimeoutMS > 30000 {
		return l, fmt.Errorf("invalid recovery quantum, queue capacity or turn timeout")
	}
	return l, l.Validate()
}

// Keep a continuation for every permitted materialized partition. An undersized
// queue could hide later partitions behind continuously active work. Entries
// allocate on admission, not at this configured upper bound.
func (cfg *Config) ValidateRecovery() error {
	if _, err := cfg.RecoverySettings().Limits(); err != nil {
		return err
	}
	maxPartitions := cfg.MaxPartitions
	if maxPartitions <= 0 {
		maxPartitions = DefaultConfig().MaxPartitions
	}
	if cfg.RecoverySettings().QueueEntries < maxPartitions {
		return fmt.Errorf("recovery.queue_entries must cover max_partitions")
	}
	_, err := cfg.RecoveryBootstrapTimeout()
	return err
}

// RecoveryBootstrapTimeout retains the old seven-minute receiver work allowance
// and adds a finite transfer allowance. Six source owners and two inbound owners
// share the broker rate; 2x wire bytes covers base64 and bounded frame overhead.
// Peer allowance includes two simultaneous transfers involving the same peer.
// This is a sizing bound, not a bandwidth reservation against other recovery.
func (cfg *Config) RecoveryBootstrapTimeout() (time.Duration, error) {
	r := cfg.RecoverySettings()
	if _, err := r.Limits(); err != nil {
		return 0, err
	}
	transfer, _, err := cfg.BootstrapAdmissionBytes()
	if err != nil {
		return 0, err
	}
	// BootstrapStructuralMaxBytes is MaxInt64/32, so these products and the
	// positive rate (at most 2^40) cannot overflow before division.
	seconds := max((transfer*16+r.BytesPerSecond-1)/r.BytesPerSecond, (transfer*4+r.SourceBytesPerSecond-1)/r.SourceBytesPerSecond)
	const maximum = 7 * 24 * time.Hour
	if seconds > int64((maximum-7*time.Minute)/time.Second) {
		return 0, fmt.Errorf("recovery rates and bootstrap_max_transfer_bytes require more than seven days; raise rates or lower the transfer limit")
	}
	return 7*time.Minute + time.Duration(seconds)*time.Second, nil
}

// Zero-value programmatic configurations inherit defaults. Explicit file/env
// values are validated during loading; zero does not mean unlimited.
func (cfg *Config) RecoverySettings() RecoveryConfig {
	if cfg == nil || cfg.Recovery == (RecoveryConfig{}) {
		return DefaultRecoveryConfig()
	}
	return cfg.Recovery
}

func (cfg *Config) loadRecoveryEnvironment() error {
	values := map[string]*int64{"RECOVERY_BYTES_PER_SECOND": &cfg.Recovery.BytesPerSecond, "RECOVERY_BURST_BYTES": &cfg.Recovery.BurstBytes, "RECOVERY_SOURCE_BYTES_PER_SECOND": &cfg.Recovery.SourceBytesPerSecond, "RECOVERY_SOURCE_BURST_BYTES": &cfg.Recovery.SourceBurstBytes, "RECOVERY_QUANTUM_BYTES": &cfg.Recovery.QuantumBytes}
	for name, target := range values {
		if raw, ok := os.LookupEnv(name); ok {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			*target = value
		}
	}
	counts := map[string]*int{"RECOVERY_WORKERS": &cfg.Recovery.Workers, "RECOVERY_SOURCE_CONCURRENCY": &cfg.Recovery.SourceConcurrency, "RECOVERY_QUEUE_ENTRIES": &cfg.Recovery.QueueEntries, "RECOVERY_TURN_TIMEOUT_MS": &cfg.Recovery.TurnTimeoutMS}
	for name, target := range counts {
		if raw, ok := os.LookupEnv(name); ok {
			value, err := strconv.Atoi(raw)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			*target = value
		}
	}
	// Loaded configurations started from defaults. An explicitly all-zero
	// object must not take the programmatic zero-value compatibility fallback.
	if _, err := cfg.Recovery.Limits(); err != nil {
		return err
	}
	return cfg.ValidateRecovery()
}
