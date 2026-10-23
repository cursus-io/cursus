package config

import (
	"testing"
	"time"
)

func TestRecoveryBootstrapLifetimeAccountsForQuotaAndRemainsBounded(t *testing.T) {
	cfg := DefaultConfig()
	timeout, err := cfg.RecoveryBootstrapTimeout()
	if err != nil {
		t.Fatal(err)
	}
	if timeout != 7*time.Minute+131072*time.Second {
		t.Fatal(timeout)
	}
	// The full admitted unit must fit even with the budgeted eight owners.
	if timeout <= time.Duration(2*cfg.BootstrapMaxTransferBytes/cfg.Recovery.SourceBytesPerSecond)*time.Second {
		t.Fatal("valid transfer cannot finish")
	}
	cfg.Recovery.BytesPerSecond, cfg.Recovery.SourceBytesPerSecond = 1, 1
	if err := cfg.ValidateRecovery(); err == nil {
		t.Fatal("perpetually expiring transfer accepted")
	}
	cfg.BootstrapMaxTransferBytes, cfg.BootstrapMaxTotalBytes = 1, 1
	if err := cfg.ValidateRecovery(); err != nil {
		t.Fatal(err)
	}
	cfg.BootstrapMaxTransferBytes, cfg.BootstrapMaxTotalBytes = BootstrapStructuralMaxBytes, BootstrapStructuralMaxBytes
	if _, err := cfg.RecoveryBootstrapTimeout(); err == nil {
		t.Fatal("overflow or unbounded lifetime")
	}
}

func TestRecoveryQueueMustCoverMaterializationLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Recovery.QueueEntries = 4
	if err := cfg.ValidateRecovery(); err == nil {
		t.Fatal("overflow starvation configuration accepted")
	}
	cfg.MaxPartitions = 4
	if err := cfg.ValidateRecovery(); err != nil {
		t.Fatal(err)
	}
	cfg.MaxPartitions = 0
	if err := cfg.ValidateRecovery(); err == nil {
		t.Fatal("implicit materialization default escaped queue coverage")
	}
}

func TestRecoveryLoadedZeroConfigDoesNotDisableAdmission(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Recovery = RecoveryConfig{}
	if err := cfg.loadRecoveryEnvironment(); err == nil {
		t.Fatal("explicit zero recovery config accepted")
	}
	if err := cfg.ValidateRecovery(); err != nil {
		t.Fatal("programmatic compatibility fallback failed", err)
	}
}

func TestRecoveryLimitsRejectUnboundedOrImpossibleSettings(t *testing.T) {
	for name, mutate := range map[string]func(*RecoveryConfig){
		"zero rate":             func(r *RecoveryConfig) { r.BytesPerSecond = 0 },
		"source exceeds broker": func(r *RecoveryConfig) { r.SourceBytesPerSecond = r.BytesPerSecond + 1 },
		"too many workers":      func(r *RecoveryConfig) { r.Workers = 33 },
		"impossible quantum":    func(r *RecoveryConfig) { r.QuantumBytes = r.SourceBurstBytes + 1 },
		"unbounded queue":       func(r *RecoveryConfig) { r.QueueEntries = 1 << 30 },
		"long turn":             func(r *RecoveryConfig) { r.TurnTimeoutMS = 30001 },
	} {
		t.Run(name, func(t *testing.T) {
			r := DefaultRecoveryConfig()
			mutate(&r)
			if _, err := r.Limits(); err == nil {
				t.Fatal("invalid settings accepted")
			}
		})
	}
	if _, err := DefaultRecoveryConfig().Limits(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryEnvironmentUsesExplicitBytesAndMilliseconds(t *testing.T) {
	cfg := DefaultConfig()
	t.Setenv("RECOVERY_BYTES_PER_SECOND", "8388608")
	t.Setenv("RECOVERY_TURN_TIMEOUT_MS", "1234")
	if err := cfg.loadRecoveryEnvironment(); err != nil {
		t.Fatal(err)
	}
	if cfg.Recovery.BytesPerSecond != 8388608 || cfg.Recovery.TurnTimeoutMS != 1234 {
		t.Fatal(cfg.Recovery)
	}
	t.Setenv("RECOVERY_BYTES_PER_SECOND", "8MiB")
	if err := cfg.loadRecoveryEnvironment(); err == nil {
		t.Fatal("ambiguous unit accepted")
	}
}
