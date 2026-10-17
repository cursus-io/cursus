package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

const (
	DefaultBootstrapMaxTransferBytes int64 = 8 << 30
	DefaultBootstrapMaxTotalBytes    int64 = 32 << 30
	BootstrapStructuralMaxBytes      int64 = math.MaxInt64 / 32
)

// BootstrapAdmissionBytes preserves all-zero programmatic configurations. File,
// flag and environment inputs are validated strictly before normalization.
func (cfg *Config) BootstrapAdmissionBytes() (transfer, total int64, err error) {
	transfer, total = cfg.BootstrapMaxTransferBytes, cfg.BootstrapMaxTotalBytes
	if transfer == 0 && total == 0 {
		return DefaultBootstrapMaxTransferBytes, DefaultBootstrapMaxTotalBytes, nil
	}
	return transfer, total, validateBootstrapAdmission(transfer, total)
}

func validateBootstrapAdmission(transfer, total int64) error {
	if transfer <= 0 || transfer > BootstrapStructuralMaxBytes || total < transfer {
		return fmt.Errorf("bootstrap limits require 0 < transfer bytes <= %d and total bytes >= transfer bytes", BootstrapStructuralMaxBytes)
	}
	return nil
}

func (cfg *Config) loadBootstrapAdmissionEnvironment() error {
	for _, field := range []struct {
		key   string
		value *int64
	}{
		{"BOOTSTRAP_MAX_TRANSFER_BYTES", &cfg.BootstrapMaxTransferBytes},
		{"BOOTSTRAP_MAX_TOTAL_BYTES", &cfg.BootstrapMaxTotalBytes},
	} {
		if raw, exists := os.LookupEnv(field.key); exists {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return fmt.Errorf("%s: %w", field.key, err)
			}
			*field.value = value
		}
	}
	return validateBootstrapAdmission(cfg.BootstrapMaxTransferBytes, cfg.BootstrapMaxTotalBytes)
}
