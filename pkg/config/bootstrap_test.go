package config

import (
	"flag"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func loadBootstrapTestConfig(t *testing.T, extension, data string, args ...string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config."+extension)
	require.NoError(t, os.WriteFile(path, []byte(data), 0600))
	oldFlags, oldArgs := flag.CommandLine, os.Args
	flag.CommandLine = flag.NewFlagSet("bootstrap-test", flag.ContinueOnError)
	os.Args = append([]string{"bootstrap-test", "--config", path}, args...)
	defer func() { flag.CommandLine = oldFlags; os.Args = oldArgs }()
	return LoadConfig()
}

func TestBootstrapConfigDefaultsAndInputs(t *testing.T) {
	cfg, err := loadBootstrapTestConfig(t, "yaml", "{}")
	require.NoError(t, err)
	require.Equal(t, int64(8<<30), cfg.BootstrapMaxTransferBytes)
	require.Equal(t, int64(32<<30), cfg.BootstrapMaxTotalBytes)
	for _, format := range []struct{ ext, data string }{
		{"yaml", "bootstrap_max_transfer_bytes: 10\nbootstrap_max_total_bytes: 20"},
		{"json", `{"bootstrap.max.transfer.bytes":10,"bootstrap.max.total.bytes":20}`},
	} {
		t.Run(format.ext, func(t *testing.T) {
			cfg, err := loadBootstrapTestConfig(t, format.ext, format.data)
			require.NoError(t, err)
			require.EqualValues(t, 10, cfg.BootstrapMaxTransferBytes)
			require.EqualValues(t, 20, cfg.BootstrapMaxTotalBytes)
			cfg, err = loadBootstrapTestConfig(t, format.ext, format.data, "--bootstrap-max-transfer-bytes=30", "--bootstrap-max-total-bytes=40")
			require.NoError(t, err)
			require.EqualValues(t, 30, cfg.BootstrapMaxTransferBytes)
			require.EqualValues(t, 40, cfg.BootstrapMaxTotalBytes)
			t.Setenv("BOOTSTRAP_MAX_TRANSFER_BYTES", "50")
			t.Setenv("BOOTSTRAP_MAX_TOTAL_BYTES", "60")
			cfg, err = loadBootstrapTestConfig(t, format.ext, format.data, "--bootstrap-max-transfer-bytes=30", "--bootstrap-max-total-bytes=40")
			require.NoError(t, err)
			require.EqualValues(t, 50, cfg.BootstrapMaxTransferBytes)
			require.EqualValues(t, 60, cfg.BootstrapMaxTotalBytes)
		})
	}
}

func TestBootstrapConfigRejectsInvalidAdmission(t *testing.T) {
	for _, value := range []string{"0", "-1", "9223372036854775808", "invalid", ""} {
		for _, key := range []string{"BOOTSTRAP_MAX_TRANSFER_BYTES", "BOOTSTRAP_MAX_TOTAL_BYTES"} {
			t.Run(key+value, func(t *testing.T) {
				t.Setenv(key, value)
				_, err := loadBootstrapTestConfig(t, "yaml", "{}")
				require.Error(t, err)
			})
		}
	}
	for _, data := range []string{"bootstrap_max_transfer_bytes: 0\nbootstrap_max_total_bytes: 0", "bootstrap_max_transfer_bytes: -1", "bootstrap_max_total_bytes: 1", "bootstrap_max_transfer_bytes: 9223372036854775807", "bootstrap_max_transfer_bytes: 9223372036854775808"} {
		_, err := loadBootstrapTestConfig(t, "yaml", data)
		require.Error(t, err)
	}
	for _, data := range []string{`{"bootstrap.max.transfer.bytes":0,"bootstrap.max.total.bytes":0}`, `{"bootstrap.max.total.bytes":-1}`, `{"bootstrap.max.transfer.bytes":9223372036854775808}`} {
		_, err := loadBootstrapTestConfig(t, "json", data)
		require.Error(t, err)
	}
	for _, arg := range []string{"--bootstrap-max-transfer-bytes=0", "--bootstrap-max-total-bytes=0", "--bootstrap-max-transfer-bytes=-1", "--bootstrap-max-transfer-bytes=9223372036854775808", "--bootstrap-max-total-bytes=malformed"} {
		_, err := loadBootstrapTestConfig(t, "yaml", "{}", arg)
		require.Error(t, err)
	}
	cfg := &Config{}
	transfer, total, err := cfg.BootstrapAdmissionBytes()
	require.NoError(t, err)
	require.Equal(t, DefaultBootstrapMaxTransferBytes, transfer)
	require.Equal(t, DefaultBootstrapMaxTotalBytes, total)
	cfg.BootstrapMaxTransferBytes = BootstrapStructuralMaxBytes
	cfg.BootstrapMaxTotalBytes = math.MaxInt64
	_, _, err = cfg.BootstrapAdmissionBytes()
	require.NoError(t, err)
}
