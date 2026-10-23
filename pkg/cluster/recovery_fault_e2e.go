//go:build e2e_faults

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/internal/e2efaults"
	"github.com/cursus-io/cursus/pkg/topic"
)

// Test-only source stall, before any FSM/storage lock. The same hook is copied
// to the measured baseline; it does not change scheduling or recovery data.
func injectedRecoverySourceDelay(ctx context.Context, name string) error {
	if os.Getenv(e2efaults.RuntimeGateEnvironment) != "1" || topic.ValidateName(name) != nil {
		return nil
	}
	root := filepath.Join(e2efaults.SentinelRoot, "recovery-source")
	if _, err := os.Stat(root); err != nil {
		return nil
	}
	marker, err := os.OpenFile(filepath.Join(root, name+".entered"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		_, _ = marker.WriteString(strconv.FormatInt(time.Now().UnixNano(), 10))
		_ = marker.Close()
	}
	data, err := os.ReadFile(filepath.Join(root, name))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	delay, err := time.ParseDuration(strings.TrimSpace(string(data)))
	if err != nil {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
