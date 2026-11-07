//go:build e2e_faults

package faultpoint

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func Pause(point, key string) error {
	path, armed, err := consume(point, key)
	if err != nil || !armed {
		return err
	}
	timer := time.NewTimer(2 * time.Minute)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path + ".release"); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("fault point %s/%s timed out without release", point, key)
		}
	}
}

func Fail(point, key string, failure error) error {
	_, armed, err := consume(point, key)
	if err != nil || !armed {
		return err
	}
	return fmt.Errorf("injected %s/%s: %w", point, key, failure)
}

func FailOffset(point, topic string, partition int, offset uint64, failure error) error {
	return Fail(point, fmt.Sprintf("%s-%d-%d", topic, partition, offset), failure)
}

func PauseOffset(point, topic string, partition int, offset uint64) error {
	return Pause(point, fmt.Sprintf("%s-%d-%d", topic, partition, offset))
}

func consume(point, key string) (string, bool, error) {
	if os.Getenv("CURSUS_E2E_FAULTS") != "1" {
		return "", false, nil
	}
	for _, component := range []string{point, key} {
		if component == "" || component == "." || component == ".." || strings.ContainsAny(component, "/\\") {
			return "", false, fmt.Errorf("invalid fault point component %q", component)
		}
	}
	root := os.Getenv("CURSUS_E2E_FAULT_ROOT")
	if root == "" {
		root = "/run/cursus-e2e-faults"
	}
	path := filepath.Join(root, point, key)
	if err := os.Rename(path, path+".active"); err != nil {
		if os.IsNotExist(err) {
			return path, false, nil
		}
		return path, false, err
	}
	if err := os.WriteFile(path+".entered", nil, 0o600); err != nil {
		return path, false, err
	}
	return path, true, nil
}
