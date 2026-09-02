package replication

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

const raftFormatMarkerName = ".cursus-raft-format"

func ensureRaftRecoveryFormat(dataDir string) error {
	markerPath := filepath.Join(dataDir, raftFormatMarkerName)
	markerInfo, err := os.Lstat(markerPath)
	switch {
	case err == nil:
		if markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() {
			return fmt.Errorf("%w: Raft format marker is not a regular file", fsm.ErrUnsupportedRecoveryProtocol)
		}
		return validateRaftFormatMarker(dataDir)
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("inspect Raft format marker: %w", err)
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("inspect Raft data directory: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf(
			"%w: Raft directory %q has data but no version %d marker; remove all Cursus persistent state and clean bootstrap",
			fsm.ErrUnsupportedRecoveryProtocol, dataDir, fsm.SnapshotVersionCurrent,
		)
	}

	// #nosec G304 -- markerPath is a constant child of the configured Raft data directory.
	marker, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return validateRaftFormatMarker(dataDir)
		}
		return fmt.Errorf("create Raft format marker: %w", err)
	}
	writeErr := error(nil)
	if _, err := marker.WriteString(strconv.Itoa(fsm.SnapshotVersionCurrent) + "\n"); err != nil {
		writeErr = fmt.Errorf("write Raft format marker: %w", err)
	} else if err := marker.Sync(); err != nil {
		writeErr = fmt.Errorf("sync Raft format marker: %w", err)
	}
	if closeErr := marker.Close(); writeErr == nil && closeErr != nil {
		writeErr = fmt.Errorf("close Raft format marker: %w", closeErr)
	}
	return writeErr
}

func validateRaftFormatMarker(dataDir string) error {
	markerPath := filepath.Join(dataDir, raftFormatMarkerName)
	markerInfo, err := os.Lstat(markerPath)
	if err != nil {
		return fmt.Errorf("inspect Raft format marker: %w", err)
	}
	if markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() {
		return fmt.Errorf("%w: Raft format marker is not a regular file", fsm.ErrUnsupportedRecoveryProtocol)
	}
	// #nosec G304 -- markerPath is a constant child checked above to be a regular, non-symlink file.
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return fmt.Errorf("read Raft format marker: %w", err)
	}
	version, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || version != fsm.SnapshotVersionCurrent {
		return fmt.Errorf(
			"%w: Raft format marker %q is not version %d; clean bootstrap required",
			fsm.ErrUnsupportedRecoveryProtocol, strings.TrimSpace(string(data)), fsm.SnapshotVersionCurrent,
		)
	}
	return nil
}
