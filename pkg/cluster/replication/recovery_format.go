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

var syncRaftDirectoryFn = syncRaftDirectory

func ensureRaftRecoveryFormat(dataDir string) error {
	markerPath := filepath.Join(dataDir, raftFormatMarkerName)
	markerInfo, err := os.Lstat(markerPath)
	switch {
	case err == nil:
		if markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() {
			return fmt.Errorf("%w: Raft format marker is not a regular file", fsm.ErrUnsupportedRecoveryProtocol)
		}
		if err := validateRaftFormatMarker(dataDir); err != nil {
			return err
		}
		if err := syncRaftDirectoryFn(dataDir); err != nil {
			return fmt.Errorf("sync Raft format marker directory: %w", err)
		}
		return nil
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
	if writeErr == nil {
		if err := syncRaftDirectoryFn(dataDir); err != nil {
			writeErr = fmt.Errorf("sync Raft format marker directory: %w", err)
		}
	}
	return writeErr
}

func validateRaftFormatMarker(dataDir string) error {
	_, err := readRaftFormatMarker(dataDir)
	return err
}

func readRaftFormatMarker(dataDir string) (int, error) {
	markerPath := filepath.Join(dataDir, raftFormatMarkerName)
	markerInfo, err := os.Lstat(markerPath)
	if err != nil {
		return 0, fmt.Errorf("inspect Raft format marker: %w", err)
	}
	if markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() {
		return 0, fmt.Errorf("%w: Raft format marker is not a regular file", fsm.ErrUnsupportedRecoveryProtocol)
	}
	// #nosec G304 -- markerPath is a constant child checked above to be a regular, non-symlink file.
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return 0, fmt.Errorf("read Raft format marker: %w", err)
	}
	version, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || (version != fsm.SnapshotVersionCurrent && version != fsm.SnapshotVersionLegacyEpoch) {
		return 0, fmt.Errorf(
			"%w: Raft format marker %q is not supported by version %d; clean bootstrap required",
			fsm.ErrUnsupportedRecoveryProtocol, strings.TrimSpace(string(data)), fsm.SnapshotVersionCurrent,
		)
	}
	return version, nil
}

// upgradeRaftRecoveryFormat permanently records that the local state has been
// recovered by the current binary. It is called only after Raft has opened and
// replayed the persisted state successfully, so a failed upgrade leaves the
// legacy marker intact and remains rollback-safe.
func upgradeRaftRecoveryFormat(dataDir string) (err error) {
	version, err := readRaftFormatMarker(dataDir)
	if err != nil {
		return err
	}
	if version == fsm.SnapshotVersionCurrent {
		return nil
	}

	markerPath := filepath.Join(dataDir, raftFormatMarkerName)
	temp, err := os.CreateTemp(dataDir, raftFormatMarkerName+".upgrade-*")
	if err != nil {
		return fmt.Errorf("create upgraded Raft format marker: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		if temp != nil {
			err = errors.Join(err, temp.Close())
		}
		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove upgraded Raft format marker temp file: %w", removeErr))
		}
	}()
	if _, err := temp.WriteString(strconv.Itoa(fsm.SnapshotVersionCurrent) + "\n"); err != nil {
		return fmt.Errorf("write upgraded Raft format marker: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync upgraded Raft format marker: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close upgraded Raft format marker: %w", err)
	}
	temp = nil
	if err := replaceRaftFormatMarker(tempPath, markerPath); err != nil {
		return fmt.Errorf("install upgraded Raft format marker: %w", err)
	}
	if err := syncRaftDirectoryFn(dataDir); err != nil {
		return fmt.Errorf("sync upgraded Raft format marker directory: %w", err)
	}
	return nil
}
