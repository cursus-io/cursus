package topic

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func validateArchiveRoot(path string, allowMissing bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && allowMissing {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect archive directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("archive directory must be a real directory")
	}
	return nil
}

func archiveRootWithinLogRoot(logRoot, archiveRoot string) (bool, error) {
	resolvedLogRoot, err := resolvePhysicalPath(logRoot)
	if err != nil {
		return false, fmt.Errorf("resolve physical log directory: %w", err)
	}
	resolvedArchiveRoot, err := resolvePhysicalPath(archiveRoot)
	if err != nil {
		return false, fmt.Errorf("resolve physical archive directory: %w", err)
	}
	return pathWithin(resolvedLogRoot, resolvedArchiveRoot), nil
}
func resolvePhysicalPath(path string) (string, error) {
	probe := path
	missing := make([]string, 0)
	for {
		_, err := os.Lstat(probe)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return "", err
		}
		missing = append(missing, filepath.Base(probe))
		probe = parent
	}
	resolved, err := filepath.EvalSymlinks(probe)
	if err != nil {
		return "", err
	}
	for index := len(missing) - 1; index >= 0; index-- {
		resolved = filepath.Join(resolved, missing[index])
	}
	return resolved, nil
}
