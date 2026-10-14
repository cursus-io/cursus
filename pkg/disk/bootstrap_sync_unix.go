//go:build !windows

package disk

func syncBootstrapDirectory(path string) error { return syncDirectory(path) }
