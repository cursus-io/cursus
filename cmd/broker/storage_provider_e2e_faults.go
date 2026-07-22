//go:build e2e_faults

package main

import (
	"github.com/cursus-io/cursus/internal/e2efaults"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

func newStorageProvider(dm *disk.DiskManager, logDir string) (topic.HandlerProvider, error) {
	return e2efaults.NewStorageProvider(dm, logDir)
}
