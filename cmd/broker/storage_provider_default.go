//go:build !e2e_faults

package main

import (
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

func newStorageProvider(dm *disk.DiskManager, _ string) (topic.HandlerProvider, error) {
	return dm, nil
}
