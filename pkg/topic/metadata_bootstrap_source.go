package topic

import (
	"context"
	"github.com/cursus-io/cursus/pkg/disk"
)

type metadataBootstrapStorage interface {
	ExportRetainedBootstrapContext(context.Context, disk.ContentGeneration, uint64, uint64, uint64, disk.BootstrapFence) (*disk.BootstrapExport, error)
}

// MetadataBootstrapSourceHWMContext requires an open partition, authoritative
// HWM, and completed local recovery even for zero. No lock survives this accessor.
func (p *Partition) MetadataBootstrapSourceHWMContext(ctx context.Context) (uint64, error) {
	return p.replicaSourceHWMContext(ctx, true)
}

// ExportMetadataBootstrapContext holds no partition gates across copying or
// checksumming. The caller owns the finite export lifetime and later publication
// fence; returning a private export is not permission to serve it remotely.
func (p *Partition) ExportMetadataBootstrapContext(ctx context.Context, generation disk.ContentGeneration, anchor, start, end uint64, fence disk.BootstrapFence) (*disk.BootstrapExport, error) {
	storage, ok := p.dh.(metadataBootstrapStorage)
	if !ok {
		return nil, ErrReplicaSourceUnsupported
	}
	return storage.ExportRetainedBootstrapContext(ctx, generation, anchor, start, end, fence)
}
