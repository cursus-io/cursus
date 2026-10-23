package fsm

import (
	"context"
	"errors"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

// Ordinary pages obey the requested quantum. A first valid oversized record
// retries once with one record and the existing storage/wire maximum. This is
// the same progress contract required by cleanup, without changing its reader.
func readRecoveryPage(ctx context.Context, p *topic.Partition, start, end uint64, records, bytes int) (disk.RetainedPage, error) {
	if bytes == 0 {
		bytes = util.MaxMessageSize
	}
	page, err := p.ReadReplicaRetainedPage(ctx, start, end, records, bytes)
	if errors.Is(err, disk.ErrRetainedPageLimit) && bytes < util.MaxMessageSize {
		return p.ReadReplicaRetainedPage(ctx, start, end, 1, util.MaxMessageSize)
	}
	return page, err
}
