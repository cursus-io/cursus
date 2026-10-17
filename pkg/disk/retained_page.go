package disk

import (
	"context"
	"errors"
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
)

var ErrRetainedPageLimit = errors.New("first retained record exceeds page byte limit")

// RetainedPage contains every physical record in [requested start, EndOffset),
// including transaction data and control markers. EndOffset is exclusive;
// Exhausted means the entire fixed requested interval has been certified.
// DecodedBytes counts serialized record payload bytes (excluding frame headers).
// Compacted indicates segment-local marked gap authority was used by the scan.
// ContentGeneration is local only and cannot authenticate a remote broker.
type RetainedPage struct {
	Messages          []types.Message
	EndOffset         uint64
	Exhausted         bool
	Compacted         bool
	DecodedBytes      int
	ContentGeneration ContentGeneration
}

// ReadRetainedPage reads a bounded, detached raw page at one content generation.
// Both limits must be positive; an oversized first record returns
// ErrRetainedPageLimit. At most one additional MaxMessageSize-bounded frame is
// decoded to certify the cutoff and any preceding compacted holes. The scanner
// starts at segment base, never trusting a potentially stale sparse index.
// Callers must not hold partition locks: use Capture/CheckContentGeneration
// around separate proof/page calls and PublishIfContentGeneration to publish.
func (d *DiskHandler) ReadRetainedPage(ctx context.Context, start, end uint64, maxRecords, maxBytes int) (RetainedPage, error) {
	var page RetainedPage
	if ctx == nil || maxRecords <= 0 || maxBytes <= 0 {
		return page, fmt.Errorf("retained page requires context and positive limits")
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	unlock, err := lockProofContext(ctx, &d.maintenanceMu, &d.appendMu, &d.mu, &d.ioMu)
	if err != nil {
		return page, err
	}
	defer unlock()
	d.writeFailureMu.RLock()
	availabilityErr := d.proofAvailabilityLocked()
	d.writeFailureMu.RUnlock()
	if availabilityErr != nil {
		return page, availabilityErr
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	// FlushedOffset is a written tail, not itself an fsync witness. Sync while
	// holding the same locks as scan/stamp before certifying durable records.
	if d.file != nil {
		if err := d.syncFile(d.file); err != nil {
			return page, d.markWriteUnavailable(fmt.Errorf("sync retained page: %w", err))
		}
	}
	result, err := d.scanRetainedFramesLocked(ctx, start, end, true, func(frame compactionFrame) (bool, error) {
		decoded := frame.size - 4
		if len(page.Messages) >= maxRecords || decoded > maxBytes-page.DecodedBytes {
			if len(page.Messages) == 0 {
				return false, ErrRetainedPageLimit
			}
			return false, nil
		}
		page.Messages = append(page.Messages, retainedMessage(frame.message))
		page.DecodedBytes += decoded
		return true, nil
	})
	if err != nil {
		return RetainedPage{}, err
	}
	d.writeFailureMu.RLock()
	defer d.writeFailureMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return RetainedPage{}, err
	}
	if err := d.proofAvailabilityLocked(); err != nil {
		return RetainedPage{}, err
	}
	page.EndOffset, page.Exhausted, page.Compacted = result.end, result.exhausted, result.compacted
	page.ContentGeneration = d.contentGenerationLocked()
	return page, nil
}
