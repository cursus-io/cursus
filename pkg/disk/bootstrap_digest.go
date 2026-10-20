package disk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/cursus-io/cursus/pkg/types"
	"sync/atomic"

	"github.com/cursus-io/cursus/util"
)

// RetainedRangeDigest proves actual canonical physical records and explicit
// logical bounds, including compacted trailing holes and fully empty ranges.
// This recovery-only operation scans retained history in bounded record memory;
// it does not trust a previous bootstrap receipt or write a derived sidecar.
func (d *DiskHandler) RetainedRangeDigest(start, end uint64) (string, error) {
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	d.Flush()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if err := d.writeAvailabilityError(); err != nil {
		return "", err
	}
	if len(d.segments) == 0 || start != d.segments[0] || start > end || end > atomic.LoadUint64(&d.FlushedOffset) {
		return "", fmt.Errorf("digest range outside durable retained anchor")
	}
	h := sha256.New()
	_, _ = h.Write([]byte("cursus-retained-range-v1\x00"))
	var bounds [16]byte
	binary.BigEndian.PutUint64(bounds[:8], start)
	binary.BigEndian.PutUint64(bounds[8:], end)
	_, _ = h.Write(bounds[:])
	_, err := d.scanRetainedRangeLocked(context.Background(), start, end, func(msg types.DiskMessage) error {
		canonical, err := util.SerializeDiskMessage(msg)
		if err != nil {
			return err
		}
		if err = validateSerializedDiskMessageSize(canonical); err != nil {
			return err
		}
		length, ok := util.SafeIntToUint32(len(canonical))
		if !ok {
			return fmt.Errorf("canonical digest frame too large")
		}
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], length)
		_, _ = h.Write(header[:])
		_, _ = h.Write(canonical)
		return nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
