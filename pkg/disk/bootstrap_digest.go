package disk

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
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
	var previous uint64
	havePrevious := false
	next := start
	for i, segment := range d.segments {
		if segment >= end {
			break
		}
		path := d.GetSegmentPath(segment)
		info, err := os.Stat(path)
		if err != nil {
			return "", err
		}
		compacted := d.segmentAllowsOffsetGaps(segment, info.Size())
		segmentEnd := end
		if i+1 < len(d.segments) && d.segments[i+1] < segmentEnd {
			segmentEnd = d.segments[i+1]
		}
		err = scanCompactionSegment(path, segment, false, compacted, func(frame compactionFrame) error {
			msg := frame.message
			if msg.Offset < start || msg.Offset >= end {
				return nil
			}
			if havePrevious && msg.Offset <= previous {
				return fmt.Errorf("non-increasing retained digest offset")
			}
			if !compacted && msg.Offset != next {
				return fmt.Errorf("unmarked retained digest offset gap")
			}
			if msg.Offset >= segmentEnd {
				return fmt.Errorf("record exceeds retained segment bounds")
			}
			previous = msg.Offset
			havePrevious = true
			next = msg.Offset + 1
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
		if !compacted && next != segmentEnd {
			return "", fmt.Errorf("unmarked retained digest trailing gap")
		}
		next = segmentEnd
	}
	if next != end {
		return "", fmt.Errorf("incomplete retained digest range")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
