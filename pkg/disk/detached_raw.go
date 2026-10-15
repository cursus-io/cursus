package disk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type DetachedRawBounds struct {
	StartOffset, EndOffset uint64
	RawBytes, IndexBytes   int64
}

// DetachedRawWork measures actual cumulative read/decode work, not reservations.
type DetachedRawWork struct {
	ReadBytes, IndexReadBytes, DecodedRecords uint64
}

type DetachedRawPage struct {
	Messages     []types.DiskMessage
	NextOffset   uint64
	Exhausted    bool
	DecodedBytes int
}

type detachedRawFrame struct {
	message      types.DiskMessage
	payloadBytes int
}

// DetachedRawLease owns independent handles and a serialized forward cursor.
// Immutable files are trusted after preparation; out-of-band mutation is unsupported.
type DetachedRawLease struct {
	mu                   sync.Mutex
	transfer             *bootstrapTransfer
	records, index       *os.File
	start, end           uint64
	rawBytes, indexBytes int64
	position             int64
	next                 uint64
	initialized          bool
	pending              *detachedRawFrame
	closed               bool
	work                 DetachedRawWork
}

func (l *DetachedRawLease) Bounds() DetachedRawBounds {
	return DetachedRawBounds{StartOffset: l.start, EndOffset: l.end, RawBytes: l.rawBytes, IndexBytes: l.indexBytes}
}

func (l *DetachedRawLease) Work() DetachedRawWork {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.work
}

func (l *DetachedRawLease) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.transfer
	if !l.closed {
		err := errors.Join(l.records.Close(), l.index.Close())
		l.closed = true
		l.pending = nil
		t.mu.Lock()
		t.detached.leases--
		if t.detached.retired && t.detached.leases == 0 {
			err = errors.Join(err, t.cleanupLocked())
		}
		t.mu.Unlock()
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached.retired && t.detached.leases == 0 {
		return t.cleanupLocked()
	}
	return nil
}

func (l *DetachedRawLease) seek(ctx context.Context, start uint64) error {
	lo, hi := int64(0), l.indexBytes/16
	position := int64(0)
	for lo < hi {
		if err := ctx.Err(); err != nil {
			return err
		}
		mid := lo + (hi-lo)/2
		var entry [16]byte
		n, err := l.index.ReadAt(entry[:], mid*16)
		l.work.IndexReadBytes += uint64(n)
		if err != nil {
			return err
		}
		offset, physical := binary.BigEndian.Uint64(entry[:8]), binary.BigEndian.Uint64(entry[8:])
		if offset < l.start || offset >= l.end || physical >= uint64(l.rawBytes) {
			return fmt.Errorf("invalid detached sparse index entry")
		}
		if offset <= start {
			lo = mid + 1
			position = int64(physical)
		} else {
			hi = mid
		}
	}
	l.position, l.next, l.pending, l.initialized = position, start, nil, true
	return nil
}

func (l *DetachedRawLease) readFrame(ctx context.Context) (*detachedRawFrame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.position == l.rawBytes {
		return nil, io.EOF
	}
	if l.position < 0 || l.rawBytes-l.position < 4 {
		return nil, fmt.Errorf("truncated detached frame header")
	}
	var header [4]byte
	n, err := l.records.ReadAt(header[:], l.position)
	l.work.ReadBytes += uint64(n)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > MaxMessageSize || int64(length) > l.rawBytes-l.position-4 {
		return nil, fmt.Errorf("invalid detached frame length")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	payload := make([]byte, length)
	n, err = l.records.ReadAt(payload, l.position+4)
	l.work.ReadBytes += uint64(n)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	message, err := util.DeserializeDiskMessage(payload)
	l.work.DecodedRecords++
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if message.Offset < l.start || message.Offset >= l.end {
		return nil, fmt.Errorf("detached frame outside lease bounds")
	}
	l.position += 4 + int64(length)
	return &detachedRawFrame{message: message, payloadBytes: int(length)}, nil
}

// ReadRaw returns every canonical field, including control and transaction data.
// Limits count payload bytes, matching retained pages. One omitted bounded frame
// may be decoded and retained for the next call; errors never return partial pages.
func (l *DetachedRawLease) ReadRaw(ctx context.Context, start uint64, maxRecords, maxBytes int) (page DetachedRawPage, resultErr error) {
	if ctx == nil {
		return DetachedRawPage{}, fmt.Errorf("detached raw read requires context")
	}
	unlock, err := lockProofContext(ctx, &l.mu)
	if err != nil {
		return DetachedRawPage{}, err
	}
	defer unlock()
	if l.closed {
		return DetachedRawPage{}, ErrDetachedBootstrapClosed
	}
	if maxRecords <= 0 || maxBytes <= 0 || start < l.start || start > l.end {
		return DetachedRawPage{}, fmt.Errorf("invalid detached raw page bounds or limits")
	}
	defer func() {
		if resultErr != nil {
			l.initialized = false
			l.pending = nil
			page = DetachedRawPage{}
		}
	}()
	if err := ctx.Err(); err != nil {
		return DetachedRawPage{}, err
	}
	page.NextOffset = start
	if start == l.end {
		page.Exhausted = true
		return page, nil
	}
	if !l.initialized || start != l.next {
		if err := l.seek(ctx, start); err != nil {
			return DetachedRawPage{}, err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return DetachedRawPage{}, err
		}
		frame := l.pending
		if frame == nil {
			var err error
			frame, err = l.readFrame(ctx)
			if errors.Is(err, io.EOF) && l.position == l.rawBytes {
				page.NextOffset = l.end
				page.Exhausted = true
				l.next = l.end
				return page, nil
			}
			if err != nil {
				return DetachedRawPage{}, err
			}
		}
		l.pending = nil
		if frame.message.Offset < start {
			continue
		}
		if frame.payloadBytes > maxBytes-page.DecodedBytes {
			l.pending = frame
			l.next = frame.message.Offset
			if len(page.Messages) == 0 {
				return DetachedRawPage{}, ErrRetainedPageLimit
			}
			page.NextOffset = l.next
			return page, nil
		}
		page.Messages = append(page.Messages, frame.message)
		page.DecodedBytes += frame.payloadBytes
		l.next = frame.message.Offset + 1
		page.NextOffset = l.next
		if l.position == l.rawBytes {
			l.next = l.end
			page.NextOffset = l.end
			page.Exhausted = true
			return page, nil
		}
		if len(page.Messages) == maxRecords {
			return page, nil
		}
	}
}
