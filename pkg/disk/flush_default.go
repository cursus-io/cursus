//go:build !linux

package disk

import (
	"fmt"
	"io"
	"net"
)

func (d *DiskHandler) SendCurrentSegmentToConn(conn net.Conn) (int, error) {
	d.mu.Lock()
	d.ioMu.Lock()
	defer d.mu.Unlock()
	defer d.ioMu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			return 0, err
		}
	}
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			return 0, err
		}
	}
	if _, err := d.file.Seek(0, 0); err != nil {
		return 0, err
	}
	_, err := io.Copy(conn, d.file)
	if err != nil {
		return 0, err
	}

	msgCount, err := d.countMessagesInSegment()
	if err != nil {
		return 0, fmt.Errorf("failed to count messages: %w", err)
	}

	return msgCount, nil
}
