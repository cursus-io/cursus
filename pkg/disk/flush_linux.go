//go:build linux
// +build linux

package disk

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/cursus-io/cursus/util"
	"golang.org/x/sys/unix"
)

func (d *DiskHandler) openSegment() error {
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND
	filePath := d.GetSegmentPath(d.CurrentSegment)

	f, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		return err
	}
	d.file = f
	d.writer = bufio.NewWriter(f)

	// sequential access hint
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
	return nil
}

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

	info, err := d.file.Stat()
	if err != nil {
		return 0, err
	}

	var offset int64 = 0
	size := info.Size()
	sysConn, ok := conn.(*net.TCPConn)
	if !ok {
		if _, err := d.file.Seek(0, 0); err != nil {
			util.Error("Seek failed: %v", err)
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

	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return 0, err
	}

	var sendErr error
	if err := rawConn.Control(func(fd uintptr) {
		inFd := int(d.file.Fd())
		outFd := int(fd)
		for offset < size {
			n, err := unix.Sendfile(outFd, inFd, &offset, int(size-offset))
			if err != nil {
				sendErr = err
				return
			}
			if n == 0 {
				break
			}
		}
	}); err != nil {
		util.Error("rawConn.Control failed: %v", err)
		return 0, fmt.Errorf("rawConn.Control: %w", err)
	}

	if sendErr != nil {
		return 0, sendErr
	}

	msgCount, err := d.countMessagesInSegment()
	if err != nil {
		return 0, fmt.Errorf("failed to count messages: %w", err)
	}

	return msgCount, nil
}
