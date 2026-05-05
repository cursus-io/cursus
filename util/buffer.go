package util

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
)

const MaxMessageSize = 64 * 1024 * 1024 // 64MB

// WriteWithLength writes data with a 4-byte length prefix.
func WriteWithLength(conn net.Conn, data []byte) error {
	if len(data) > MaxMessageSize {
		return fmt.Errorf("data size %d exceeds maximum %d", len(data), MaxMessageSize)
	}
	if len(data) > math.MaxUint32 {
		return fmt.Errorf("data size %d exceeds uint32 max", len(data))
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	buf := append(lenBuf, data...)
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	return nil
}

// ReadWithLength reads data with a 4-byte length prefix.
func ReadWithLength(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)
	if length > MaxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds maximum %d", length, MaxMessageSize)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return buf, nil
}
