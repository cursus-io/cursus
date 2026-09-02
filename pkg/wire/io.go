package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

func WriteLengthPrefixed(writer io.Writer, payload []byte) error {
	if payload == nil {
		return fmt.Errorf("payload must not be nil")
	}
	if len(payload) > MaxFramePayload {
		return fmt.Errorf("payload size %d exceeds maximum %d", len(payload), MaxFramePayload)
	}
	header := make([]byte, 4)
	// #nosec G115 -- payload length is checked against MaxFramePayload above.
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if err := writeAll(writer, header); err != nil {
		return fmt.Errorf("write payload length: %w", err)
	}
	if err := writeAll(writer, payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

func ReadLengthPrefixed(reader io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, fmt.Errorf("read payload length: %w", err)
	}
	length := binary.BigEndian.Uint32(header)
	if length > MaxFramePayload {
		return nil, fmt.Errorf("payload size %d exceeds maximum %d", length, MaxFramePayload)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	return payload, nil
}
