package util

import (
	"net"

	"github.com/cursus-io/cursus/pkg/wire"
)

const MaxMessageSize = wire.MaxFramePayload

type payloadWriter interface {
	WritePayload([]byte) error
}

func WriteWithLength(conn net.Conn, data []byte) error {
	if writer, ok := conn.(payloadWriter); ok {
		return writer.WritePayload(data)
	}
	return wire.WriteLengthPrefixed(conn, data)
}

func ReadWithLength(conn net.Conn) ([]byte, error) {
	return wire.ReadLengthPrefixed(conn)
}
