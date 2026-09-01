package util

import (
	"net"

	"github.com/cursus-io/cursus/pkg/wire"
)

const MaxMessageSize = wire.MaxFramePayload

func WriteWithLength(conn net.Conn, data []byte) error {
	return wire.WriteLengthPrefixed(conn, data)
}

func ReadWithLength(conn net.Conn) ([]byte, error) {
	return wire.ReadLengthPrefixed(conn)
}
