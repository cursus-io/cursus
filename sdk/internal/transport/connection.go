package transport

import (
	"net"

	"github.com/cursus-io/cursus/pkg/wire"
)

type Conn = wire.ClientConn

func NewClient(conn net.Conn, compression string) (*Conn, error) {
	return wire.NewClientConn(conn, compression)
}
