package recovery

import (
	"context"
	"net"
	"sync/atomic"
)

type meterKey struct{}
type Meter struct{ bytes atomic.Int64 }

func Measure(ctx context.Context) (context.Context, *Meter) {
	m := &Meter{}
	return context.WithValue(ctx, meterKey{}, m), m
}
func RecordTransfer(ctx context.Context, bytes int) {
	if m, ok := ctx.Value(meterKey{}).(*Meter); ok && bytes > 0 {
		m.bytes.Add(int64(bytes))
	}
}
func (m *Meter) Bytes() int64 { return m.bytes.Load() }

// MeterConnection counts actual socket reads, including partial failed frames.
// It leaves normal traffic alone when no recovery measurement is attached.
func MeterConnection(ctx context.Context, conn net.Conn) net.Conn {
	if _, ok := ctx.Value(meterKey{}).(*Meter); !ok {
		return conn
	}
	return &meteredConnection{Conn: conn, ctx: ctx}
}

type meteredConnection struct {
	net.Conn
	ctx context.Context
}

func (c *meteredConnection) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	RecordTransfer(c.ctx, n)
	return n, err
}
