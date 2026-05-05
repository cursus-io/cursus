package disk

import (
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func newBenchDiskHandler(b *testing.B) (*DiskHandler, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "disk-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	cfg := &config.Config{
		LogDir:             dir,
		SegmentSize:        64 * 1024 * 1024,
		IndexSize:          10 * 1024 * 1024,
		IndexIntervalBytes: 4096,
		ChannelBufferSize:  10000,
		DiskFlushBatchSize: 500,
		LingerMS:           5,
		DiskWriteTimeoutMS: 5000,
		DiskFlushIntervalMS: 100,
	}
	dh, err := NewDiskHandler(cfg, "bench-topic", 0)
	if err != nil {
		_ = os.RemoveAll(dir)
		b.Fatal(err)
	}
	return dh, func() {
		_ = dh.Close()
		_ = os.RemoveAll(dir)
	}
}

func makeBenchMessages(n int) []types.DiskMessage {
	msgs := make([]types.DiskMessage, n)
	for i := range msgs {
		msgs[i] = types.DiskMessage{
			Topic:      "bench-topic",
			Partition:  0,
			Offset:     uint64(i),
			ProducerID: "producer-1",
			SeqNum:     uint64(i + 1),
			Epoch:      1,
			Payload:    "benchmark-payload-data-1234567890abcdefghijklmnopqrstuvwxyz",
		}
	}
	return msgs
}

func BenchmarkWriteBatch_100(b *testing.B) {
	dh, cleanup := newBenchDiskHandler(b)
	defer cleanup()
	msgs := makeBenchMessages(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range msgs {
			msgs[j].Offset = uint64(i*100 + j)
		}
		if err := dh.WriteBatch(msgs); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteBatch_500(b *testing.B) {
	dh, cleanup := newBenchDiskHandler(b)
	defer cleanup()
	msgs := makeBenchMessages(500)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range msgs {
			msgs[j].Offset = uint64(i*500 + j)
		}
		if err := dh.WriteBatch(msgs); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeDiskMessage(b *testing.B) {
	msg := types.DiskMessage{
		Topic:      "bench-topic",
		Partition:  0,
		Offset:     12345,
		ProducerID: "producer-1",
		SeqNum:     100,
		Epoch:      1,
		Payload:    "benchmark-payload-data-1234567890abcdefghijklmnopqrstuvwxyz",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := util.SerializeDiskMessage(msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadMessages(b *testing.B) {
	dh, cleanup := newBenchDiskHandler(b)
	defer cleanup()

	msgs := makeBenchMessages(1000)
	if err := dh.WriteBatch(msgs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := dh.ReadMessages(0, 100)
		if err != nil {
			b.Fatal(err)
		}
	}
}
