package util

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDurableAppendTimeCodec(t *testing.T) {
	for _, stamp := range []int64{0, 1700000000123, 1<<63 - 1} {
		m := types.DiskMessage{Topic: "orders", Payload: "same", LeaderEpoch: 4, AppendTimeMS: stamp}
		encoded, err := SerializeDiskMessage(m)
		require.NoError(t, err)
		require.Len(t, encoded, EstimateDiskMessageSize(m))
		magic := "CDM5"
		if stamp != 0 {
			magic = "CDM6"
		}
		require.Equal(t, magic, string(encoded[:4]))
		decoded, err := DeserializeDiskMessage(encoded)
		require.NoError(t, err)
		require.Equal(t, m, decoded)
		if stamp == 0 {
			continue
		}
		encoded[len(encoded)-5] ^= 1
		_, err = DeserializeDiskMessage(encoded)
		require.ErrorIs(t, err, ErrDiskMessageChecksumMismatch)
		binary.BigEndian.PutUint64(encoded[len(encoded)-12:], 0)
		binary.BigEndian.PutUint32(encoded[len(encoded)-4:], crc32.Checksum(encoded[:len(encoded)-4], diskMessageChecksumTable))
		_, err = DeserializeDiskMessage(encoded)
		require.ErrorContains(t, err, "invalid append time")
	}
	_, err := SerializeDiskMessage(types.DiskMessage{AppendTimeMS: -1})
	require.ErrorContains(t, err, "negative append time")
}
