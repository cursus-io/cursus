package wire_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestFrameGoldenUncompressedRequest(t *testing.T) {
	codec, err := wire.NewCodec(wire.CompressionNone)
	require.NoError(t, err)
	encoded, err := codec.Encode(wire.Frame{
		Kind: wire.KindRequest, Command: wire.CommandPublish, RequestID: 42, Payload: []byte("hello"),
	})
	require.NoError(t, err)
	require.Equal(t,
		"4352533200020301000a0000000000000000002a00000005000000059a71bb4c68656c6c6f",
		hex.EncodeToString(encoded),
	)
	decoded, err := codec.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, uint16(2), decoded.Version)
	require.Equal(t, uint64(42), decoded.RequestID)
	require.Equal(t, []byte("hello"), decoded.Payload)
}

func TestFrameCompressionRoundTripAndNegotiationBoundary(t *testing.T) {
	for _, compression := range []wire.Compression{
		wire.CompressionNone, wire.CompressionGZIP, wire.CompressionSnappy, wire.CompressionLZ4,
	} {
		t.Run(compression.String(), func(t *testing.T) {
			codec, err := wire.NewCodec(compression)
			require.NoError(t, err)
			payload := bytes.Repeat([]byte("wire-v2-"), 100)
			encoded, err := codec.Encode(wire.Frame{
				Kind: wire.KindResponse, Command: wire.CommandConsume, Status: wire.StatusOK,
				RequestID: 91, Payload: payload,
			})
			require.NoError(t, err)
			decoded, err := codec.Decode(encoded)
			require.NoError(t, err)
			require.Equal(t, payload, decoded.Payload)

			negotiation, err := codec.Encode(wire.Frame{
				Kind: wire.KindNegotiationRequest, Command: wire.CommandNegotiate,
				Payload: []byte{0, 2},
			})
			require.NoError(t, err)
			require.Zero(t, negotiation[7], "negotiation must never carry compression flags")
		})
	}
}

func TestFrameRejectsChecksumCompressionAndLengthViolations(t *testing.T) {
	gzipCodec, err := wire.NewCodec(wire.CompressionGZIP)
	require.NoError(t, err)
	encoded, err := gzipCodec.Encode(wire.Frame{
		Kind: wire.KindResponse, Command: wire.CommandList, Status: wire.StatusOK,
		RequestID: 1, Payload: []byte("payload"),
	})
	require.NoError(t, err)

	corrupted := append([]byte(nil), encoded...)
	corrupted[len(corrupted)-1] ^= 0xff
	_, err = gzipCodec.Decode(corrupted)
	require.ErrorIs(t, err, wire.ErrChecksumMismatch)

	noneCodec, err := wire.NewCodec(wire.CompressionNone)
	require.NoError(t, err)
	_, err = noneCodec.Decode(encoded)
	require.ErrorIs(t, err, wire.ErrCompressionMismatch)

	oversized := append([]byte(nil), encoded[:wire.HeaderSize]...)
	binary.BigEndian.PutUint32(oversized[20:24], wire.MaxFramePayload+1)
	_, err = gzipCodec.ReadFrame(bytes.NewReader(oversized))
	require.ErrorIs(t, err, wire.ErrFrameTooLarge)
}

func TestFrameReadWriteHandlesShortWriters(t *testing.T) {
	codec, err := wire.NewCodec(wire.CompressionNone)
	require.NoError(t, err)
	buffer := new(bytes.Buffer)
	writer := &shortWriter{writer: buffer, limit: 3}
	require.NoError(t, codec.WriteFrame(writer, wire.Frame{
		Kind: wire.KindRequest, Command: wire.CommandHelp, RequestID: 7,
	}))
	decoded, err := codec.ReadFrame(buffer)
	require.NoError(t, err)
	require.Equal(t, wire.CommandHelp, decoded.Command)
}

func TestFrameRejectsMissingExplicitCompression(t *testing.T) {
	codec, err := wire.NewCodec(wire.CompressionNone)
	require.NoError(t, err)
	encoded, err := codec.Encode(wire.Frame{Kind: wire.KindRequest, Command: wire.CommandList, RequestID: 1})
	require.NoError(t, err)
	encoded[7] = 0
	_, err = codec.Decode(encoded)
	require.True(t, errors.Is(err, wire.ErrCompressionMismatch))
}

type shortWriter struct {
	writer *bytes.Buffer
	limit  int
}

func (w *shortWriter) Write(payload []byte) (int, error) {
	if len(payload) > w.limit {
		payload = payload[:w.limit]
	}
	return w.writer.Write(payload)
}

func FuzzCodecDecode(f *testing.F) {
	codec, _ := wire.NewCodec(wire.CompressionNone)
	f.Add([]byte("not-a-frame"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = codec.Decode(data)
	})
}
