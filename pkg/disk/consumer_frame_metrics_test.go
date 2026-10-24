package disk

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

func TestConsumerFrameWorkCountsRejectedAndRepeatedReads(t *testing.T) {
	var data []byte
	for offset := uint64(0); offset < 2; offset++ {
		body, err := util.SerializeDiskMessage(types.DiskMessage{Offset: offset, Payload: "value"})
		require.NoError(t, err)
		data = binary.BigEndian.AppendUint32(data, uint32(len(body))) // #nosec G115 -- fixed tiny fixture.
		data = append(data, body...)
	}
	data = append(data, 0, 0, 0, 0) // Invalid length after two decoded frames.
	path := filepath.Join(t.TempDir(), "frames")
	require.NoError(t, os.WriteFile(path, data, 0600))
	r, err := mmap.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	value := func(unit string) float64 {
		families, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, family := range families {
			if family.GetName() != "cursus_recovery_work_total" {
				continue
			}
			for _, metric := range family.Metric {
				labels := map[string]string{}
				for _, label := range metric.Label {
					labels[label.GetName()] = label.GetValue()
				}
				if labels["stage"] == "consumer_frames" && labels["unit"] == unit {
					return metric.Counter.GetValue()
				}
			}
		}
		return 0
	}
	bytesBefore, framesBefore := value("read_bytes"), value("decoded_frames")
	d := &DiskHandler{internalMetadata: true}
	for range 2 {
		_, _, _, err := d.readMessagesFromPositionBounded(r, 0, 10, 0, true, 1, 0, false, false)
		require.ErrorContains(t, err, "corrupt message length")
	}
	require.Equal(t, float64(2*len(data)), value("read_bytes")-bytesBefore)
	require.Equal(t, float64(4), value("decoded_frames")-framesBefore, "includes decoded records preceding the requested offset")
	_, _, _, err = d.readMessagesFromPositionBounded(r, 0, 10, 1, false, 0, 0, false, false)
	require.NoError(t, err)
	require.Equal(t, float64(2*len(data)+4), value("read_bytes")-bytesBefore, "budget rejection still read a length header")
	d.internalMetadata = false
	_, _, _, _ = d.readMessagesFromPositionBounded(r, 0, 10, 0, true, 0, 0, false, false)
	require.Equal(t, float64(2*len(data)+4), value("read_bytes")-bytesBefore, "ordinary topics are outside this counter")
}
