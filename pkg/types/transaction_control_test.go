package types

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionMarkerControlWireVectors(t *testing.T) {
	for _, tc := range []struct {
		name, marker, key, value string
		epoch                    int64
	}{
		{"commit zero", TransactionMarkerCommit, "00000000", "000000000000", 0},
		{"abort positive", TransactionMarkerAbort, "00000001", "000000000007", 7},
		{"negative epoch", TransactionMarkerCommit, "00000000", "0000ffffffff", -1},
		{"minimum epoch", TransactionMarkerAbort, "00000001", "000080000000", -1 << 31},
		{"maximum epoch", TransactionMarkerCommit, "00000000", "00007fffffff", 1<<31 - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, value, err := TransactionMarkerControlBytes(tc.marker, tc.epoch)
			require.NoError(t, err)
			require.Equal(t, tc.key, hex.EncodeToString(key))
			require.Equal(t, tc.value, hex.EncodeToString(value))
		})
	}
}

func TestTransactionMarkerControlRejectsUnrepresentableAuthority(t *testing.T) {
	for _, tc := range []struct {
		marker string
		epoch  int64
	}{
		{TransactionMarkerCommit, -1<<31 - 1},
		{TransactionMarkerAbort, 1 << 31},
		{"", 0},
		{"prepare", 1},
	} {
		key, value, err := TransactionMarkerControlBytes(tc.marker, tc.epoch)
		require.Error(t, err)
		require.Nil(t, key)
		require.Nil(t, value)
	}
}
