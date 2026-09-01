package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	SnapshotVersionCurrent     = 9
	CommittedHWMVersionCurrent = 1
)

var ErrUnsupportedRecoveryProtocol = errors.New("unsupported recovery protocol")

func decodeStrictJSON(data []byte, target interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
