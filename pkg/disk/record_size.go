package disk

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func validateDiskMessageSize(message types.DiskMessage) error {
	serialized, err := util.SerializeDiskMessage(message)
	if err != nil {
		return fmt.Errorf("serialize disk message for size validation: %w", err)
	}
	return validateSerializedDiskMessageSize(serialized)
}

func validateSerializedDiskMessageSize(serialized []byte) error {
	if len(serialized) == 0 {
		return fmt.Errorf("serialized disk message is empty")
	}
	if len(serialized) > MaxMessageSize {
		return fmt.Errorf("serialized disk message size %d exceeds maximum %d", len(serialized), MaxMessageSize)
	}
	return nil
}
