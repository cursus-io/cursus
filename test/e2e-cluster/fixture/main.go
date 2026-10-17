//go:build e2e_faults

// This executable exists only in the isolated fault-test image. It appends an
// uncommitted epoch-zero tail to one stopped fixture broker, never a live broker.
package main

import (
	"fmt"
	"os"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
)

func main() {
	if err := appendFixtureTail(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func appendFixtureTail() error {
	if os.Getenv("CURSUS_E2E_FAULTS") != "1" {
		return fmt.Errorf("fixture requires the isolated fault runtime")
	}
	cfg := config.DefaultConfig()
	cfg.LogDir = "/app/cluster-logs"
	dm := disk.NewDiskManager(cfg)
	defer dm.CloseAllHandlers()
	storage, err := dm.GetHandler("indexed-tail-history", 0)
	if err != nil {
		return err
	}
	d := storage.(*disk.DiskHandler)
	start := uint64(1)
	if os.Getenv("CURSUS_E2E_TAIL_START") == "10001" {
		start = 10001
	}
	if d.GetAbsoluteOffset() != start || d.GetFirstOffset() != 0 {
		return fmt.Errorf("fixture requires the expected retained committed seeds")
	}
	messages := make([]types.DiskMessage, 10000)
	for i := range messages {
		messages[i] = types.DiskMessage{Topic: "indexed-tail-history", Offset: start + uint64(i), Payload: "uncommitted-legacy-divergence", LeaderEpoch: 0}
	}
	if err := d.WriteBatchSync(messages); err != nil {
		return err
	}
	if d.GetAbsoluteOffset() != start+10000 {
		return fmt.Errorf("fixture did not persist the complete tail")
	}
	return nil
}
