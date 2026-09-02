package e2e

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()

	fmt.Println("All tests finished. Cleaning up docker compose environment...")
	args := []string{"-f", composeFile, "down"}
	if os.Getenv("CURSUS_E2E_REMOVE_VOLUMES") == "1" {
		args = append(args, "-v")
	}
	cmd := RunCompose(args...)

	if err := cmd.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Warning: docker compose down failed: %v\n", err)
	}

	os.Exit(code)
}
