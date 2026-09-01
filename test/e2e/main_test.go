package e2e

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()

	fmt.Println("All tests finished. Cleaning up docker compose environment...")
	cmd := RunCompose("-f", composeFile, "down", "-v")

	if err := cmd.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Warning: docker compose down failed: %v\n", err)
	}

	os.Exit(code)
}
