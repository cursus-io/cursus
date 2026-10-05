package e2e

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if code != 0 {
		fmt.Println("E2E tests failed. Capturing broker logs before teardown...")
		logs := RunCompose("-f", composeFile, "logs", "--no-color", "broker")
		if output, err := logs.CombinedOutput(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Warning: docker compose logs failed: %v\n%s\n", err, output)
		} else {
			fmt.Print(string(output))
		}
	}

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
