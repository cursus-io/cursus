package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestElection_Basic(t *testing.T) {
	// Use unbuffered channel for deterministic synchronization
	ch := make(chan bool)
	rm := &MockRaftManager{leaderCh: ch}
	election := NewControllerElection(rm)
	assert.NotNil(t, election)

	election.Start()

	// These will block until the monitor goroutine reads them, ensuring synchronization without sleep
	ch <- true
	ch <- false

	election.Stop()
	// Optionally wait a tiny bit for the goroutine to exit cleanly, though not strictly required
	// if we don't assert anything afterwards. However, to wait for termination we could use a WaitGroup,
	// but context cancellation is immediate enough.
}
