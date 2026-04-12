package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestElection_Basic(t *testing.T) {
	ch := make(chan bool, 1)
	rm := &MockRaftManager{leaderCh: ch}
	election := NewControllerElection(rm)
	assert.NotNil(t, election)

	election.Start()
	
	ch <- true
	// Give a small amount of time for the goroutine to process
	time.Sleep(10 * time.Millisecond)
	
	ch <- false
	time.Sleep(10 * time.Millisecond)
	
	election.Stop()
	time.Sleep(10 * time.Millisecond)
}
