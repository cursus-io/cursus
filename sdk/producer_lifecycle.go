package sdk

import "fmt"

// ProducerState describes whether a producer still accepts messages, is
// draining accepted messages, or has completed all shutdown cleanup.
type ProducerState uint32

const (
	ProducerStateOpen ProducerState = iota
	ProducerStateClosing
	ProducerStateClosed
)

func (s ProducerState) String() string {
	switch s {
	case ProducerStateOpen:
		return "open"
	case ProducerStateClosing:
		return "closing"
	case ProducerStateClosed:
		return "closed"
	default:
		return fmt.Sprintf("producer_state(%d)", s)
	}
}

// State returns the producer's current delivery lifecycle state.
func (p *Producer) State() ProducerState {
	if p == nil {
		return ProducerStateClosed
	}
	return ProducerState(p.state.Load())
}
