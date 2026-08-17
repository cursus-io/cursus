package main

import (
	"fmt"
	"time"

	sdk "github.com/cursus-io/cursus/sdk"
)

func main() {
	registry := sdk.NewUpcasterRegistry()
	if err := registry.Register("GameFinished", 1, func(event sdk.EventEnvelope) (sdk.EventEnvelope, error) {
		event.SchemaVersion = 2
		return event, nil
	}); err != nil {
		panic(err)
	}

	deadlines := sdk.NewDeadlineManager()
	if err := deadlines.Schedule("saga-1:timeout", time.Unix(100, 0), func() { fmt.Println("deadline fired: compensate") }); err != nil {
		panic(err)
	}
	deadlines.RunDue(time.Unix(100, 0))
	fmt.Println(sdk.FrameworkHelp())
}
