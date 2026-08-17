package main

import (
	"context"
	"fmt"
	"log"

	sdk "github.com/cursus-io/cursus/sdk"
)

type game struct {
	id      string
	version uint64
	status  string
}

func newGame(id string) *game { return &game{id: id} }

func (g *game) ID() string      { return g.id }
func (g *game) Type() string    { return "game" }
func (g *game) Version() uint64 { return g.version }

func (g *game) Apply(event sdk.EventEnvelope) error {
	switch event.EventType {
	case "GameCreated":
		g.status = "created"
	case "GameFinished":
		g.status = "finished"
	default:
		return fmt.Errorf("unknown event type %q", event.EventType)
	}
	g.version = event.AggregateVersion
	return nil
}

func main() {
	ctx := context.Background()
	_ = ctx // Keep the example ready for command handlers that accept context.

	store := sdk.NewEventStore("localhost:9000", "framework-games", "framework-example")
	defer func() { _ = store.Close() }()
	if err := store.CreateTopic(1); err != nil {
		log.Fatal(err)
	}

	repository, err := sdk.NewAggregateRepository(store, func(id string) sdk.Aggregate {
		return newGame(id)
	})
	if err != nil {
		log.Fatal(err)
	}

	const gameID = "game-example-1"
	aggregate, err := repository.Load(gameID)
	if err != nil {
		log.Fatal(err)
	}

	created, err := sdk.NewEventEnvelope("game", gameID, "GameCreated", map[string]any{"mode": "ranked"})
	if err != nil {
		log.Fatal(err)
	}
	finished, err := sdk.NewEventEnvelope("game", gameID, "GameFinished", map[string]any{"winner": "player-1"})
	if err != nil {
		log.Fatal(err)
	}
	if err := repository.Save(aggregate, []sdk.EventEnvelope{created, finished}); err != nil {
		log.Fatal(err)
	}

	reloaded, err := repository.Load(gameID)
	if err != nil {
		log.Fatal(err)
	}
	state := reloaded.(*game)
	fmt.Printf("aggregate=%s version=%d status=%s\n", state.ID(), state.Version(), state.status)
}
