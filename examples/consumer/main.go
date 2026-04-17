package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cursus-io/cursus/sdk"
)

func main() {
	cfg := sdk.NewDefaultConsumerConfig()
	// Try loading from current dir, then parent dir
	if err := sdk.LoadConfig("config.yaml", cfg); err != nil {
		if err := sdk.LoadConfig("../config.yaml", cfg); err != nil {
			log.Printf("Config file not found or invalid, using defaults: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := sdk.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	go func() {
		<-ctx.Done()
		log.Println("Shutting down consumer...")
		if err := c.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}()

	log.Printf("Starting consumer for topic: %s", cfg.Topic)
	err = c.Start(func(msg sdk.Message) error {
		log.Printf("Received message: Offset=%d, SeqNum=%d, Payload=%s", msg.Offset, msg.SeqNum, msg.Payload)
		return nil
	})

	if err != nil && err != context.Canceled {
		log.Fatalf("Consumer error: %v", err)
	}
}
