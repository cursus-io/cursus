package main

import (
	"log"

	"github.com/cursus-io/cursus/sdk"
)

func main() {
	cfg := sdk.NewDefaultConsumerConfig()
	if err := sdk.LoadConfig("config.yaml", cfg); err != nil {
		log.Printf("Config file not found or invalid, using defaults: %v", err)
	}

	c, err := sdk.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	log.Printf("Starting consumer for topic: %s", cfg.Topic)
	err = c.Start(func(msg sdk.Message) error {
		log.Printf("Received message: Offset=%d, SeqNum=%d, Payload=%s", msg.Offset, msg.SeqNum, msg.Payload)
		return nil
	})

	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}
