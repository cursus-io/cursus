package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cursus-io/cursus/test/consumer/config"
	"github.com/cursus-io/cursus/test/consumer/subscriber"
	"github.com/cursus-io/cursus/util"
)

func main() {
	cfg, err := config.LoadConsumerConfig("/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	c, err := subscriber.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		if err := c.Start(); err != nil {
			errCh <- err
		}
	}()

	select {
	case sig := <-sigCh:
		util.Info("Received signal: %v, shutting down...", sig)
	case err := <-errCh:
		util.Error("Consumer error: %v", err)
	case <-c.Done():
		util.Info("Benchmark completed successfully.")
	}
	if err := c.Close(); err != nil {
		util.Error("❌ Error closing consumer: %v", err)
	}

	util.Info("✅ Consumer closed gracefully")

	if cfg.EnableBenchmark {
		c.GetMetrics().PrintSummary()
	}
	os.Exit(0)
}
