package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := prometheus.NewRegistry()
	applicationBatches := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "commerce_consumer_batches_total",
		Help: "Application batches durably processed before offset commit.",
	})
	registry.MustRegister(applicationBatches)

	metrics, err := sdk.NewMetrics(registry, registry)
	if err != nil {
		log.Fatal(err)
	}

	base := sdk.NewDefaultConsumerConfig()
	base.Topic = "commerce-events"
	base.GroupID = "commerce-projection"
	base.EnableAutoCommit = false
	base.RequireExistingCatalog = true
	base.Metrics = metrics

	var active atomic.Pointer[sdk.Consumer]
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		consumer := active.Load()
		if consumer == nil {
			http.Error(w, "consumer membership is not ready", http.StatusServiceUnavailable)
			return
		}
		membership := consumer.Membership()
		if !membership.Ready {
			http.Error(w, "consumer membership is not ready", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(membership)
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf("HTTP server stopped: %v", serveErr)
			stop()
		}
	}()

	if err := superviseConsumer(ctx, base, &active, applicationBatches); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("consumer supervisor stopped: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown: %v", err)
	}
}

func superviseConsumer(
	ctx context.Context,
	base *sdk.ConsumerConfig,
	active *atomic.Pointer[sdk.Consumer],
	applicationBatches prometheus.Counter,
) error {
	for attempt := 0; ; attempt++ {
		cfg := *base
		consumer, err := sdk.NewConsumerWithContext(ctx, &cfg)
		if err == nil {
			active.Store(consumer)
			err = consumer.StartBatch(func(batchCtx context.Context, batch sdk.ConsumerBatch) error {
				// Keep the DB transaction and optional dead-letter write in the
				// application. Commit only after that durable work succeeds.
				if dbErr := processInDatabase(batchCtx, batch.Messages); dbErr != nil {
					return dbErr
				}
				applicationBatches.Inc()
				return consumer.CommitBatch(batchCtx, batch)
			})
			active.CompareAndSwap(consumer, nil)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("consumer attempt %d stopped: %v", attempt+1, err)
		delay := time.Second << min(attempt, 5)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		// Consumer is one-shot: the next attempt always constructs a new one.
	}
}

func processInDatabase(context.Context, []sdk.Message) error {
	return nil
}
