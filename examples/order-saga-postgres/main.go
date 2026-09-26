// order-saga-postgres demonstrates the service-owned side of the B-stage
// contract. It writes Saga state, inbox, command outbox, immutable history,
// and history outbox in one PostgreSQL transaction, then publishes history.
//go:build legacy_sql_saga

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/sdk/sagapg"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx := context.Background()
	databaseURL := required("CURSUS_ORDER_SAGA_DATABASE_URL")
	brokerAddress := required("CURSUS_ORDER_SAGA_BROKER")
	historyTopic := value("CURSUS_ORDER_SAGA_HISTORY_TOPIC", sagapg.DefaultHistoryTopic)
	sourceTopic := value("CURSUS_ORDER_SAGA_SOURCE_TOPIC", "orders.events")

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}
	if err := sagapg.Migrate(ctx, db); err != nil {
		log.Fatal(err)
	}

	manager, err := sdk.NewTransactionalSagaManager(sdk.SagaDefinition{
		Type: "order-fulfillment",
		Handlers: map[string]sdk.SagaHandler{
			"OrderCreated": func(_ context.Context, saga *sdk.SagaState, event sdk.EventEnvelope) ([]sdk.Command, error) {
				saga.Step, saga.Status = "reserve-inventory", sdk.SagaWaiting
				return []sdk.Command{{EffectID: "reserve:" + saga.ID, Type: "ReserveInventory", Payload: string(event.Payload)}}, nil
			},
			"InventoryRetryRequested": func(_ context.Context, saga *sdk.SagaState, event sdk.EventEnvelope) ([]sdk.Command, error) {
				saga.Step, saga.Status = "reserve-inventory", sdk.SagaWaiting
				return []sdk.Command{{EffectID: fmt.Sprintf("reserve:%s:retry-%d", saga.ID, saga.RetryCount+1), Type: "ReserveInventory", Payload: string(event.Payload)}}, nil
			},
			"InventoryReserved": func(_ context.Context, saga *sdk.SagaState, _ sdk.EventEnvelope) ([]sdk.Command, error) {
				saga.Step, saga.Status = "complete-order", sdk.SagaCompleted
				return nil, nil
			},
		},
	}, sagapg.Transaction{DB: db, HistoryTopic: historyTopic}, sdk.SagaHistoryOptions{EnvironmentID: value("CURSUS_ORDER_SAGA_ENVIRONMENT", "compose"), ServiceName: "orders"})
	if err != nil {
		log.Fatal(err)
	}

	// One successful run, one explicit retry run, and one compensated run make
	// the resulting UI timeline useful for a real smoke test.
	if err := handle(manager, sourceTopic, "order-success", "OrderCreated"); err != nil {
		log.Fatal(err)
	}
	if err := manager.RecordEffectResult(ctx, "order-success", "reserve:order-success", true, nil); err != nil {
		log.Fatal(err)
	}
	if err := handle(manager, sourceTopic, "order-success", "InventoryReserved"); err != nil {
		log.Fatal(err)
	}
	if err := handle(manager, sourceTopic, "order-retry", "OrderCreated"); err != nil {
		log.Fatal(err)
	}
	if err := manager.RecordEffectResult(ctx, "order-retry", "reserve:order-retry", false, fmt.Errorf("inventory temporarily unavailable")); err != nil {
		log.Fatal(err)
	}
	if err := handle(manager, sourceTopic, "order-retry", "InventoryRetryRequested"); err != nil {
		log.Fatal(err)
	}
	if err := manager.RecordEffectResult(ctx, "order-retry", "reserve:order-retry:retry-1", true, nil); err != nil {
		log.Fatal(err)
	}
	if err := handle(manager, sourceTopic, "order-retry", "InventoryReserved"); err != nil {
		log.Fatal(err)
	}
	if err := handle(manager, sourceTopic, "order-compensated", "OrderCreated"); err != nil {
		log.Fatal(err)
	}
	if err := manager.RecordEffectResult(ctx, "order-compensated", "reserve:order-compensated", false, fmt.Errorf("payment declined after inventory reservation")); err != nil {
		log.Fatal(err)
	}
	if _, err := manager.StartCompensation(ctx, "order-compensated", "release-inventory", fmt.Errorf("payment declined")); err != nil {
		log.Fatal(err)
	}
	if err := manager.CompleteCompensation(ctx, "order-compensated"); err != nil {
		log.Fatal(err)
	}

	publisherConfig := sdk.NewDefaultPublisherConfig()
	publisherConfig.BrokerAddrs = []string{brokerAddress}
	publisherConfig.Topic, publisherConfig.AutoCreateTopics = historyTopic, true
	// A broker behind Docker port mapping may advertise localhost as its
	// leader. Publish through the explicitly configured bootstrap endpoint.
	publisherConfig.UseBootstrapAddressForAdvertisedLoopback = true
	producer, err := sdk.NewProducer(publisherConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	published, err := (sagapg.HistoryOutboxPublisher{DB: db, Publisher: sagapg.SDKHistoryPublisher{Producer: producer}}).PublishPending(ctx, 100)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("published %d Saga history records to %s", published, historyTopic)
}

func handle(manager *sdk.SagaManager, sourceTopic, orderID, eventType string) error {
	event, err := sdk.NewEventEnvelope("order", orderID, eventType, map[string]string{"order_id": orderID})
	if err != nil {
		return err
	}
	event.AssociationKey, event.CorrelationID = orderID, orderID
	event.AggregateVersion = 1
	event.SourceTopic, event.SourcePartition, event.SourceOffset = sourceTopic, 0, 1
	return manager.Handle(context.Background(), event)
}

func required(key string) string {
	if result := os.Getenv(key); result != "" {
		return result
	}
	log.Fatalf("%s is required", key)
	return ""
}
func value(key, fallback string) string {
	if result := os.Getenv(key); result != "" {
		return result
	}
	return fallback
}
