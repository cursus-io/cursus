// order-saga-mysql demonstrates the same service-owned v1 history boundary
// as the PostgreSQL example, using MySQL 8+.
//go:build legacy_sql_saga

package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/sdk/sagamysql"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	ctx := context.Background()
	dsn := required("CURSUS_ORDER_SAGA_MYSQL_DSN")
	historyTopic := required("CURSUS_ORDER_SAGA_HISTORY_TOPIC")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}
	if err := sagamysql.Migrate(ctx, db); err != nil {
		log.Fatal(err)
	}
	manager, err := sdk.NewTransactionalSagaManager(sdk.SagaDefinition{
		Type: "order-fulfillment",
		Handlers: map[string]sdk.SagaHandler{
			"OrderCreated": func(_ context.Context, state *sdk.SagaState, event sdk.EventEnvelope) ([]sdk.Command, error) {
				state.Status, state.Step = sdk.SagaWaiting, "reserve-inventory"
				return []sdk.Command{{EffectID: "reserve:" + state.ID, Type: "ReserveInventory", Payload: string(event.Payload)}}, nil
			},
		},
	}, sagamysql.Transaction{DB: db, HistoryTopic: historyTopic}, sdk.SagaHistoryOptions{EnvironmentID: value("CURSUS_ENVIRONMENT", "development"), ServiceName: "orders"})
	if err != nil {
		log.Fatal(err)
	}
	event, err := sdk.NewEventEnvelope("order", "order-42", "OrderCreated", map[string]string{"order_id": "order-42"})
	if err != nil {
		log.Fatal(err)
	}
	event.AssociationKey, event.CorrelationID = "order-42", "order-42"
	if err := manager.Handle(ctx, event); err != nil {
		log.Fatal(err)
	}
}

func required(key string) string {
	if value := os.Getenv(key); value != "" {
		return value
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
