package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/cursus-io/cursus/sdk"
)


type OrderCreated struct {
	OrderID  string `json:"order_id"`
	Customer string `json:"customer"`
}

type ItemAdded struct {
	SKU   string  `json:"sku"`
	Name  string  `json:"name"`
	Qty   int     `json:"qty"`
	Price float64 `json:"price"`
}

type OrderShipped struct {
	Carrier  string `json:"carrier"`
	Tracking string `json:"tracking"`
}

type PartialRefund struct {
	SKU    string  `json:"sku"`
	Amount float64 `json:"amount"`
	Reason string  `json:"reason"`
}

// --- Order Aggregate (for snapshot) ---

type Order struct {
	ID       string            `json:"id"`
	Customer string            `json:"customer"`
	Items    map[string]Item   `json:"items"`
	Total    float64           `json:"total"`
	Status   string            `json:"status"`
	Tracking string            `json:"tracking,omitempty"`
}

type Item struct {
	Name  string  `json:"name"`
	Qty   int     `json:"qty"`
	Price float64 `json:"price"`
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func main() {
	addr := "localhost:9000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	store := sdk.NewEventStore(addr, "orders", "order-service")
	defer func() { _ = store.Close() }()

	fmt.Println("=== Order Aggregate — Event Sourcing Example ===")
	fmt.Println()

	// 1. Create topic
	if err := store.CreateTopic(4); err != nil {
		log.Fatalf("CreateTopic: %v", err)
	}
	fmt.Println("[Topic] 'orders' created (4 partitions, event_sourcing=true)")
	fmt.Println()

	orderID := "order-1001"

	// 2. Create order
	fmt.Println("[Command] CreateOrder")
	r, err := store.Append(orderID, 1, &sdk.Event{
		Type: "OrderCreated",
		Payload: toJSON(OrderCreated{
			OrderID:  orderID,
			Customer: "Alice Kim",
		}),
	})
	if err != nil {
		log.Fatalf("  failed: %v", err)
	}
	fmt.Printf("  -> version=%d offset=%d\n\n", r.Version, r.Offset)

	// 3. Add items
	fmt.Println("[Command] AddItem (Mechanical Keyboard)")
	r, err = store.Append(orderID, 2, &sdk.Event{
		Type: "ItemAdded",
		Payload: toJSON(ItemAdded{SKU: "KB-MX01", Name: "Mechanical Keyboard", Qty: 1, Price: 89.99}),
	})
	if err != nil {
		log.Fatalf("  failed: %v", err)
	}
	fmt.Printf("  -> version=%d\n", r.Version)

	fmt.Println("[Command] AddItem (USB-C Cable x3)")
	r, err = store.Append(orderID, 3, &sdk.Event{
		Type: "ItemAdded",
		Payload: toJSON(ItemAdded{SKU: "CBL-UC3", Name: "USB-C Cable", Qty: 3, Price: 9.99}),
	})
	if err != nil {
		log.Fatalf("  failed: %v", err)
	}
	fmt.Printf("  -> version=%d\n\n", r.Version)

	// 4. Check version
	ver, err := store.StreamVersion(orderID)
	if err != nil {
		log.Fatalf("StreamVersion failed: %v", err)
	}
	fmt.Printf("[Query] StreamVersion = %d\n\n", ver)

	// 5. Version conflict demo
	fmt.Println("[Command] AddItem with stale version (expecting conflict)")
	_, err = store.Append(orderID, 2, &sdk.Event{
		Type:    "ItemAdded",
		Payload: toJSON(ItemAdded{SKU: "STALE", Name: "Should Fail", Qty: 1, Price: 0}),
	})
	fmt.Printf("  -> expected error: %v\n\n", err)

	// 6. Ship order
	fmt.Println("[Command] ShipOrder")
	r, err = store.Append(orderID, 4, &sdk.Event{
		Type: "OrderShipped",
		Payload: toJSON(OrderShipped{Carrier: "FedEx", Tracking: "TRK-98765"}),
	})
	if err != nil {
		log.Fatalf("  failed: %v", err)
	}
	fmt.Printf("  -> version=%d\n\n", r.Version)

	// 7. Save snapshot (aggregate state at version 4)
	fmt.Println("[Snapshot] Saving aggregate state at version 4")
	snapshot := Order{
		ID:       orderID,
		Customer: "Alice Kim",
		Items: map[string]Item{
			"KB-MX01": {Name: "Mechanical Keyboard", Qty: 1, Price: 89.99},
			"CBL-UC3": {Name: "USB-C Cable", Qty: 3, Price: 9.99},
		},
		Total:    119.96,
		Status:   "shipped",
		Tracking: "TRK-98765",
	}
	if err := store.SaveSnapshot(orderID, 4, toJSON(snapshot)); err != nil {
		log.Fatalf("  failed: %v", err)
	}
	fmt.Println("  -> saved")
	fmt.Println()

	// 8. Read snapshot back
	fmt.Println("[Query] ReadSnapshot")
	snap, err := store.ReadSnapshot(orderID)
	if err != nil {
		log.Fatalf("ReadSnapshot failed: %v", err)
	}
	if snap != nil {
		var order Order
		_ = json.Unmarshal([]byte(snap.Payload), &order)
		fmt.Printf("  version  : %d\n", snap.Version)
		fmt.Printf("  customer : %s\n", order.Customer)
		fmt.Printf("  items    : %d\n", len(order.Items))
		fmt.Printf("  total    : $%.2f\n", order.Total)
		fmt.Printf("  status   : %s\n", order.Status)
	}
	fmt.Println()

	// 9. Append after snapshot
	fmt.Println("[Command] PartialRefund")
	r, err = store.Append(orderID, 5, &sdk.Event{
		Type: "PartialRefund",
		Payload: toJSON(PartialRefund{SKU: "CBL-UC3", Amount: 9.99, Reason: "defective"}),
	})
	if err != nil {
		log.Fatalf("  failed: %v", err)
	}
	fmt.Printf("  -> version=%d\n\n", r.Version)

	// 10. Final state
	finalVer, err := store.StreamVersion(orderID)
	if err != nil {
		log.Fatalf("StreamVersion failed: %v", err)
	}
	fmt.Printf("[Result] Order '%s' — %d events, snapshot at v4, current v%d\n", orderID, finalVer, finalVer)
	fmt.Println()
	fmt.Println("=== Example Complete ===")
}
