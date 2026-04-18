package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

const brokerAddr = "localhost:9000"

func main() {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to broker at %s: %v\n", brokerAddr, err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	fmt.Println("=== Cursus Event Sourcing Example ===")
	fmt.Printf("Connected to broker at %s\n\n", brokerAddr)

	fmt.Println("--- Step 1: Create event-sourcing topic ---")
	resp := sendCommand(conn, "", "CREATE topic=orders partitions=4 event_sourcing=true")
	fmt.Printf("Response: %s\n\n", resp)

	fmt.Println("--- Step 2: Append first event (OrderCreated) ---")
	orderID := "order-12345"
	eventPayload := `{"order_id":"order-12345","customer":"alice","items":[{"sku":"WIDGET-1","qty":2}],"total":49.98}`

	cmd := fmt.Sprintf(
		"APPEND_STREAM topic=orders key=%s version=1 event_type=OrderCreated message=%s",
		orderID, eventPayload,
	)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Response: %s\n\n", resp)

	fmt.Println("--- Step 3: Append second event (ItemAdded) ---")

	eventPayload2 := `{"sku":"GADGET-7","qty":1,"price":19.99}`
	cmd = fmt.Sprintf(
		"APPEND_STREAM topic=orders key=%s version=2 event_type=ItemAdded message=%s",
		orderID, eventPayload2,
	)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Response: %s\n\n", resp)

	fmt.Println("--- Step 4: Append third event (OrderShipped) ---")
	eventPayload3 := `{"tracking_number":"TRK-98765","carrier":"FedEx"}`
	cmd = fmt.Sprintf(
		"APPEND_STREAM topic=orders key=%s version=3 event_type=OrderShipped message=%s",
		orderID, eventPayload3,
	)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Response: %s\n\n", resp)

	fmt.Println("--- Step 5: Check stream version ---")
	cmd = fmt.Sprintf("STREAM_VERSION topic=orders key=%s", orderID)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Current version of '%s': %s\n\n", orderID, resp)

	fmt.Println("--- Step 6: Demonstrate version conflict ---")
	fmt.Println("Attempting to append with stale version=2 (current is 3)...")

	conflictPayload := `{"note":"this should fail"}`
	cmd = fmt.Sprintf(
		"APPEND_STREAM topic=orders key=%s version=2 event_type=StaleEvent message=%s",
		orderID, conflictPayload,
	)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Response (expected error): %s\n\n", resp)

	fmt.Println("--- Step 7: Save a snapshot at version 3 ---")
	snapshotPayload := `{"order_id":"order-12345","customer":"alice","items":[{"sku":"WIDGET-1","qty":2},{"sku":"GADGET-7","qty":1}],"total":69.97,"status":"shipped","tracking":"TRK-98765"}`
	cmd = fmt.Sprintf(
		"SAVE_SNAPSHOT topic=orders key=%s version=3 message=%s",
		orderID, snapshotPayload,
	)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Response: %s\n\n", resp)

	fmt.Println("--- Step 8: Read snapshot ---")
	cmd = fmt.Sprintf("READ_SNAPSHOT topic=orders key=%s", orderID)
	resp = sendCommand(conn, "", cmd)
	prettyPrintJSON("Snapshot", resp)
	fmt.Println()

	fmt.Println("--- Step 9: Append events after snapshot ---")
	eventPayload4 := `{"refund_amount":19.99,"reason":"defective item"}`
	cmd = fmt.Sprintf(
		"APPEND_STREAM topic=orders key=%s version=4 event_type=PartialRefund message=%s",
		orderID, eventPayload4,
	)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Response: %s\n\n", resp)

	eventPayload5 := `{"new_total":49.98}`
	cmd = fmt.Sprintf(
		"APPEND_STREAM topic=orders key=%s version=5 event_type=TotalRecalculated message=%s",
		orderID, eventPayload5,
	)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Response: %s\n\n", resp)

	fmt.Println("--- Step 10: Verify final stream version ---")
	cmd = fmt.Sprintf("STREAM_VERSION topic=orders key=%s", orderID)
	resp = sendCommand(conn, "", cmd)
	fmt.Printf("Final version of '%s': %s\n\n", orderID, resp)

	fmt.Println("=== Example Complete ===")
}

// sendCommand sends a text command to the broker using length-prefixed framing
// with the topic+payload envelope, then reads and returns the response.
//
// Framing: [4-byte BE length][body]
// Body (topic envelope): [topicLen:2][topic][payload]
//
// If topic is empty, topicLen is 0 and the body is just [0x00 0x00][payload].
func sendCommand(conn net.Conn, topicName, command string) string {
	// [topicLen:2][topic][command]
	topicBytes := []byte(topicName)
	cmdBytes := []byte(command)
	bodyLen := 2 + len(topicBytes) + len(cmdBytes)

	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(topicBytes)))
	copy(body[2:], topicBytes)
	copy(body[2+len(topicBytes):], cmdBytes)

	// Write length prefix + body.
	frame := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(frame[0:4], uint32(bodyLen))
	copy(frame[4:], body)

	if _, err := conn.Write(frame); err != nil {
		log.Fatalf("Failed to send command: %v", err)
	}

	// Read response: [4-byte BE length][response body]
	var respLen uint32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		log.Fatalf("Failed to read response length: %v", err)
	}

	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		log.Fatalf("Failed to read response body: %v", err)
	}

	return string(respBody)
}

// prettyPrintJSON attempts to format a JSON string with indentation.
// If the input is not valid JSON, it prints the raw string.
func prettyPrintJSON(label, s string) {
	s = strings.TrimSpace(s)
	var obj interface{}
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		fmt.Printf("%s: %s\n", label, s)
		return
	}
	pretty, err := json.MarshalIndent(obj, "  ", "  ")
	if err != nil {
		fmt.Printf("%s: %s\n", label, s)
		return
	}
	fmt.Printf("%s:\n  %s\n", label, string(pretty))
}
