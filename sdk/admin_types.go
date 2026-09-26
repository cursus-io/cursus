//go:build legacy_sql_saga

package sdk

import "time"

// AdminConfig configures read-only management queries. It deliberately has no
// consumer group fields: AdminClient never joins a group or commits offsets.
type AdminConfig struct {
	BrokerAddrs                  []string
	UseTLS                       bool
	TLSCertPath                  string
	TLSKeyPath                   string
	Principal                    string
	AuthToken                    string
	ProtocolVersion              int
	ProtocolFeatures             []string
	RequireProtocolFeatures      bool
	ProtocolNegotiationTimeoutMS int
	RequestTimeout               time.Duration
}

type BrowseRequest struct {
	Topic      string
	Partition  int
	FromOffset uint64
	ToOffset   *uint64 // exclusive fixed page boundary
	MaxRecords int
	MaxBytes   int
}

type BrowseResult struct {
	Messages          []AdminMessage
	NextOffset        uint64
	EarliestOffset    uint64
	ReadableEndOffset uint64
	HasMore           bool
}

type HistoryRequest struct {
	Topic       string
	Key         string
	FromVersion uint64
	ToVersion   *uint64 // inclusive fixed page boundary
	MaxRecords  int
	MaxBytes    int
}

type HistoryCompleteness string

const (
	HistoryComplete HistoryCompleteness = "complete"
	HistoryPartial  HistoryCompleteness = "partial"
	HistoryUnknown  HistoryCompleteness = "unknown"
)

type HistoryResult struct {
	Events       []StreamEvent
	NextVersion  uint64
	HeadVersion  uint64
	Completeness HistoryCompleteness
	HasMore      bool
}

type GroupOffset struct {
	Partition int
	Offset    uint64
	Earliest  uint64
	Latest    uint64
	Lag       uint64
}

// AdminMessage is the public read-only representation of a broker record.
type AdminMessage struct {
	Offset           uint64
	Key              string
	Payload          string
	Metadata         string
	EventType        string
	SchemaVersion    uint32
	AggregateVersion uint64
}
