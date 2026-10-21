package types

// ReplayCursor is a derived physical position, valid only for the partition
// identity/generation and retention boundary pinned by the caller.
type ReplayCursor struct {
	Initialized bool
	Segment     uint64
	Position    uint64
	LastOffset  uint64
	HasLast     bool
}

// ReplayPage reports all decoded record work, including records before the
// requested offset or beyond the captured committed end.
type ReplayPage struct {
	Messages []Message
	Next     uint64
	Cursor   ReplayCursor
	Records  int
	Bytes    int
}

// BoundedReplayReader reads at most one physical segment per call without an
// offset-index lookup. Empty compacted segments still advance Cursor.
type BoundedReplayReader interface {
	ReadReplayPage(offset, end uint64, cursor ReplayCursor, maxRecords, maxBytes int) (ReplayPage, error)
}
