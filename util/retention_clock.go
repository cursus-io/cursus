package util

import "time"

// RetentionClockSafe compares wall time with monotonic elapsed process time.
// It detects steps within this process, not an incorrect clock at startup.
func RetentionClockSafe(anchor, observed time.Time, elapsed time.Duration) bool {
	if anchor.IsZero() {
		return true
	}
	projected := anchor.Add(elapsed).UnixMilli()
	actual := observed.UnixMilli()
	return actual > 0 && actual >= projected-60000 && actual <= projected+60000
}
