package util

import "math"

func SafeIntToUint8(v int) (uint8, bool) {
	if v < 0 || v > math.MaxUint8 {
		return 0, false
	}
	return uint8(v), true // #nosec G115
}

func SafeIntToUint16(v int) (uint16, bool) {
	if v < 0 || v > math.MaxUint16 {
		return 0, false
	}
	return uint16(v), true // #nosec G115
}

func SafeIntToUint32(v int) (uint32, bool) {
	if v < 0 || v > math.MaxUint32 {
		return 0, false
	}
	return uint32(v), true // #nosec G115
}

func SafeIntToInt32(v int) (int32, bool) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, false
	}
	return int32(v), true // #nosec G115
}

func SafeInt64ToUint64(v int64) (uint64, bool) {
	if v < 0 {
		return 0, false
	}
	return uint64(v), true // #nosec G115
}

func SafeInt32ToUint32(v int32) (uint32, bool) {
	if v < 0 {
		return 0, false
	}
	return uint32(v), true // #nosec G115
}

func SafeUint32ToInt32(v uint32) (int32, bool) {
	if v > math.MaxInt32 {
		return 0, false
	}
	return int32(v), true // #nosec G115
}

func SafeUint64ToInt64(v uint64) (int64, bool) {
	if v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true // #nosec G115
}

func SafeUint64ToInt(v uint64) (int, bool) {
	if v > math.MaxInt {
		return 0, false
	}
	return int(v), true // #nosec G115
}
