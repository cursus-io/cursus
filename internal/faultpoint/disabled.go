//go:build !e2e_faults

// Package faultpoint provides deterministic operation boundaries for the
// isolated fault-test image. Production builds perform no filesystem/env access.
package faultpoint

// Pause is a no-op in builds without the e2e_faults tag.
func Pause(string, string) error { return nil }

// Fail is a no-op in builds without the e2e_faults tag.
func Fail(string, string, error) error { return nil }

// FailOffset is a no-op in builds without the e2e_faults tag.
func FailOffset(string, string, int, uint64, error) error { return nil }

// PauseOffset is a no-op in builds without the e2e_faults tag.
func PauseOffset(string, string, int, uint64) error { return nil }
