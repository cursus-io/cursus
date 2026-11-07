//go:build !e2e_faults

// Package faultpoint provides deterministic operation boundaries for the
// isolated fault-test image. Production builds perform no filesystem/env access.
package faultpoint

func Pause(string, string) error { return nil }

func Fail(string, string, error) error { return nil }

func FailOffset(string, string, int, uint64, error) error { return nil }

func PauseOffset(string, string, int, uint64) error { return nil }
