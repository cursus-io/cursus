//go:build !e2e_faults

package cluster

import "context"

func injectedRecoverySourceDelay(context.Context, string) error { return nil }
