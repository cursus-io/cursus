//go:build !e2e_faults

package topic

import "github.com/cursus-io/cursus/pkg/types"

func consumerCleanupReadFault(types.ReplayCursor) error { return nil }
