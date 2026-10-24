//go:build !e2e_faults

package topic

import "github.com/cursus-io/cursus/pkg/coordinator"

func consumerCleanupEvidenceFault(page coordinator.ConsumerCleanupPage, _ uint64) (coordinator.ConsumerCleanupPage, error) {
	return page, nil
}
