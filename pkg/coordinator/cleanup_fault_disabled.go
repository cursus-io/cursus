//go:build !e2e_faults

package coordinator

func consumerCleanupAcknowledgementFault(ConsumerMetadataRecord) error { return nil }
