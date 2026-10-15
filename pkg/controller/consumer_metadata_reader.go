package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

type stagedConsumerMetadata struct {
	ch             *CommandHandler
	ctx            context.Context
	releaseContext func()
	count          int
	leases         map[int]*topic.DetachedMetadataLease
	sources        map[int]ConsumerMetadataRecoverySource
	done           func(error)
	once           sync.Once
	closeErr       error
}

func (s *stagedConsumerMetadata) Sources() map[int]ConsumerMetadataRecoverySource {
	copy := make(map[int]ConsumerMetadataRecoverySource, len(s.sources))
	for p, source := range s.sources {
		copy[p] = source
	}
	return copy
}
func (s *stagedConsumerMetadata) Close() error {
	s.once.Do(func() {
		if s.releaseContext != nil {
			s.releaseContext()
		}
		for _, lease := range s.leases {
			s.closeErr = errors.Join(s.closeErr, lease.Close())
		}
		if s.done != nil {
			s.done(s.closeErr)
		}
	})
	return s.closeErr
}
func (s *stagedConsumerMetadata) lease(name string, p int) (*topic.DetachedMetadataLease, error) {
	lease := s.leases[p]
	if name != config.ConsumerOffsetsTopicName || lease == nil {
		return nil, fmt.Errorf("metadata partition was not staged")
	}
	return lease, nil
}
func (s *stagedConsumerMetadata) ExistingPartitionCount(name string) (int, error) {
	if name != config.ConsumerOffsetsTopicName {
		return 0, fmt.Errorf("metadata topic was not staged")
	}
	return s.count, nil
}
func (s *stagedConsumerMetadata) EarliestTopicOffset(name string, p int) (uint64, error) {
	lease, err := s.lease(name, p)
	if err != nil {
		return 0, err
	}
	return lease.Bounds().StartOffset, nil
}
func (s *stagedConsumerMetadata) EndTopicOffset(name string, p int) (uint64, error) {
	lease, err := s.lease(name, p)
	if err != nil {
		return 0, err
	}
	return lease.Bounds().EndOffset, nil
}
func (s *stagedConsumerMetadata) ReadCommittedConsumerMetadata(name string, p int, offset uint64, maxRecords int) ([]types.Message, error) {
	lease, err := s.lease(name, p)
	if err != nil {
		return nil, err
	}
	var resolver topic.TransactionDecisionResolver
	if s.ch.TxnManager != nil {
		resolver = s.ch.TxnManager
	}
	return lease.ReadCommittedMetadata(s.ctx, offset, maxRecords, resolver)
}
