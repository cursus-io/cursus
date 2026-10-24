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
	ch                *CommandHandler
	ctx               context.Context
	releaseContext    func()
	count             int
	leases            map[int]*topic.DetachedMetadataLease
	sources           map[int]ConsumerMetadataRecoverySource
	historyIdentities map[int]string
	done              func(error)
	once              sync.Once
	closeErr          error
}

// These methods deliberately do not implement ConsumerMetadataIndexReader:
// adopted transfer evidence is process-local, not a durable startup index.
func (s *stagedConsumerMetadata) ConsumerHistoryIdentity(p int) (string, error) {
	if err := s.ctx.Err(); err != nil {
		return "", err
	}
	lease, err := s.lease(config.ConsumerOffsetsTopicName, p)
	if err != nil || s.historyIdentities[p] == "" {
		return "", fmt.Errorf("staged history generation unavailable")
	}
	b := lease.Bounds()
	return fmt.Sprintf("%s:%d:%d:%d:%d", s.historyIdentities[p], b.StartOffset, b.EndOffset, b.RawBytes, b.IndexBytes), nil
}

func (s *stagedConsumerMetadata) ReadRawConsumerHistory(p int, next uint64, max int) ([]types.Message, error) {
	lease, err := s.lease(config.ConsumerOffsetsTopicName, p)
	if err != nil {
		return nil, err
	}
	return lease.ReadRawMetadata(s.ctx, next, max)
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
