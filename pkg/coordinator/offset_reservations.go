package coordinator

import (
	"fmt"
	"reflect"
	"sort"
	"time"
)

// ReservedTransactionOffset binds a next offset to the input partition whose
// committed position must remain stable until the transaction is resolved.
type ReservedTransactionOffset struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// TransactionOffsetReservation is the durable authorization granted by the
// group coordinator before the transaction coordinator prepares a commit.
// Group registration epoch and snapshot revision live on the enclosing record.
// Membership may subsequently change without invalidating this authorization.
type TransactionOffsetReservation struct {
	TransactionalID string                      `json:"transactional_id"`
	ProducerID      string                      `json:"producer_id"`
	ProducerEpoch   int64                       `json:"producer_epoch"`
	MemberID        string                      `json:"member_id"`
	Generation      int                         `json:"generation"`
	Offsets         []ReservedTransactionOffset `json:"offsets"`
}

func cloneOffsetReservations(reservations []TransactionOffsetReservation) []TransactionOffsetReservation {
	if len(reservations) == 0 {
		return nil
	}
	result := append([]TransactionOffsetReservation(nil), reservations...)
	for i := range result {
		result[i].Offsets = append([]ReservedTransactionOffset(nil), result[i].Offsets...)
		sort.Slice(result[i].Offsets, func(a, b int) bool {
			left, right := result[i].Offsets[a], result[i].Offsets[b]
			if left.Topic != right.Topic {
				return left.Topic < right.Topic
			}
			return left.Partition < right.Partition
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TransactionalID < result[j].TransactionalID })
	return result
}

func validateOffsetReservations(reservations []TransactionOffsetReservation) error {
	transactions := make(map[string]bool, len(reservations))
	partitions := make(map[TopicPartition]bool)
	for _, reservation := range reservations {
		if reservation.TransactionalID == "" || reservation.ProducerID == "" || reservation.ProducerEpoch < 0 || reservation.MemberID == "" || reservation.Generation < 0 || len(reservation.Offsets) == 0 {
			return fmt.Errorf("invalid transaction offset reservation identity or offsets")
		}
		if transactions[reservation.TransactionalID] {
			return fmt.Errorf("duplicate transaction offset reservation %q", reservation.TransactionalID)
		}
		transactions[reservation.TransactionalID] = true
		for _, offset := range reservation.Offsets {
			if offset.Topic == "" || offset.Partition < 0 {
				return fmt.Errorf("invalid reserved offset partition")
			}
			partition := TopicPartition{Topic: offset.Topic, Partition: offset.Partition}
			if partitions[partition] {
				return fmt.Errorf("overlapping transaction offset reservations topic=%s partition=%d", offset.Topic, offset.Partition)
			}
			partitions[partition] = true
		}
	}
	return nil
}

func restoreOffsetReservations(groups map[string]*GroupMetadata, snapshots map[string]lifecycleSnapshotCandidate) (int, error) {
	orphans := 0
	for name, candidate := range snapshots {
		record := candidate.record
		group := groups[name]
		if group == nil || group.RegistrationEpoch != record.Epoch {
			orphans++
			continue
		}
		if err := validateOffsetReservations(record.Reservations); err != nil {
			return orphans, err
		}
		for _, reservation := range record.Reservations {
			for _, offset := range reservation.Offsets {
				if !groupAcceptsTopic(group, offset.Topic) || offset.Partition >= groupTopicPartitionCount(group, offset.Topic) {
					return orphans, fmt.Errorf("reservation references undeclared group partition group=%s topic=%s partition=%d", name, offset.Topic, offset.Partition)
				}
			}
		}
		group.OffsetReservations = cloneOffsetReservations(record.Reservations)
		group.ReservationRevision = record.Revision
	}
	return orphans, nil
}

// PrepareOffsetReservation persists authorization while membership and offset
// mutations are excluded. The controller must receive success before persisting
// prepare_commit. An identical retry remains valid after membership changes.
func (c *Coordinator) PrepareOffsetReservation(groupName string, registrationEpoch uint64, reservation TransactionOffsetReservation) error {
	if err := validateOffsetReservations([]TransactionOffsetReservation{reservation}); err != nil {
		return err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	if group == nil {
		return fmt.Errorf("group %q not found", groupName)
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if registrationEpoch == 0 || group.RegistrationEpoch != registrationEpoch {
		return fmt.Errorf("ERROR: group_epoch_mismatch group=%s", groupName)
	}
	if c.lifecyclePending[groupName] {
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}
	reservation = cloneOffsetReservations([]TransactionOffsetReservation{reservation})[0]
	for _, existing := range group.OffsetReservations {
		if existing.TransactionalID == reservation.TransactionalID {
			if reflect.DeepEqual(existing, reservation) {
				// A previous append may have failed after reaching disk. Keep
				// the input fenced and confirm a new checkpoint before ACKing.
				return c.persistOffsetReservationsLocked(groupName, group, group.OffsetReservations)
			}
			return fmt.Errorf("transaction offset reservation conflict transactional_id=%s", reservation.TransactionalID)
		}
	}
	if response := c.validateMemberGenerationLocked(groupName, reservation.MemberID, reservation.Generation); response != "" {
		return fmt.Errorf("%s", response)
	}
	member := group.Members[reservation.MemberID]
	for _, offset := range reservation.Offsets {
		if !groupAcceptsTopic(group, offset.Topic) || offset.Partition >= groupTopicPartitionCount(group, offset.Topic) {
			return fmt.Errorf("reservation references undeclared group partition")
		}
		owned := false
		for _, assignment := range member.TopicAssignments {
			if assignment.Topic == offset.Topic && assignment.Partition == offset.Partition {
				owned = true
				break
			}
		}
		if len(member.TopicAssignments) == 0 && groupTopicMatches(group.TopicName, offset.Topic) {
			owned = contains(member.Assignments, offset.Partition)
		}
		if !owned {
			return fmt.Errorf("ERROR: NOT_OWNER topic=%s partition=%d member=%s", offset.Topic, offset.Partition, reservation.MemberID)
		}
		current, found := group.getOffsetSafe(offset.Topic, offset.Partition)
		if c.transactionalOffsets != nil {
			if committed, ok := c.transactionalOffsets.CommittedOffset(groupName, offset.Topic, offset.Partition, registrationEpoch); ok && (!found || committed > current) {
				current, found = committed, true
			}
		}
		if found && offset.Offset < current {
			return fmt.Errorf("offset regression topic=%s partition=%d", offset.Topic, offset.Partition)
		}
	}
	next := append(cloneOffsetReservations(group.OffsetReservations), reservation)
	if err := validateOffsetReservations(next); err != nil {
		return err
	}
	if err := c.persistOffsetReservationsLocked(groupName, group, next); err != nil {
		// Preserve the fence even on an ambiguous append error. Otherwise a
		// replacement member could read the old offset before a restart
		// recovered this same reservation as durable authorization.
		group.OffsetReservations = cloneOffsetReservations(next)
		return err
	}
	return nil
}

// ResolveOffsetReservation is called only after the transaction coordinator's
// durable final decision. A commit materializes offsets before releasing the
// reservation; failure keeps stable reads fenced for an idempotent retry.
func (c *Coordinator) ResolveOffsetReservation(groupName string, registrationEpoch uint64, transactionalID, producerID string, producerEpoch int64, committed bool) error {
	if transactionalID == "" || producerID == "" || producerEpoch < 0 {
		return fmt.Errorf("invalid transaction reservation identity")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	if group == nil {
		return fmt.Errorf("group %q not found", groupName)
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if registrationEpoch == 0 || group.RegistrationEpoch != registrationEpoch {
		return fmt.Errorf("ERROR: group_epoch_mismatch group=%s", groupName)
	}
	if c.lifecyclePending[groupName] {
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}
	index := -1
	for i, reservation := range group.OffsetReservations {
		if reservation.TransactionalID != transactionalID {
			continue
		}
		if reservation.ProducerID != producerID || reservation.ProducerEpoch != producerEpoch {
			return fmt.Errorf("transaction offset reservation producer fenced")
		}
		index = i
		break
	}
	if index < 0 && committed {
		return nil
	}
	if index >= 0 && committed {
		byTopic := make(map[string][]OffsetItem)
		for _, offset := range group.OffsetReservations[index].Offsets {
			value := offset.Offset
			if current, ok := group.getOffsetSafe(offset.Topic, offset.Partition); ok && current > value {
				value = current
			}
			byTopic[offset.Topic] = append(byTopic[offset.Topic], OffsetItem{Partition: offset.Partition, Offset: value})
		}
		topics := make([]string, 0, len(byTopic))
		for name := range byTopic {
			topics = append(topics, name)
		}
		sort.Strings(topics)
		for _, name := range topics {
			if group.OffsetRevisions[name] == ^uint64(0) {
				return fmt.Errorf("offset revision overflow")
			}
			if group.OffsetRevisions == nil {
				group.OffsetRevisions = make(map[string]uint64)
			}
			// A durable append can succeed even when its response is lost. Never
			// reuse that revision for a different later snapshot.
			group.OffsetRevisions[name]++
			items := mergedOffsetSnapshot(group, name, byTopic[name])
			if err := c.writeOffsetSnapshot(groupName, name, registrationEpoch, group.OffsetRevisions[name], items); err != nil {
				return err
			}
			for _, item := range byTopic[name] {
				group.storeOffset(name, item.Partition, item.Offset)
			}
		}
	}
	next := cloneOffsetReservations(group.OffsetReservations)
	if index >= 0 {
		// cloneOffsetReservations sorts the slice, so remove by identity.
		for i, reservation := range next {
			if reservation.TransactionalID == transactionalID {
				next = append(next[:i], next[i+1:]...)
				break
			}
		}
	}
	// Even an absent abort writes a checkpoint: an earlier failed prepare
	// append may have reached disk without being installed in memory.
	return c.persistOffsetReservationsLocked(groupName, group, next)
}

func (c *Coordinator) persistOffsetReservationsLocked(groupName string, group *GroupMetadata, reservations []TransactionOffsetReservation) error {
	if !c.authoritativeOffsetWritesEnabled() {
		return fmt.Errorf("durable offset reservation writer unavailable")
	}
	if group.ReservationRevision == ^uint64(0) {
		return fmt.Errorf("reservation revision overflow")
	}
	group.ReservationRevision++
	record := ConsumerMetadataRecord{Version: ConsumerMetadataRecordVersionReservations, Type: ConsumerMetadataRecordOffsetReservations, Group: groupName, Epoch: group.RegistrationEpoch, Revision: group.ReservationRevision, Reservations: cloneOffsetReservations(reservations), Timestamp: time.Now().UTC()}
	if err := c.writeConsumerMetadataRecord(record); err != nil {
		return err
	}
	group.OffsetReservations = cloneOffsetReservations(reservations)
	return nil
}

func reservedOffsetError(group *GroupMetadata, groupName, topic string, partition int) error {
	for _, reservation := range group.OffsetReservations {
		for _, offset := range reservation.Offsets {
			if offset.Topic == topic && offset.Partition == partition {
				return fmt.Errorf("ERROR: unstable_offset_commit group=%s topic=%s partition=%d transactional_id=%s", groupName, topic, partition, reservation.TransactionalID)
			}
		}
	}
	return nil
}

// GetStableOffset refuses to expose a resume position while its transaction
// has a durable reservation. Unrelated partitions remain available.
func (c *Coordinator) GetStableOffset(groupName, topic string, partition int) (uint64, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	if group == nil {
		return 0, false, nil
	}
	group.mu.RLock()
	defer group.mu.RUnlock()
	if err := reservedOffsetError(group, groupName, topic, partition); err != nil {
		return 0, false, err
	}
	offset, found := group.getOffsetSafe(topic, partition)
	if c.transactionalOffsets != nil {
		if committed, ok := c.transactionalOffsets.CommittedOffset(groupName, topic, partition, group.RegistrationEpoch); ok && (!found || committed > offset) {
			offset, found = committed, true
		}
	}
	return offset, found, nil
}
