package coordinator

import (
	"errors"
	"fmt"
)

var ErrConsumerBootstrapObservationStale = errors.New("consumer bootstrap observation is stale")

// ConsumerBootstrapObservation is an opaque runtime token owned by one
// coordinator. Capture it BEFORE reading a partition's pending/status state.
// Every successful ticket removal advances the revision, including removals
// for other partitions; an overlapping observation must retry its storage read.
// The revision lasts for the coordinator's lifetime and is never imported/reset.
type ConsumerBootstrapObservation struct {
	owner    *Coordinator
	revision uint64
}

func (c *Coordinator) CaptureConsumerBootstrapObservation() ConsumerBootstrapObservation {
	if c == nil || c.standalone {
		return ConsumerBootstrapObservation{}
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	return ConsumerBootstrapObservation{owner: c, revision: g.bootstrapObservationRevision}
}

type ConsumerBootstrapIdentity struct {
	Partition int
	Lifecycle uint64
	Attempt   string
}

// ConsumerBootstrapTicket survives leases and imports. Its conservative scope
// must be retained before old files disappear. Installation, splice and cleanup
// require a lease; pending observations use only the admission mutex.
type ConsumerBootstrapTicket struct {
	owner    *Coordinator
	identity ConsumerBootstrapIdentity
	scope    *ConsumerMetadataScope
	spliced  bool
	prepared bool
	stamp    consumerBootstrapPreparedStamp
}
type consumerBootstrapPreparedStamp struct{ generation, revision, view uint64 }
type ConsumerBootstrapOutcome int

const (
	ConsumerBootstrapUncertain ConsumerBootstrapOutcome = iota
	ConsumerBootstrapPreIntentAbort
	ConsumerBootstrapCompleted
)

// ApplyConsumerBootstrapReplay preserves the expanded scope before splicing.
// Even an uncertain apply result prevents later pre-intent-abort cleanup.
func (c *Coordinator) ApplyConsumerBootstrapReplay(ticket *ConsumerBootstrapTicket, prepared *PreparedConsumerMetadataReplay, view uint64) error {
	g := &c.consumerAdmission
	g.mu.Lock()
	if !c.consumerTicketValidLocked(ticket) || prepared == nil || prepared.owner != c || prepared.scope == nil || prepared.scope.partition != ticket.identity.Partition {
		g.mu.Unlock()
		return fmt.Errorf("consumer bootstrap replay identity mismatch")
	}
	for name := range prepared.scope.names {
		ticket.scope.names[name] = struct{}{}
	}
	ticket.spliced = true
	g.mu.Unlock()
	if err := c.ApplyScopedConsumerMetadataReplay(prepared); err != nil {
		return err
	}
	return c.markConsumerBootstrapPrepared(ticket, view, prepared.stamp)
}

func (c *Coordinator) RetainConsumerBootstrapTicket(identity ConsumerBootstrapIdentity, scope *ConsumerMetadataScope) (*ConsumerBootstrapTicket, error) {
	return c.retainConsumerBootstrapTicket(identity, scope, true, nil)
}

// ObservePendingConsumerBootstrap reconstructs availability before startup can
// open. It takes only the admission mutex and never drains. An empty Attempt
// records an unreadable/missing manifest; later proven identity may replace that
// placeholder while preserving scope and invalidating stale cleanup handles.
// A stale token makes no change; the caller must discard that observation and
// retry from CaptureConsumerBootstrapObservation before reading storage again.
func (c *Coordinator) ObservePendingConsumerBootstrap(identity ConsumerBootstrapIdentity, observation ConsumerBootstrapObservation) error {
	_, err := c.retainConsumerBootstrapTicket(identity, nil, false, &observation)
	return err
}

func (c *Coordinator) retainConsumerBootstrapTicket(identity ConsumerBootstrapIdentity, scope *ConsumerMetadataScope, lease bool, observation *ConsumerBootstrapObservation) (*ConsumerBootstrapTicket, error) {
	if c == nil || c.standalone {
		return nil, ErrConsumerMetadataRecoveryStandalone
	}
	if identity.Partition < 0 || (lease && identity.Attempt == "") {
		return nil, fmt.Errorf("invalid consumer bootstrap identity")
	}
	if scope != nil && (scope.owner != c || scope.partition != identity.Partition) {
		return nil, fmt.Errorf("consumer bootstrap scope identity mismatch")
	}
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if observation != nil && (observation.owner != c || observation.revision != g.bootstrapObservationRevision) {
		return nil, ErrConsumerBootstrapObservationStale
	}
	if lease && !g.owned {
		return nil, fmt.Errorf("consumer bootstrap ticket requires exclusive lease")
	}
	if g.tickets == nil {
		g.tickets = make(map[int]*ConsumerBootstrapTicket)
	}
	ticket := g.tickets[identity.Partition]
	if ticket != nil && ticket.identity != identity {
		if !lease && identity.Attempt == "" && ticket.identity.Lifecycle == identity.Lifecycle {
			return ticket, nil
		}
		if ticket.identity.Attempt != "" || ticket.identity.Lifecycle != identity.Lifecycle || ticket.spliced {
			return nil, fmt.Errorf("different consumer bootstrap attempt remains pending")
		}
		if scope == nil {
			scope = ticket.scope
		}
		ticket = nil
	}
	if ticket == nil {
		ticket = &ConsumerBootstrapTicket{owner: c, identity: identity, scope: &ConsumerMetadataScope{owner: c, partition: identity.Partition, names: make(map[string]struct{})}}
		g.tickets[identity.Partition] = ticket
	}
	if scope != nil {
		for name := range scope.names {
			ticket.scope.names[name] = struct{}{}
		}
	}
	g.notifyLocked()
	return ticket, nil
}

func (c *Coordinator) consumerTicketValidLocked(ticket *ConsumerBootstrapTicket) bool {
	return ticket != nil && ticket.owner == c && c.consumerAdmission.owned && c.consumerAdmission.tickets[ticket.identity.Partition] == ticket
}

// ConsumerBootstrapScope detaches the retained name set for preparation.
func (c *Coordinator) ConsumerBootstrapScope(ticket *ConsumerBootstrapTicket) (*ConsumerMetadataScope, error) {
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if !c.consumerTicketValidLocked(ticket) {
		return nil, fmt.Errorf("consumer bootstrap ticket is stale")
	}
	scope := &ConsumerMetadataScope{owner: c, partition: ticket.identity.Partition, names: make(map[string]struct{})}
	for name := range ticket.scope.names {
		scope.names[name] = struct{}{}
	}
	return scope, nil
}

func (c *Coordinator) markConsumerBootstrapPrepared(ticket *ConsumerBootstrapTicket, view uint64, applied consumerMetadataReplayStamp) error {
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if !c.consumerTicketValidLocked(ticket) || g.restoreInstalling || g.restoreError != nil || g.requiredGeneration != applied.required || g.lease != applied.lease || c.authorityRevision != applied.revision+1 || c.stateImportGeneration != applied.generation {
		return fmt.Errorf("consumer bootstrap ticket is stale")
	}
	ticket.spliced, ticket.prepared = true, true
	ticket.stamp = consumerBootstrapPreparedStamp{g.requiredGeneration, c.authorityRevision, view}
	return nil
}

func (c *Coordinator) ConsumerBootstrapPreparedCurrent(ticket *ConsumerBootstrapTicket, view uint64) bool {
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	return c.consumerTicketValidLocked(ticket) && ticket.prepared && !g.restoreInstalling && g.restoreError == nil && ticket.stamp == (consumerBootstrapPreparedStamp{g.requiredGeneration, c.authorityRevision, view})
}

// FinishConsumerBootstrapTicket must run AFTER session.Close and BEFORE lease
// release. No error or an absent manifest alone is never completion evidence.
func (c *Coordinator) FinishConsumerBootstrapTicket(ticket *ConsumerBootstrapTicket, outcome ConsumerBootstrapOutcome, partitionPending bool, view uint64) error {
	c.authorityMu.Lock()
	defer c.authorityMu.Unlock()
	g := &c.consumerAdmission
	g.mu.Lock()
	defer g.mu.Unlock()
	if !c.consumerTicketValidLocked(ticket) {
		return fmt.Errorf("consumer bootstrap ticket is stale")
	}
	valid := !partitionPending && outcome == ConsumerBootstrapPreIntentAbort && !ticket.spliced
	if !partitionPending && outcome == ConsumerBootstrapCompleted {
		valid = ticket.prepared && !g.restoreInstalling && g.restoreError == nil && ticket.stamp == (consumerBootstrapPreparedStamp{g.requiredGeneration, c.authorityRevision, view})
	}
	if !valid {
		return fmt.Errorf("consumer bootstrap outcome remains uncertain")
	}
	delete(g.tickets, ticket.identity.Partition)
	g.bootstrapObservationRevision++
	g.notifyLocked()
	return nil
}
