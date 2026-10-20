package disk

import (
	"fmt"
	"sync"
)

var bootstrapBudget = struct {
	sync.Mutex
	active int
	bytes  int64
}{}

// Owners are private to a transfer and serialized by its mutex after publication.
// Keeping them separate allows the active slot and physical-byte reservation to
// have distinct lifetimes without sharing a release counter.
type bootstrapActiveToken struct{ owned bool }

type bootstrapByteOwner struct {
	owned    bool
	reserved int64
	limits   BootstrapLimits
}

func (token *bootstrapActiveToken) release() {
	if !token.owned {
		return
	}
	bootstrapBudget.Lock()
	bootstrapBudget.active--
	token.owned = false
	bootstrapBudget.Unlock()
}

func (owner *bootstrapByteOwner) release() {
	if !owner.owned {
		return
	}
	bootstrapBudget.Lock()
	bootstrapBudget.bytes -= owner.reserved
	owner.owned = false
	bootstrapBudget.Unlock()
}

func (owner *bootstrapByteOwner) grow(d *DiskHandler, size int64) error {
	return owner.growWithHeadroom(d.headroom, size)
}

// Probe outside process-wide accounting: a blocked filesystem must not stop
// unrelated admissions. A later budget rejection retains the conservative
// headroom reservation; no physical free-space refund is fabricated.
func (owner *bootstrapByteOwner) growWithHeadroom(headroom *diskHeadroomGuard, size int64) error {
	if size < 0 || !owner.owned {
		return fmt.Errorf("invalid bootstrap byte ownership or size")
	}
	if size <= owner.reserved {
		return nil
	}
	if size > owner.limits.MaxTransferBytes {
		return &BootstrapLimitError{Resource: "canonical bytes", Requested: size, Limit: owner.limits.MaxTransferBytes}
	}
	delta := size - owner.reserved
	// #nosec G115 -- size > owner.reserved >= 0 above, so delta is positive and fits int64.
	if _, err := headroom.reserve(uint64(delta), false); err != nil {
		return err
	}
	bootstrapBudget.Lock()
	defer bootstrapBudget.Unlock()
	requested, err := addBootstrapBytes(bootstrapBudget.bytes, delta)
	if err != nil {
		return err
	}
	if requested > owner.limits.MaxTotalBytes {
		return &BootstrapLimitError{Resource: "total staging bytes", Requested: requested, Limit: owner.limits.MaxTotalBytes}
	}
	bootstrapBudget.bytes = requested
	owner.reserved = size
	return nil
}

// At most BootstrapMaxActive retired transfers can retain failed cleanup work:
// every queue entry still owns its active slot, including zero-byte staging.
var bootstrapCleanup = struct {
	sync.Mutex
	pending []*bootstrapTransfer
}{}

// Caller holds t.mu. Never enroll an installed transfer: durable intent owns it.
func retainBootstrapCleanup(t *bootstrapTransfer) {
	if !t.activeToken.owned || t.installed {
		panic("bootstrap cleanup requires transient active ownership")
	}
	bootstrapCleanup.Lock()
	defer bootstrapCleanup.Unlock()
	for _, pending := range bootstrapCleanup.pending {
		if pending == t {
			return
		}
	}
	bootstrapCleanup.pending = append(bootstrapCleanup.pending, t)
}

func forgetBootstrapCleanup(t *bootstrapTransfer) {
	bootstrapCleanup.Lock()
	defer bootstrapCleanup.Unlock()
	for i, pending := range bootstrapCleanup.pending {
		if pending == t {
			copy(bootstrapCleanup.pending[i:], bootstrapCleanup.pending[i+1:])
			last := len(bootstrapCleanup.pending) - 1
			bootstrapCleanup.pending[last] = nil
			bootstrapCleanup.pending = bootstrapCleanup.pending[:last]
			return
		}
	}
}

func retryBootstrapCleanup() {
	bootstrapCleanup.Lock()
	if len(bootstrapCleanup.pending) == 0 {
		bootstrapCleanup.Unlock()
		return
	}
	t := bootstrapCleanup.pending[0]
	copy(bootstrapCleanup.pending, bootstrapCleanup.pending[1:])
	bootstrapCleanup.pending[len(bootstrapCleanup.pending)-1] = t
	bootstrapCleanup.Unlock()
	// Admission may hold partition locks needed by an installer holding t.mu.
	// Neither the queue nor accounting mutex may span filesystem cleanup.
	if !t.mu.TryLock() {
		return
	}
	defer t.mu.Unlock()
	_ = t.cleanupLocked()
}
