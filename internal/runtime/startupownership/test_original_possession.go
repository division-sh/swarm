package startupownership

import (
	"context"
	"errors"
	"sync"
)

// PossessionObservationHoldForTest pauses scheduling before serialization and
// deadline admission. It neither supplies a possession result nor holds opMu.
// The dormant production hook is used only when this exact owner is armed.
type PossessionObservationHoldForTest struct {
	owner   *processCapability
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	joined  chan struct{}
	started bool
	closed  bool
}

func HoldOriginalPossessionObservationForTest(original MutationAuthority) (*PossessionObservationHoldForTest, error) {
	if err := original.Validate(); err != nil {
		return nil, err
	}
	hold := &PossessionObservationHoldForTest{
		owner: original.owner, entered: make(chan struct{}), release: make(chan struct{}), joined: make(chan struct{}),
	}
	if !original.owner.observationHold.CompareAndSwap(nil, hold) {
		return nil, errors.New("original possession observation already has a hold")
	}
	return hold, nil
}

func (h *PossessionObservationHoldForTest) Entered() <-chan struct{} { return h.entered }

func (h *PossessionObservationHoldForTest) wait(ctx context.Context) error {
	h.mu.Lock()
	if h.closed || h.started {
		h.mu.Unlock()
		return nil
	}
	h.started = true
	close(h.entered)
	h.mu.Unlock()
	defer close(h.joined)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.release:
		return nil
	}
}

// Close releases and joins the scheduling hold, not the process capability.
func (h *PossessionObservationHoldForTest) Close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		close(h.release)
	}
	started := h.started
	h.mu.Unlock()
	h.owner.observationHold.CompareAndSwap(h, nil)
	if started {
		<-h.joined
	}
}
