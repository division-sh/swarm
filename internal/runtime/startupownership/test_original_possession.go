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

type originalSessionInterruptionPreparerForTest interface {
	PrepareOriginalSessionInterruptionForTest(context.Context) (func(context.Context) error, error)
}

// OriginalSessionInterruptionForTest retains one fixed operation issued by the
// original native owner; neither session coordinates nor SQL escape that owner.
type OriginalSessionInterruptionForTest struct {
	original  MutationAuthority
	interrupt func(context.Context) error
}

func PrepareOriginalSessionInterruptionForTest(ctx context.Context, original MutationAuthority) (*OriginalSessionInterruptionForTest, error) {
	if err := original.Validate(); err != nil {
		return nil, err
	}
	p := original.owner
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := original.Validate(); err != nil {
		return nil, err
	}
	owner, ok := p.session.(originalSessionInterruptionPreparerForTest)
	if !ok {
		return nil, errors.New("original session has no PostgreSQL interruption proof owner")
	}
	interrupt, err := owner.PrepareOriginalSessionInterruptionForTest(ctx)
	if err != nil {
		return nil, err
	}
	return &OriginalSessionInterruptionForTest{original: original, interrupt: interrupt}, nil
}

func (h *OriginalSessionInterruptionForTest) Interrupt(ctx context.Context, original MutationAuthority) error {
	if h == nil || h.interrupt == nil {
		return errors.New("original session interruption is missing")
	}
	h.original.owner.opMu.Lock()
	defer h.original.owner.opMu.Unlock()
	if err := original.Validate(); err != nil {
		return err
	}
	if original.owner != h.original.owner || original.process != h.original.process || original.grant != h.original.grant {
		return errors.New("session interruption belongs to a different original owner")
	}
	return h.interrupt(ctx)
}
