package transactiontest

import (
	"context"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// WorkflowOrderWait exposes only the native owner's closed ordering observation.
type WorkflowOrderWait interface {
	SuccessorWaiting(context.Context) (bool, error)
	Close()
}

type WorkflowDomainObservation struct {
	original startupownership.MutationAuthority
	claim    deliverylifecycle.Claim
	mu       sync.Mutex
	entered  chan struct{}
	release  chan struct{}
	joined   chan struct{}
	started  bool
	closed   bool
	wait     WorkflowOrderWait
}

func (s *Slot) HoldWorkflowDomainForTest(original startupownership.MutationAuthority, claim deliverylifecycle.Claim) (*WorkflowDomainObservation, error) {
	if err := original.Validate(); err != nil {
		return nil, err
	}
	if err := claim.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workflow != nil {
		return nil, errors.New("original workflow owner already has a commit observation")
	}
	p := &WorkflowDomainObservation{original: original, claim: claim,
		entered: make(chan struct{}), release: make(chan struct{}), joined: make(chan struct{})}
	s.workflow = p
	return p, nil
}

type workflowDomainNotificationKey struct{}
type workflowDomainNotification func(context.Context, deliverylifecycle.Claim) error

// WithWorkflowDomainNotification binds the native attempt's passive endpoint.
// The endpoint receives no SQL or mutation authority from its caller.
func WithWorkflowDomainNotification(ctx context.Context, notify func(context.Context, deliverylifecycle.Claim) error) context.Context {
	return context.WithValue(ctx, workflowDomainNotificationKey{}, workflowDomainNotification(notify))
}

func NotifyWorkflowDomainExit(ctx context.Context, claim deliverylifecycle.Claim) error {
	if notify, ok := ctx.Value(workflowDomainNotificationKey{}).(workflowDomainNotification); ok {
		return notify(ctx, claim)
	}
	return nil
}

// WorkflowDomainExit is inert unless this backend's exact original claim was
// armed. Native capture runs only after the successful domain callback exits.
func (s *Slot) WorkflowDomainExit(ctx context.Context, claim deliverylifecycle.Claim, capture func() (WorkflowOrderWait, error)) error {
	s.mu.Lock()
	p := s.workflow
	s.mu.Unlock()
	if p == nil || claim != p.claim {
		return nil
	}
	original, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil || original.ProcessAuthority() != p.original.ProcessAuthority() {
		return nil
	}
	actual, actualErr := canonicaljson.Bytes(original.GrantEvidence())
	want, wantErr := canonicaljson.Bytes(p.original.GrantEvidence())
	if actualErr != nil || wantErr != nil || string(actual) != string(want) {
		return nil
	}
	p.mu.Lock()
	if p.closed || p.started {
		p.mu.Unlock()
		return nil
	}
	if capture != nil {
		p.wait, err = capture()
		if err != nil {
			p.mu.Unlock()
			return err
		}
	}
	p.started = true
	close(p.entered)
	p.mu.Unlock()
	defer close(p.joined)
	if p.wait != nil {
		defer p.wait.Close()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.release:
		return ctx.Err()
	}
}

func (p *WorkflowDomainObservation) Entered() <-chan struct{} { return p.entered }

func (p *WorkflowDomainObservation) SuccessorWaiting(ctx context.Context) (bool, error) {
	if p == nil {
		return false, errors.New("original workflow observation is missing")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.started || p.wait == nil {
		return false, errors.New("original workflow observation is not held by PostgreSQL")
	}
	return p.wait.SuccessorWaiting(ctx)
}

func (p *WorkflowDomainObservation) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.release)
	}
	if p.wait != nil {
		p.wait.Close()
	}
	started := p.started
	p.mu.Unlock()
	if started {
		<-p.joined
	}
}
