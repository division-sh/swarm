package startupownership

import (
	"context"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeowner "github.com/division-sh/swarm/internal/runtime/startupownership"
)

// OriginalProcessCaptureForTest is armed before acquisition. It retains only
// the first original constructor result, never a mutable current-session lookup.
type OriginalProcessCaptureForTest struct {
	mu         sync.Mutex
	session    *postgresSession
	capability runtimeowner.ProcessCapability
	issued     bool
}

func (s *StartupPostgresOwner) CaptureOriginalProcessForTest() (*OriginalProcessCaptureForTest, error) {
	s.constructionMu.Lock()
	defer s.constructionMu.Unlock()
	s.processOperations.mu.Lock()
	alreadyConstructed := s.processOperations.current != nil
	s.processOperations.mu.Unlock()
	if s.originalObservation != nil || alreadyConstructed {
		return nil, errors.New("original process construction observation already installed")
	}
	p := &OriginalProcessCaptureForTest{}
	s.originalObservation = p
	return p, nil
}

func (s *StartupPostgresOwner) observeConstructedProcess(session *postgresSession, capability runtimeowner.ProcessCapability) {
	s.constructionMu.Lock()
	p := s.originalObservation
	s.constructionMu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session == nil {
		p.session, p.capability = session, capability
	}
}

type OriginalSessionInterruptionForTest struct {
	original  runtimeowner.MutationAuthority
	interrupt func(context.Context) error
}

func (p *OriginalProcessCaptureForTest) PrepareForTest(ctx context.Context, original runtimeowner.MutationAuthority) (*OriginalSessionInterruptionForTest, error) {
	if p == nil {
		return nil, errors.New("original process construction observation is missing")
	}
	if err := original.Validate(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session == nil || p.capability == nil || p.issued {
		return nil, errors.New("original process construction observation is absent or already consumed")
	}
	actual, err := p.capability.Evidence()
	if err != nil {
		return nil, err
	}
	if actual != original.ProcessAuthority() {
		return nil, errors.New("interruption authority differs from the original constructed process")
	}
	interrupt, err := p.session.owner.backend.PrepareOriginalSessionInterruptionForTest(ctx, p.session.lease.Session())
	if err != nil {
		return nil, err
	}
	p.issued = true
	return &OriginalSessionInterruptionForTest{original: original, interrupt: interrupt}, nil
}

func (h *OriginalSessionInterruptionForTest) Interrupt(ctx context.Context, original runtimeowner.MutationAuthority) error {
	if h == nil || h.interrupt == nil {
		return errors.New("original session interruption is missing")
	}
	if err := original.Validate(); err != nil {
		return err
	}
	actual, actualErr := canonicaljson.Bytes(original.GrantEvidence())
	want, wantErr := canonicaljson.Bytes(h.original.GrantEvidence())
	if original.ProcessAuthority() != h.original.ProcessAuthority() || actualErr != nil || wantErr != nil || string(actual) != string(want) {
		return errors.New("session interruption belongs to a different original owner")
	}
	return h.interrupt(ctx)
}
