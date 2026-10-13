// Package startuphandoff contains delegating witnesses for startup regression tests.
package startuphandoff

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type Barrier struct {
	Entered chan struct{}
	release chan struct{}
	enter   sync.Once
	once    sync.Once
}

func newBarrier() *Barrier {
	return &Barrier{Entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *Barrier) Release() { b.once.Do(func() { close(b.release) }) }

func (b *Barrier) wait(ctx context.Context) error {
	b.enter.Do(func() { close(b.Entered) })
	select {
	case <-b.release:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Proof struct {
	Recovery     *Barrier
	Continuation *Barrier
	Published    chan lifecycleprobe.Signal

	mu                  sync.Mutex
	recoverySettled     bool
	continuationSettled bool
	partial             int
	blocked             bool
	recoveryAccepted    bool
	continuationScans   int
	ids                 map[string]bool
	types               map[string]bool
	publications        map[string]int
	claims              map[string]int
	violations          []string
	RecoveryError       error
}

func New() *Proof {
	return &Proof{
		Recovery: newBarrier(), Continuation: newBarrier(), Published: make(chan lifecycleprobe.Signal, 16),
		ids: make(map[string]bool), types: make(map[string]bool),
		publications: make(map[string]int), claims: make(map[string]int),
	}
}

func (p *Proof) WatchID(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids[id] = true
}

func (p *Proof) WatchType(eventType string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.types[eventType] = true
}

func (p *Proof) Release() {
	p.Recovery.Release()
	p.Continuation.Release()
}

func (p *Proof) MarkContinuationsSettled() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.recoverySettled || p.continuationScans < 2 {
		p.violations = append(p.violations, "continuation handoff omitted native recovery or post-Start enumeration")
	}
	p.continuationSettled = true
}

func (p *Proof) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.EventPersisted {
		return
	}
	p.mu.Lock()
	if !p.ids[signal.EventID] && !p.types[signal.EventType] {
		p.mu.Unlock()
		return
	}
	p.checkLocked("publication", signal.EventID)
	p.publications[signal.EventID]++
	p.mu.Unlock()
	select {
	case p.Published <- signal:
	default:
	}
}

func (p *Proof) checkLocked(kind, id string) {
	if !p.recoverySettled {
		p.violations = append(p.violations, fmt.Sprintf("%s %s before recovery exhaustion settled", kind, id))
	} else if !p.continuationSettled {
		p.violations = append(p.violations, fmt.Sprintf("%s %s before continuation synchronization settled", kind, id))
	}
}

func (p *Proof) ObserveExecutionAttempt(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checkLocked("callback attempt", id)
}

type Observation struct {
	Partial, ContinuationScans           int
	RecoverySettled, ContinuationSettled bool
	Publications, Claims                 map[string]int
	Violations                           []string
}

func (p *Proof) Observe() Observation {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := Observation{
		Partial: p.partial, ContinuationScans: p.continuationScans,
		RecoverySettled: p.recoverySettled, ContinuationSettled: p.continuationSettled,
		Publications: make(map[string]int), Claims: make(map[string]int),
		Violations: append([]string(nil), p.violations...),
	}
	for id, count := range p.publications {
		result.Publications[id] = count
	}
	for id, count := range p.claims {
		result.Claims[id] = count
	}
	return result
}

func (p *Proof) RecoveryStore(owner pipelineobligation.Store) pipelineobligation.Store {
	return &recoveryStore{Store: owner, proof: p}
}

type recoveryStore struct {
	pipelineobligation.Store
	proof *Proof
}

func (s *recoveryStore) ClaimPublication(ctx context.Context, id string) (pipelineobligation.Claim, error) {
	s.proof.mu.Lock()
	if s.proof.ids[id] {
		s.proof.checkLocked("claim", id)
		s.proof.claims[id]++
	}
	s.proof.mu.Unlock()
	return s.Store.ClaimPublication(ctx, id)
}

func (s *recoveryStore) ClaimBatch(ctx context.Context, scan pipelineobligation.Scan, _ int) (pipelineobligation.ScanBatch, error) {
	batch, err := s.Store.ClaimBatch(ctx, scan, 1)
	if err != nil {
		return batch, err
	}
	s.proof.mu.Lock()
	if !batch.Exhausted && batch.Examined > 0 {
		s.proof.partial++
	}
	s.proof.blocked = s.proof.blocked || batch.LocallyBlocked
	s.proof.mu.Unlock()
	if batch.Exhausted {
		if err := s.proof.Recovery.wait(ctx); err != nil {
			return batch, err
		}
		if s.proof.RecoveryError != nil {
			return batch, s.proof.RecoveryError
		}
		s.proof.mu.Lock()
		s.proof.recoveryAccepted = !s.proof.blocked && ctx.Err() == nil
		s.proof.mu.Unlock()
	}
	return batch, nil
}

func (s *recoveryStore) CloseScan(ctx context.Context, scan pipelineobligation.Scan) error {
	err := s.Store.CloseScan(ctx, scan)
	s.proof.mu.Lock()
	if err == nil && s.proof.recoveryAccepted && ctx.Err() == nil {
		s.proof.recoverySettled = true
	}
	s.proof.mu.Unlock()
	return err
}

func (p *Proof) DeliveryStore(owner deliverylifecycle.Store) deliverylifecycle.Store {
	return &deliveryStore{Store: owner, proof: p}
}

type deliveryStore struct {
	deliverylifecycle.Store
	proof *Proof
}

func (s *deliveryStore) ScanDeliveryContinuations(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, cursor deliverylifecycle.ContinuationCursor, limit int) (deliverylifecycle.ContinuationPage, error) {
	s.proof.mu.Lock()
	s.proof.continuationScans++
	postStart := s.proof.continuationScans > 1
	s.proof.mu.Unlock()
	if postStart {
		if err := s.proof.Continuation.wait(ctx); err != nil {
			return deliverylifecycle.ContinuationPage{}, err
		}
	}
	return s.Store.ScanDeliveryContinuations(ctx, authority, cursor, limit)
}

func (p *Proof) CheckWithheld() error {
	observed := p.Observe()
	if len(observed.Publications) != 0 || len(observed.Claims) != 0 || len(observed.Violations) != 0 {
		return fmt.Errorf("autonomous work escaped held startup handoff: %+v", observed)
	}
	return nil
}

func (p *Proof) CheckPublished(id string) error {
	observed := p.Observe()
	if !observed.RecoverySettled || !observed.ContinuationSettled || observed.Publications[id] != 1 || len(observed.Violations) != 0 {
		return fmt.Errorf("late publication lost exact handoff/occurrence: %s %+v", id, observed)
	}
	return nil
}

func (p *Proof) Wait(ctx context.Context, barrier *Barrier) error {
	select {
	case <-barrier.Entered:
		return p.CheckWithheld()
	case <-ctx.Done():
		return errors.Join(errors.New("startup handoff witness not reached"), ctx.Err())
	}
}
