package runhandoff

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type CandidateCoordinator struct {
	mu      sync.Mutex
	entries map[string]*candidateSinkEntry
}

func NewCandidateCoordinator() *CandidateCoordinator {
	return &CandidateCoordinator{}
}

type candidateSinkEntry struct {
	sink        runtimerunlifecycle.CandidateSink
	pending     int
	pendingZero chan struct{}
}

type candidateRegistration struct {
	once    sync.Once
	release func()
}

type CandidateHandoff struct {
	lease      *worklifetime.Lease
	ctx        context.Context
	handoffs   []candidateHandoff
	barriers   []*candidateRegistrationBarrier
	identities map[runtimerunlifecycle.CandidateIdentity]struct{}
	settled    bool
}

type candidateRegistrationBarrier struct {
	once    sync.Once
	release func()
}

type candidateHandoff struct {
	admission runtimerunlifecycle.CandidateAdmission
	candidate runtimerunlifecycle.Candidate
}

func detachedRunLifecycleCandidateContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithoutCancel(ctx)
}

func ReserveCandidateHandoff(ctx context.Context) (*CandidateHandoff, error) {
	detached := detachedRunLifecycleCandidateContext(ctx)
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok {
		return &CandidateHandoff{ctx: detached}, nil
	}
	lease, err := owner.Begin(detached)
	if err != nil {
		return nil, fmt.Errorf("reserve completion candidate handoff: %w", err)
	}
	return &CandidateHandoff{lease: lease, ctx: detached}, nil
}

func WithCandidateHandoff(
	ctx context.Context,
	fn func(*CandidateHandoff) error,
) error {
	handoff, err := ReserveCandidateHandoff(ctx)
	if err != nil {
		return err
	}
	defer handoff.Rollback()
	if fn != nil {
		if err := fn(handoff); err != nil {
			return err
		}
	}
	return handoff.Commit()
}

func WithCandidateHandoffResult[T any](
	ctx context.Context,
	fn func(*CandidateHandoff) (T, error),
) (T, error) {
	var zero T
	handoff, err := ReserveCandidateHandoff(ctx)
	if err != nil {
		return zero, err
	}
	defer handoff.Rollback()
	result, err := fn(handoff)
	if err != nil {
		return zero, err
	}
	if err := handoff.Commit(); err != nil {
		return zero, err
	}
	return result, nil
}

func (r *CandidateHandoff) Prepare(
	sinks *CandidateCoordinator,
	result runtimerunlifecycle.CandidateRequestResult,
) error {
	if !result.RequiresRepresentation() {
		return nil
	}
	if r == nil {
		return errors.New("completion candidate request requires explicit post-commit handoff ownership")
	}
	if sinks == nil {
		return errors.New("completion candidate coordinator is required")
	}
	if err := result.Candidate.Validate(); err != nil {
		return err
	}
	identity := result.Candidate.Identity()
	if r.identities == nil {
		r.identities = make(map[runtimerunlifecycle.CandidateIdentity]struct{})
	}
	if _, exists := r.identities[identity]; exists {
		return nil
	}
	r.identities[identity] = struct{}{}
	sink, barrier := sinks.reserve(result.Candidate.BundleHash)
	if sink == nil {
		r.barriers = append(r.barriers, barrier)
		return nil
	}
	handoffCtx := r.ctx
	if r.lease != nil {
		handoffCtx = detachedRunLifecycleCandidateContext(r.lease.Context())
	}
	admission, err := sink.ReserveCompletionCandidate(handoffCtx)
	if err != nil {
		return fmt.Errorf("reserve completion candidate executor admission: %w", err)
	}
	r.handoffs = append(r.handoffs, candidateHandoff{
		admission: admission, candidate: result.Candidate,
	})
	return nil
}

func (r *CandidateHandoff) Commit() error {
	if r == nil || r.settled {
		return nil
	}
	r.settled = true
	var submitErr error
	for _, handoff := range r.handoffs {
		submitErr = errors.Join(
			submitErr,
			handoff.admission.Submit(handoff.candidate),
		)
	}
	for _, barrier := range r.barriers {
		barrier.Settle()
	}
	if r.lease != nil {
		submitErr = errors.Join(submitErr, r.lease.Done())
	}
	return submitErr
}

func (r *CandidateHandoff) Rollback() {
	if r == nil || r.settled {
		return
	}
	r.settled = true
	for _, handoff := range r.handoffs {
		_ = handoff.admission.Cancel()
	}
	for _, barrier := range r.barriers {
		barrier.Settle()
	}
	if r.lease != nil {
		_ = r.lease.Done()
	}
}

func (b *candidateRegistrationBarrier) Settle() {
	if b != nil {
		b.once.Do(b.release)
	}
}

func (r *candidateRegistration) Release() {
	if r != nil {
		r.once.Do(r.release)
	}
}

func (r *CandidateCoordinator) Register(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	sink runtimerunlifecycle.CandidateSink,
) (runtimerunlifecycle.CandidateRegistration, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errors.New("completion candidate sink is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	bundleHash := strings.TrimSpace(scope.BundleHash)
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[string]*candidateSinkEntry)
	}
	entry := r.entries[bundleHash]
	if entry == nil {
		entry = &candidateSinkEntry{}
		r.entries[bundleHash] = entry
	}
	if entry.sink != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("completion candidate sink already registered for bundle_hash %s", bundleHash)
	}
	entry.sink = sink
	pendingZero := entry.pendingZero
	r.mu.Unlock()

	if pendingZero != nil {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			if entry.sink == sink {
				entry.sink = nil
			}
			if entry.pending == 0 {
				delete(r.entries, bundleHash)
			}
			r.mu.Unlock()
			return nil, fmt.Errorf("wait for pre-registration completion candidate mutations: %w", context.Cause(ctx))
		case <-pendingZero:
		}
	}
	return &candidateRegistration{release: func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if entry.sink == sink {
			entry.sink = nil
		}
		if entry.pending == 0 {
			delete(r.entries, bundleHash)
		}
	}}, nil
}

// Registered reports whether sink currently owns the exact candidate scope.
// It is an observation only; admission still happens through Register/reserve.
func (r *CandidateCoordinator) Registered(bundleHash string, sink runtimerunlifecycle.CandidateSink) bool {
	if r == nil || sink == nil {
		return false
	}
	bundleHash = strings.TrimSpace(bundleHash)
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[bundleHash]
	return entry != nil && entry.sink == sink
}

func (r *CandidateCoordinator) reserve(
	bundleHash string,
) (runtimerunlifecycle.CandidateSink, *candidateRegistrationBarrier) {
	bundleHash = strings.TrimSpace(bundleHash)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[string]*candidateSinkEntry)
	}
	entry := r.entries[bundleHash]
	if entry == nil {
		entry = &candidateSinkEntry{}
		r.entries[bundleHash] = entry
	}
	if entry.sink != nil {
		return entry.sink, nil
	}
	if entry.pending == 0 {
		entry.pendingZero = make(chan struct{})
	}
	entry.pending++
	return nil, &candidateRegistrationBarrier{release: func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		entry.pending--
		if entry.pending < 0 {
			panic("completion candidate registration barrier underflow")
		}
		if entry.pending == 0 {
			close(entry.pendingZero)
			entry.pendingZero = nil
			if entry.sink == nil {
				delete(r.entries, bundleHash)
			}
		}
	}}
}
