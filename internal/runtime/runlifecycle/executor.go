package runlifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

const defaultCandidatePageSize = 128

type WakeClock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realWakeClock struct{}

func (realWakeClock) Now() time.Time                             { return time.Now().UTC() }
func (realWakeClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

type RetryPolicy interface {
	Delay(attempt int) time.Duration
}

type boundedRetryPolicy struct{}

func (boundedRetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := 25 * time.Millisecond
	for i := 0; i < attempt && delay < time.Second; i++ {
		delay *= 2
	}
	if delay > time.Second {
		delay = time.Second
	}
	return delay
}

type ExecutorOptions struct {
	Clock       WakeClock
	RetryPolicy RetryPolicy
	PageSize    int
}

type candidateChain struct {
	candidate Candidate
	cancel    context.CancelCauseFunc
}

type candidateAdmission struct {
	executor *Executor
	lease    *worklifetime.Lease
	once     sync.Once
	err      error
}

type Executor struct {
	store      CandidateStore
	scope      CandidateScope
	catalog    TerminalCatalog
	occurrence *worklifetime.RuntimeOccurrence
	clock      WakeClock
	retry      RetryPolicy
	pageSize   int

	mu          sync.Mutex
	chains      map[string]candidateChain
	started     bool
	ready       bool
	retiring    bool
	pending     int
	pendingZero chan struct{}
}

func NewExecutor(
	store CandidateStore,
	scope CandidateScope,
	catalog TerminalCatalog,
	occurrence *worklifetime.RuntimeOccurrence,
	opts ExecutorOptions,
) (*Executor, error) {
	if store == nil {
		return nil, errors.New("run lifecycle executor requires selected store")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if occurrence == nil {
		return nil, errors.New("run lifecycle executor requires runtime occurrence")
	}
	if opts.Clock == nil {
		opts.Clock = realWakeClock{}
	}
	if opts.RetryPolicy == nil {
		opts.RetryPolicy = boundedRetryPolicy{}
	}
	if opts.PageSize <= 0 {
		opts.PageSize = defaultCandidatePageSize
	}
	return &Executor{
		store: store, scope: scope, catalog: catalog, occurrence: occurrence,
		clock: opts.Clock, retry: opts.RetryPolicy, pageSize: opts.PageSize,
		chains: make(map[string]candidateChain),
	}, nil
}

func (e *Executor) Start(ctx context.Context) error {
	if e == nil {
		return errors.New("run lifecycle executor is required")
	}
	e.mu.Lock()
	if e.retiring {
		e.mu.Unlock()
		return worklifetime.ErrRetired
	}
	if e.started {
		e.mu.Unlock()
		return errors.New("run lifecycle executor already started")
	}
	e.started = true
	e.mu.Unlock()

	cursor := CandidateCursor{}
	for {
		page, err := e.store.ListCompletionCandidates(ctx, e.scope, cursor, e.pageSize)
		if err != nil {
			return fmt.Errorf("enumerate completion candidates: %w", err)
		}
		for _, candidate := range page.Candidates {
			if err := e.SubmitCompletionCandidate(ctx, candidate); err != nil {
				return fmt.Errorf("represent startup completion candidate %s/%d: %w", candidate.RunID, candidate.Revision, err)
			}
		}
		if page.Exhausted {
			break
		}
		if page.Next.RunID == "" || page.Next.RunID == cursor.RunID {
			return errors.New("completion candidate enumeration did not advance")
		}
		cursor = page.Next
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.retiring {
		return worklifetime.ErrRetired
	}
	e.ready = true
	return nil
}

func (e *Executor) Ready() bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ready && !e.retiring
}

func (e *Executor) SubmitCompletionCandidate(ctx context.Context, candidate Candidate) error {
	admission, err := e.ReserveCompletionCandidate(ctx)
	if err != nil {
		return err
	}
	if err := admission.Submit(candidate); err != nil {
		return errors.Join(err, admission.Cancel())
	}
	return nil
}

func (e *Executor) ReserveCompletionCandidate(ctx context.Context) (CandidateAdmission, error) {
	if e == nil {
		return nil, errors.New("run lifecycle executor is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.retiring {
		return nil, worklifetime.ErrRetired
	}
	lease, err := e.occurrence.Begin(context.WithoutCancel(ctx))
	if err != nil {
		return nil, err
	}
	if e.pending == 0 {
		e.pendingZero = make(chan struct{})
	}
	e.pending++
	return &candidateAdmission{executor: e, lease: lease}, nil
}

func (a *candidateAdmission) Submit(candidate Candidate) error {
	if a == nil || a.executor == nil || a.lease == nil {
		return errors.New("completion candidate admission is required")
	}
	a.once.Do(func() {
		a.err = a.executor.installReserved(a.lease, candidate)
		if a.err != nil {
			a.err = errors.Join(a.err, a.lease.Done())
		}
		a.executor.settleAdmission()
	})
	return a.err
}

func (a *candidateAdmission) Cancel() error {
	if a == nil || a.executor == nil || a.lease == nil {
		return nil
	}
	a.once.Do(func() {
		a.err = a.lease.Done()
		a.executor.settleAdmission()
	})
	return a.err
}

func (e *Executor) settleAdmission() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending--
	if e.pending < 0 {
		panic("run lifecycle executor candidate admission underflow")
	}
	if e.pending == 0 && e.pendingZero != nil {
		close(e.pendingZero)
		e.pendingZero = nil
	}
}

func (e *Executor) installReserved(lease *worklifetime.Lease, candidate Candidate) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	if candidate.BundleHash != e.scope.BundleHash {
		return fmt.Errorf("completion candidate bundle_hash %s does not match executor scope %s", candidate.BundleHash, e.scope.BundleHash)
	}
	e.mu.Lock()
	if e.retiring {
		e.mu.Unlock()
		return worklifetime.ErrRetired
	}
	if current, ok := e.chains[candidate.RunID]; ok {
		if current.candidate.Revision >= candidate.Revision {
			e.mu.Unlock()
			return lease.Done()
		}
		current.cancel(errors.New("completion candidate superseded"))
	}
	chainCtx, cancel := context.WithCancelCause(lease.Context())
	e.chains[candidate.RunID] = candidateChain{candidate: candidate, cancel: cancel}
	e.mu.Unlock()

	go e.runChain(chainCtx, lease, candidate)
	return nil
}

func (e *Executor) runChain(ctx context.Context, lease *worklifetime.Lease, candidate Candidate) {
	defer func() {
		e.mu.Lock()
		current, ok := e.chains[candidate.RunID]
		if ok && current.candidate.Revision == candidate.Revision {
			delete(e.chains, candidate.RunID)
		}
		e.mu.Unlock()
		_ = lease.Done()
	}()

	attempt := 0
	for {
		if err := e.waitUntilDue(ctx, candidate.DueAt); err != nil {
			return
		}
		// Retirement cancels waits and retries, but an admitted persistence
		// operation must finish before its occurrence lease can settle.
		result, err := e.store.ExecuteCompletionCandidate(context.WithoutCancel(ctx), candidate, e.catalog)
		if err != nil {
			result = CompletionResult{Outcome: OutcomeRetryCurrent, Retryable: err}
		}
		if validationErr := result.Validate(); validationErr != nil {
			result = CompletionResult{Outcome: OutcomeRetryCurrent, Retryable: validationErr}
		}
		switch result.Outcome {
		case OutcomeRetryCurrent:
			delay := e.retry.Delay(attempt)
			attempt++
			if err := e.wait(ctx, delay); err != nil {
				return
			}
		case OutcomeRearmAt:
			if result.Candidate.Revision == candidate.Revision {
				candidate = result.Candidate
				attempt = 0
				continue
			}
			admission, err := e.ReserveCompletionCandidate(context.WithoutCancel(ctx))
			if err != nil {
				return
			}
			if err := admission.Submit(result.Candidate); err != nil {
				return
			}
			return
		case OutcomeTerminallyEligible, OutcomeAwaitMutation, OutcomeExactNoop:
			return
		default:
			return
		}
	}
}

func (e *Executor) waitUntilDue(ctx context.Context, dueAt time.Time) error {
	delay := dueAt.Sub(e.clock.Now())
	if delay <= 0 {
		return nil
	}
	return e.wait(ctx, delay)
}

func (e *Executor) wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-e.clock.After(delay):
		return nil
	}
}

func (e *Executor) Retire(ctx context.Context) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	if !e.retiring {
		e.retiring = true
	}
	pendingZero := e.pendingZero
	chains := make([]candidateChain, 0, len(e.chains))
	for _, chain := range e.chains {
		chains = append(chains, chain)
	}
	e.mu.Unlock()
	for _, chain := range chains {
		chain.cancel(worklifetime.ErrRetired)
	}
	if pendingZero == nil {
		return nil
	}
	select {
	case <-pendingZero:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (e *Executor) ActiveCandidates() int {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.chains)
}
