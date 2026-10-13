package runlifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
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
	Clock            WakeClock
	RetryPolicy      RetryPolicy
	PageSize         int
	GenericSchedules GenericScheduleWakeupOwner
	ExecutionContext context.Context
}

type GenericScheduleWakeupOwner interface {
	ReconcileWakeupWithRecovery(context.Context, string) (bool, error)
}

type candidateChain struct {
	candidate              Candidate
	successor              *Candidate
	notificationGeneration uint64
	cancel                 context.CancelCauseFunc
	wake                   chan struct{}
}

type candidateChainAction uint8

const (
	candidateChainStop candidateChainAction = iota
	candidateChainContinue
	candidateChainRetry
)

type candidateAdmission struct {
	executor *Executor
	lease    *worklifetime.Lease
	once     sync.Once
	err      error
}

type Executor struct {
	store            CandidateStore
	scope            CandidateScope
	catalog          FinalCatalog
	occurrence       completionOccurrence
	executionContext context.Context
	startGate        chan struct{}
	completionWake   chan struct{}
	chainsDone       sync.WaitGroup
	clock            WakeClock
	retry            RetryPolicy
	pageSize         int
	genericSchedules GenericScheduleWakeupOwner

	mu            sync.Mutex
	chains        map[string]*candidateChain
	started       bool
	ready         bool
	retiring      bool
	pending       int
	pendingZero   chan struct{}
	settlementErr error
}

type completionOccurrence interface {
	worklifetime.Occurrence
	BeginAcceptedDescendant(context.Context) (*worklifetime.Lease, error)
}

func NewExecutor(
	store CandidateStore,
	scope CandidateScope,
	catalog FinalCatalog,
	occurrence worklifetime.Occurrence,
	opts ExecutorOptions,
) (*Executor, error) {
	if store == nil {
		return nil, errors.New("run lifecycle executor requires selected store")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	owner, err := requireExecutorLifetime(scope, occurrence, opts.ExecutionContext)
	if err != nil {
		return nil, err
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
		store: store, scope: scope, catalog: catalog, occurrence: owner,
		executionContext: opts.ExecutionContext, startGate: make(chan struct{}), completionWake: make(chan struct{}, 1),
		clock: opts.Clock, retry: opts.RetryPolicy, pageSize: opts.PageSize, genericSchedules: opts.GenericSchedules,
		chains: make(map[string]*candidateChain),
	}, nil
}

func requireExecutorLifetime(scope CandidateScope, occurrence worklifetime.Occurrence, ctx context.Context) (completionOccurrence, error) {
	switch owner := occurrence.(type) {
	case *worklifetime.RuntimeOccurrence:
		if owner == nil || scope.SelectedForkRunID != "" || ctx != nil || owner.Identity().BundleHash != scope.BundleHash {
			return nil, errors.New("normal completion executor requires normal runtime scope")
		}
		return owner, nil
	case *worklifetime.SelectedForkOccurrence:
		if owner == nil || ctx == nil || scope.SelectedForkRunID == "" || owner.Identity().RunID != scope.SelectedForkRunID {
			return nil, errors.New("selected completion executor requires exact selected lifetime")
		}
		authority, present := effects.AuthorityFromContext(ctx)
		source, hasSource := correlation.SourceArtifactFactFromContext(ctx)
		identity := owner.Identity()
		if !present || !hasSource || source.BundleHash() != scope.BundleHash || !authority.Valid() || authority.Kind != effects.AuthoritySelectedContractFork ||
			authority.SelectedFork.ForkRunID != identity.RunID ||
			authority.SelectedFork.ExecutionID != identity.ExecutionID || authority.SelectedFork.Generation != identity.Generation {
			return nil, errors.New("selected completion executor context contradicts its admitted lifetime")
		}
		return owner, nil
	default:
		return nil, errors.New("completion executor requires normal or selected runtime lifetime")
	}
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
	close(e.startGate)
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
	if e.executionContext != nil {
		ctx = e.executionContext
	}
	lease, err := e.occurrence.BeginAcceptedDescendant(context.WithoutCancel(ctx))
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
	if !e.scope.MatchesCandidate(candidate) {
		return fmt.Errorf("completion candidate does not match executor scope %+v", e.scope)
	}
	e.mu.Lock()
	if e.retiring {
		e.mu.Unlock()
		return worklifetime.ErrRetired
	}
	if current, ok := e.chains[candidate.RunID]; ok {
		err := e.installIntoChainLocked(current, candidate)
		e.mu.Unlock()
		if err != nil {
			return err
		}
		return lease.Done()
	}
	chainCtx, cancel := context.WithCancelCause(lease.Context())
	chain := &candidateChain{
		candidate:              candidate,
		notificationGeneration: 1,
		cancel:                 cancel,
		wake:                   make(chan struct{}, 1),
	}
	e.chains[candidate.RunID] = chain
	e.chainsDone.Add(1)
	e.mu.Unlock()

	go e.runChain(chainCtx, lease, chain)
	return nil
}

func (e *Executor) installIntoChainLocked(chain *candidateChain, candidate Candidate) error {
	authority := chain.candidate
	if chain.successor != nil && chain.successor.Revision > authority.Revision {
		authority = *chain.successor
	}
	if candidate.Revision < authority.Revision {
		return nil
	}
	if candidate.Revision == authority.Revision {
		if !candidate.SameIdentity(authority) {
			return fmt.Errorf(
				"completion candidate conflicts with represented identity for run_id=%s revision=%d",
				candidate.RunID,
				candidate.Revision,
			)
		}
		if chain.successor == nil {
			chain.notificationGeneration++
		}
		return nil
	}
	candidateCopy := candidate
	chain.successor = &candidateCopy
	signalCandidateChain(chain)
	return nil
}

func signalCandidateChain(chain *candidateChain) {
	select {
	case chain.wake <- struct{}{}:
	default:
	}
}

func (e *Executor) runChain(ctx context.Context, lease *worklifetime.Lease, chain *candidateChain) {
	defer func() {
		_ = lease.Done()
		e.chainsDone.Done()
	}()
	select {
	case <-ctx.Done():
		e.removeChain(chain)
		return
	case <-e.startGate:
	}

	attempt := 0
	for {
		candidate, ok := e.currentChainCandidate(ctx, chain)
		if !ok {
			return
		}
		woke, err := e.waitUntilDue(ctx, chain, candidate.DueAt)
		if err != nil {
			e.removeChain(chain)
			return
		}
		if woke {
			attempt = 0
			continue
		}
		candidate, generation, admitted, stopped := e.admitAttempt(ctx, chain)
		if stopped {
			return
		}
		if !admitted {
			attempt = 0
			continue
		}
		// Retirement cancels waits and retries, but an admitted persistence
		// operation must finish before its occurrence lease can settle.
		result, err := e.store.ExecuteCompletionCandidate(context.WithoutCancel(ctx), candidate, e.catalog)
		if errors.Is(err, ErrCompletionAuthority) && !result.Committed {
			e.mu.Lock()
			e.settlementErr = errors.Join(e.settlementErr, err)
			e.mu.Unlock()
			e.removeChain(chain)
			return
		}
		if err != nil && !result.Committed {
			result = CompletionResult{Outcome: OutcomeRetryCurrent, Retryable: err}
		} else if err != nil {
			e.mu.Lock()
			e.settlementErr = errors.Join(e.settlementErr, err)
			e.mu.Unlock()
		}
		if validationErr := result.Validate(); validationErr != nil {
			if result.Committed {
				e.mu.Lock()
				e.settlementErr = errors.Join(e.settlementErr, validationErr)
				e.mu.Unlock()
				e.removeChain(chain)
				return
			}
			result = CompletionResult{Outcome: OutcomeRetryCurrent, Retryable: validationErr}
		}
		if result.Outcome != OutcomeRetryCurrent && !e.reconcileCommittedGenericSchedules(ctx, result.GenericScheduleActivations) {
			e.removeChain(chain)
			return
		}
		if result.Committed && result.Outcome == OutcomeTerminallyEligible {
			select {
			case e.completionWake <- struct{}{}:
			default:
			}
		}
		action := e.finishAttempt(ctx, chain, candidate, generation, result)
		switch action {
		case candidateChainRetry:
			delay := e.retry.Delay(attempt)
			attempt++
			woke, err := e.wait(ctx, chain, delay)
			if err != nil {
				e.removeChain(chain)
				return
			}
			if woke {
				attempt = 0
			}
		case candidateChainContinue:
			attempt = 0
		case candidateChainStop:
			return
		default:
			return
		}
	}
}

func (e *Executor) reconcileCommittedGenericSchedules(ctx context.Context, activations []CommittedGenericScheduleActivation) bool {
	for _, activation := range activations {
		attempt := 0
		for {
			if e.genericSchedules != nil {
				queued, err := e.genericSchedules.ReconcileWakeupWithRecovery(context.WithoutCancel(ctx), activation.ID())
				if err == nil || queued {
					break
				}
			}
			delay := e.retry.Delay(attempt)
			attempt++
			select {
			case <-ctx.Done():
				return false
			case <-e.clock.After(delay):
			}
		}
	}
	return true
}

func (e *Executor) currentChainCandidate(ctx context.Context, chain *candidateChain) (Candidate, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current, ok := e.chains[chain.candidate.RunID]
	if !ok || current != chain || e.retiring || ctx.Err() != nil {
		if ok && current == chain {
			delete(e.chains, chain.candidate.RunID)
		}
		return Candidate{}, false
	}
	e.promoteSuccessorLocked(chain)
	return chain.candidate, true
}

func (e *Executor) admitAttempt(ctx context.Context, chain *candidateChain) (Candidate, uint64, bool, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current, ok := e.chains[chain.candidate.RunID]
	if !ok || current != chain || e.retiring || ctx.Err() != nil {
		if ok && current == chain {
			delete(e.chains, chain.candidate.RunID)
		}
		return Candidate{}, 0, false, true
	}
	if chain.successor != nil {
		e.promoteSuccessorLocked(chain)
		return Candidate{}, 0, false, false
	}
	return chain.candidate, chain.notificationGeneration, true, false
}

func (e *Executor) finishAttempt(
	ctx context.Context,
	chain *candidateChain,
	candidate Candidate,
	generation uint64,
	result CompletionResult,
) candidateChainAction {
	e.mu.Lock()
	defer e.mu.Unlock()
	current, ok := e.chains[candidate.RunID]
	if !ok || current != chain {
		return candidateChainStop
	}
	// The admitted attempt may settle during retirement, but its result does
	// not retain authority to start a successor attempt.
	if e.retiring || ctx.Err() != nil {
		delete(e.chains, candidate.RunID)
		return candidateChainStop
	}
	if result.Outcome == OutcomeRearmAt {
		if err := e.mergeRearmLocked(chain, result.Candidate); err != nil {
			result = CompletionResult{Outcome: OutcomeRetryCurrent, Retryable: err}
		}
	}
	if chain.successor != nil {
		e.promoteSuccessorLocked(chain)
		return candidateChainContinue
	}
	if chain.notificationGeneration > generation {
		return candidateChainContinue
	}
	switch result.Outcome {
	case OutcomeRetryCurrent:
		return candidateChainRetry
	case OutcomeRearmAt:
		return candidateChainContinue
	case OutcomeTerminallyEligible, OutcomeAwaitMutation, OutcomeExactNoop:
		delete(e.chains, candidate.RunID)
		return candidateChainStop
	default:
		delete(e.chains, candidate.RunID)
		return candidateChainStop
	}
}

func (e *Executor) mergeRearmLocked(chain *candidateChain, candidate Candidate) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	if candidate.RunID != chain.candidate.RunID || candidate.BundleHash != e.scope.BundleHash {
		return errors.New("rearmed completion candidate does not match its executor run and bundle scope")
	}
	if candidate.Revision < chain.candidate.Revision {
		return errors.New("rearmed completion candidate revision moved backwards")
	}
	if candidate.Revision == chain.candidate.Revision {
		// The selected store owns rearm coordinates. Unlike an external
		// handoff, its admitted result may replace the due coordinate without
		// advancing the durable revision.
		chain.candidate = candidate
		return nil
	}
	if chain.successor != nil {
		if candidate.Revision < chain.successor.Revision {
			return nil
		}
		if candidate.Revision == chain.successor.Revision && !candidate.SameIdentity(*chain.successor) {
			return errors.New("rearmed completion candidate conflicts with pending successor identity")
		}
	}
	candidateCopy := candidate
	chain.successor = &candidateCopy
	return nil
}

func (e *Executor) promoteSuccessorLocked(chain *candidateChain) {
	if chain.successor == nil {
		return
	}
	chain.candidate = *chain.successor
	chain.successor = nil
	chain.notificationGeneration = 1
	select {
	case <-chain.wake:
	default:
	}
}

func (e *Executor) removeChain(chain *candidateChain) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current, ok := e.chains[chain.candidate.RunID]
	if ok && current == chain {
		delete(e.chains, chain.candidate.RunID)
	}
}

func (e *Executor) waitUntilDue(ctx context.Context, chain *candidateChain, dueAt time.Time) (bool, error) {
	delay := dueAt.Sub(e.clock.Now())
	if delay <= 0 {
		return false, nil
	}
	return e.wait(ctx, chain, delay)
}

func (e *Executor) wait(ctx context.Context, chain *candidateChain, delay time.Duration) (bool, error) {
	if delay <= 0 {
		return false, nil
	}
	select {
	case <-ctx.Done():
		return false, context.Cause(ctx)
	case <-chain.wake:
		return true, nil
	case <-e.clock.After(delay):
		return false, nil
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
	chains := make([]*candidateChain, 0, len(e.chains))
	for _, chain := range e.chains {
		chains = append(chains, chain)
	}
	e.mu.Unlock()
	for _, chain := range chains {
		chain.cancel(worklifetime.ErrRetired)
	}
	if pendingZero == nil {
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.settlementErr
	}
	select {
	case <-pendingZero:
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.settlementErr
	case <-ctx.Done():
		e.mu.Lock()
		defer e.mu.Unlock()
		return errors.Join(context.Cause(ctx), e.settlementErr)
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

func (e *Executor) CompletionWakeups() <-chan struct{} {
	return e.completionWake
}

type completionJoinDiagnostic struct {
	cause error
}

func (e *completionJoinDiagnostic) Error() string { return e.cause.Error() }
func (e *completionJoinDiagnostic) Unwrap() error { return e.cause }

// CompletionJoinSucceeded classifies only the owner's exact Wait result, not
// an independently wrapped or joined error that may include incomplete work.
func CompletionJoinSucceeded(err error) bool {
	if err == nil {
		return true
	}
	_, joined := err.(*completionJoinDiagnostic)
	return joined
}

func (e *Executor) Wait(ctx context.Context) error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	retiring := e.retiring
	e.mu.Unlock()
	if !retiring {
		return errors.New("completion executor must retire before joining")
	}
	done := make(chan struct{})
	go func() {
		e.chainsDone.Wait()
		close(done)
	}()
	select {
	case <-done:
		e.mu.Lock()
		diagnostic := e.settlementErr
		e.mu.Unlock()
		if diagnostic != nil {
			return &completionJoinDiagnostic{cause: diagnostic}
		}
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
