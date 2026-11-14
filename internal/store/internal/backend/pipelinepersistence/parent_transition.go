package pipelinepersistence

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// Run-scoped admission lives beside the existing selected-store claim registry.
// Membership retires with each claim; no transaction or registry mutex is held
// while draining.
type pipelineRecoveryTransitions struct {
	mu   sync.Mutex
	runs map[string]*pipelineRecoveryRun
}

type pipelineRecoveryRun struct {
	claims, publications, transitions, forks int
	drained                                  chan struct{}
	allDrained                               chan struct{}
}

type pipelineParentTransition struct {
	once sync.Once
	done func()
}

func (t *pipelineParentTransition) Done() { t.once.Do(t.done) }

func (r *pipelineRecoveryTransitions) runLocked(runID string) *pipelineRecoveryRun {
	if r.runs == nil {
		r.runs = make(map[string]*pipelineRecoveryRun)
	}
	state := r.runs[runID]
	if state == nil {
		state = &pipelineRecoveryRun{drained: make(chan struct{}), allDrained: make(chan struct{})}
		close(state.drained)
		close(state.allDrained)
		r.runs[runID] = state
	}
	return state
}

func (r *pipelineRecoveryTransitions) reserve(runID string) (func(), bool) {
	if runID == "" {
		return func() {}, true
	}
	r.mu.Lock()
	state := r.runLocked(runID)
	if state.transitions != 0 || state.forks != 0 {
		r.mu.Unlock()
		return nil, false
	}
	if state.claims == 0 {
		state.drained = make(chan struct{})
	}
	if state.claims+state.publications == 0 {
		state.allDrained = make(chan struct{})
	}
	state.claims++
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			state.claims--
			if state.claims == 0 {
				close(state.drained)
			}
			if state.claims+state.publications == 0 {
				close(state.allDrained)
				if state.transitions == 0 && state.forks == 0 {
					delete(r.runs, runID)
				}
			}
		})
	}, true
}

func (r *pipelineRecoveryTransitions) reservePublication(runID string, accepted bool) (func(), bool) {
	if runID == "" {
		return func() {}, true
	}
	r.mu.Lock()
	state := r.runLocked(runID)
	if state.forks != 0 && !accepted {
		r.mu.Unlock()
		return nil, false
	}
	if state.claims+state.publications == 0 {
		state.allDrained = make(chan struct{})
	}
	state.publications++
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			state.publications--
			if state.claims+state.publications == 0 {
				close(state.allDrained)
				if state.transitions == 0 && state.forks == 0 {
					delete(r.runs, runID)
				}
			}
		})
	}, true
}

func (r *pipelineRecoveryTransitions) begin(ctx context.Context, runID string) (pipelineobligation.ParentTransition, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, errors.New("pipeline parent transition requires a run ID")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	state := r.runLocked(runID)
	state.transitions++
	drained := state.drained
	r.mu.Unlock()
	transition := &pipelineParentTransition{done: func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		state.transitions--
		if state.claims+state.publications == 0 && state.transitions == 0 && state.forks == 0 {
			delete(r.runs, runID)
		}
	}}
	select {
	case <-drained:
		if err := ctx.Err(); err != nil {
			transition.Done()
			return nil, err
		}
		return transition, nil
	case <-ctx.Done():
		transition.Done()
		return nil, ctx.Err()
	}
}

func (r *pipelineRecoveryTransitions) beginFork(ctx context.Context, runID string) (pipelineobligation.ParentTransition, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, errors.New("pipeline source fork transition requires a run ID")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	state := r.runLocked(runID)
	state.forks++
	drained := state.allDrained
	r.mu.Unlock()
	transition := &pipelineParentTransition{done: func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		state.forks--
		if state.claims+state.publications == 0 && state.transitions == 0 && state.forks == 0 {
			delete(r.runs, runID)
		}
	}}
	select {
	case <-drained:
		if err := ctx.Err(); err != nil {
			transition.Done()
			return nil, err
		}
		return transition, nil
	case <-ctx.Done():
		transition.Done()
		return nil, ctx.Err()
	}
}

func (s *postgresPipelineObligationStore) BeginParentTransition(ctx context.Context, runID string) (pipelineobligation.ParentTransition, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.postgresPipelineClaims().recoveryTransitions.begin(ctx, runID)
}

func (s *sqlitePipelineObligationStore) BeginParentTransition(ctx context.Context, runID string) (pipelineobligation.ParentTransition, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.recoveryTransitions.begin(ctx, runID)
}

func (s *postgresPipelineObligationStore) BeginSourceForkTransition(ctx context.Context, runID string) (pipelineobligation.ParentTransition, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.postgresPipelineClaims().recoveryTransitions.beginFork(ctx, runID)
}

func (s *sqlitePipelineObligationStore) BeginSourceForkTransition(ctx context.Context, runID string) (pipelineobligation.ParentTransition, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.recoveryTransitions.beginFork(ctx, runID)
}

func (s *postgresPipelineObligationStore) retainRecoveryTransition(claim pipelineobligation.Claim, done func()) error {
	r := s.postgresPipelineClaims()
	token, err := r.issuer.Token(claim)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.claims[token]
	if state == nil {
		return pipelineobligation.ErrStaleClaim
	}
	state.recoveryDone = done
	return nil
}

func (s *sqlitePipelineObligationStore) retainRecoveryTransition(claim pipelineobligation.Claim, done func()) error {
	s.pipelineClaimMu.Lock()
	defer s.pipelineClaimMu.Unlock()
	issuer, claims := s.pipelineClaimOwner()
	token, err := issuer.Token(claim)
	if err != nil {
		return err
	}
	state := claims[token]
	if state == nil {
		return pipelineobligation.ErrStaleClaim
	}
	state.recoveryDone = done
	return nil
}
