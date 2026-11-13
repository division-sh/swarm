package serveapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type continuedRunLoad struct {
	id      string
	phase   string
	failure *failures.Envelope
	stage   string
	detail  string
}

// A canceled request must not wait behind a process-owned target startup.
// The waiter gives the existing lifecycle mutex back if cancellation wins.
func lockContinueOperation(ctx context.Context, mu *sync.Mutex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mu.TryLock() {
		if err := ctx.Err(); err != nil {
			mu.Unlock()
			return err
		}
		return nil
	}
	acquired := make(chan struct{})
	go func() {
		mu.Lock()
		select {
		case acquired <- struct{}{}:
		case <-ctx.Done():
			mu.Unlock()
		}
	}()
	select {
	case <-acquired:
		if err := ctx.Err(); err != nil {
			mu.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// The native transition is the request's commit point. Target construction and
// queued-work recovery then belong to the accepted process occurrence.
func (s *processLifecycleSupervisor) continueRunRuntime(ctx context.Context, req runcontrol.TransitionRequest) (runcontrol.TransitionResult, error) {
	if s == nil {
		return runcontrol.TransitionResult{}, errors.New("run continue requires process lifecycle ownership")
	}
	if err := lockContinueOperation(ctx, &s.operationMu); err != nil {
		return runcontrol.TransitionResult{}, err
	}
	defer s.operationMu.Unlock()
	s.mu.RLock()
	manager, liveRuns := s.runtimeContexts, s.selectedRecoveryEnvironment.LiveRuns
	owner, current, stopping := s.selectedProcess, s.currentRT, s.stopping
	var processCtx context.Context
	if len(s.resetRequests) > 0 {
		processCtx = s.resetRequests[0].Ctx
	}
	s.mu.RUnlock()
	if manager == nil || liveRuns == nil || owner == nil || current == nil || current.RunControl == nil || processCtx == nil || stopping {
		return runcontrol.TransitionResult{}, errors.New("run continue requires its live process and selected store")
	}
	if err := processCtx.Err(); err != nil {
		return runcontrol.TransitionResult{}, err
	}
	scope, err := liveRuns.LoadRunLiveScope(ctx, req.RunID)
	if err != nil {
		return runcontrol.TransitionResult{}, err
	}
	if !scope.Empty() {
		live, found, err := liveRuns.LoadLiveRun(ctx, scope)
		if err != nil {
			return runcontrol.TransitionResult{}, err
		}
		if found && live.RunID != req.RunID {
			return runcontrol.TransitionResult{}, &runlifecycle.LiveRunConflictError{Scope: scope, LiveRunID: live.RunID}
		}
	}
	_, availability, err := manager.LookupRunStatus(ctx, req.RunID)
	if err != nil {
		return runcontrol.TransitionResult{}, err
	}
	if !availability.Available() {
		return runcontrol.TransitionResult{}, fmt.Errorf("continue requires its retained source artifact: %s", availability.DetailString())
	}
	currentSources, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil || !exists {
		return runcontrol.TransitionResult{}, errors.Join(err, errors.New("continue requires its committed source set"))
	}
	lease, err := owner.Begin(processCtx)
	if err != nil {
		return runcontrol.TransitionResult{}, err
	}
	if err := lease.Context().Err(); err != nil {
		return runcontrol.TransitionResult{}, errors.Join(err, lease.Done())
	}
	result, err := current.RunControl.ContinueDeferred(ctx, req)
	if err != nil {
		if errors.Is(err, runcontrol.ErrNotPaused) {
			return s.recheckPausedRunHandlersLocked(ctx, lease, req.RunID, availability.RootReference, currentSources.Revision)
		}
		return result, errors.Join(err, lease.Done())
	}
	id := uuid.NewString()
	s.mu.Lock()
	if s.continuedRuns == nil {
		s.continuedRuns = make(map[string]continuedRunLoad)
	}
	s.continuedRuns[req.RunID] = continuedRunLoad{id: id, phase: "starting"}
	s.mu.Unlock()
	go s.finishContinuedRun(lease, req.RunID, availability.RootReference, currentSources.Revision, id)
	return result, nil
}

func (s *processLifecycleSupervisor) recheckPausedRunHandlersLocked(ctx context.Context, lease *worklifetime.Lease, runID string, reference sourceartifact.RootAdmissionReference, revision string) (result runcontrol.TransitionResult, err error) {
	defer func() { err = errors.Join(err, lease.Done()) }()
	if err := s.validateContinuedRunTarget(ctx, runID, reference, revision); err != nil {
		return result, err
	}
	use, err := s.acquireLiveTargetRuntimeLocked(ctx, reference)
	if err != nil {
		return result, err
	}
	if use == nil {
		return result, errors.New("handler resume did not acquire its exact runtime")
	}
	defer func() { err = errors.Join(err, use.Done()) }()
	if use.Runtime() == nil || use.Runtime().RunControl == nil {
		return result, errors.New("handler resume runtime has no control owner")
	}
	workCtx, cancel := context.WithCancel(use.WorkContext())
	defer cancel()
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(joined); cancel() })
	defer func() {
		if !stop() {
			<-joined
		}
	}()
	return use.Runtime().RunControl.RecheckPausedFanOut(workCtx, runID)
}

func (s *processLifecycleSupervisor) finishContinuedRun(lease *worklifetime.Lease, runID string, reference sourceartifact.RootAdmissionReference, revision, id string) {
	var use *runtime.RuntimeContextUse
	var err error
	stage := "validate_target"
	defer func() {
		if use != nil {
			err = errors.Join(err, use.Done())
		}
		err = errors.Join(err, lease.Done())
		s.mu.Lock()
		if state, ok := s.continuedRuns[runID]; ok && state.id == id {
			state.phase = "ready"
			if err != nil {
				state.phase = "failed"
				failure, detail := continuedRunFailure(err, stage)
				state.failure = &failure
				state.stage = stage
				state.detail = detail
			}
			s.continuedRuns[runID] = state
		}
		s.mu.Unlock()
	}()
	s.operationMu.Lock()
	err = s.validateContinuedRunTarget(lease.Context(), runID, reference, revision)
	if err == nil {
		stage = "load_target"
		use, err = s.acquireLiveTargetRuntimeLocked(lease.Context(), reference)
	}
	s.operationMu.Unlock()
	if err != nil {
		return
	}
	if use == nil {
		err = errors.New("continued run did not acquire its loaded runtime")
		return
	}
	if use.Runtime() == nil || use.Runtime().RunControl == nil {
		stage = "acquire_recovery"
		err = errors.New("continued run has no loaded recovery owner")
		return
	}
	stage = "recover_work"
	recovery := use.Runtime().RunControl.RecoverContinuedRun(use.WorkContext(), runID)
	err = recovery.Err
}

// The lifecycle operation lock is held while checking the exact committed
// run/source selection and while acquiring its runtime.
func (s *processLifecycleSupervisor) validateContinuedRunTarget(ctx context.Context, runID string, reference sourceartifact.RootAdmissionReference, revision string) error {
	manager, liveRuns := s.runtimeContexts, s.selectedRecoveryEnvironment.LiveRuns
	if s.stopping || manager == nil || liveRuns == nil {
		return errors.New("continued run lost its live process")
	}
	_, observed, err := manager.LookupRunStatus(ctx, runID)
	if err != nil {
		return err
	}
	if (observed.Status != string(runlifecycle.StateRunning) && observed.Status != string(runlifecycle.StateLive)) || observed.RootReference != reference {
		return errors.New("continued run was superseded before target startup")
	}
	scope, err := liveRuns.LoadRunLiveScope(ctx, runID)
	if err != nil {
		return err
	}
	if !scope.Empty() {
		live, found, err := liveRuns.LoadLiveRun(ctx, scope)
		if err != nil {
			return err
		}
		if !found || live.RunID != runID {
			return errors.New("continued run lost its exact live selection")
		}
	}
	current, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil || !exists {
		return errors.Join(err, errors.New("continued run lost its source set"))
	}
	if current.Revision != revision && !sourceSetContainsRoot(current, reference) {
		return errors.New("continued run source set was superseded")
	}
	return nil
}

func sourceSetContainsRoot(plan agenttopology.SourceSetPlan, reference sourceartifact.RootAdmissionReference) bool {
	for _, source := range plan.Sources {
		if source.RootReference() == reference {
			return true
		}
	}
	return false
}

func (s *processLifecycleSupervisor) continuedRunReadiness(_ context.Context, runID string) (*apiv1.RunRuntimeReadiness, error) {
	if s == nil {
		return nil, errors.New("run runtime readiness requires process lifecycle ownership")
	}
	s.mu.RLock()
	state, tracked := s.continuedRuns[runID]
	s.mu.RUnlock()
	if tracked {
		return &apiv1.RunRuntimeReadiness{Ready: state.phase == "ready", Reason: "runtime_" + state.phase, Failure: state.failure, Stage: state.stage, Detail: state.detail}, nil
	}
	return nil, nil
}

// Only simple diagnostic prose is exported. Structured values, paths and
// credential-shaped text remain in the private error chain, never in RPC.
func continuedRunFailureDetail(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	if len(message) == 0 || len(message) > 96 {
		return "details unavailable"
	}
	// Unknown words may contain a credential, even without a sensitive label.
	for _, word := range strings.Fields(strings.ToLower(message)) {
		switch word {
		case "target", "startup", "source", "runtime", "load", "construction", "recovery", "unavailable", "failed", "cancelled", "deadline", "expired", "refused", "superseded", "missing", "closed", "stopped":
		default:
			return "details unavailable"
		}
	}
	for _, char := range message {
		if char != ' ' && (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') {
			return "details unavailable"
		}
	}
	return message
}

func continuedRunFailure(err error, stage string) (failures.Envelope, string) {
	if _, typed := failures.As(err); !typed {
		var class failures.Class
		var code, detail string
		switch continuedRunContextCause(err) {
		case context.DeadlineExceeded:
			class, code, detail = failures.ClassTimeout, "continued_run_load_timeout", "load timeout"
		case context.Canceled:
			class, code, detail = failures.ClassLifecycleConflict, "continued_run_load_cancelled", "load cancelled"
		}
		if code != "" {
			cause := failures.Wrap(class, code, "runtime_context", "continue_load", map[string]any{"stage": stage}, err)
			return failures.FromError(cause, "runtime_context", "continue_load").Failure, detail
		}
	}
	return failures.FromError(err, "runtime_context", "continue_load").Failure, continuedRunFailureDetail(err)
}

// A joined independent failure is not reclassified as mere cancellation.
func continuedRunContextCause(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var cause error
		for _, child := range joined.Unwrap() {
			found := continuedRunContextCause(child)
			if found == nil || (cause != nil && found != cause) {
				return nil
			}
			cause = found
		}
		return cause
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return continuedRunContextCause(wrapped.Unwrap())
	}
	if err == context.DeadlineExceeded || err == context.Canceled {
		return err
	}
	return nil
}
