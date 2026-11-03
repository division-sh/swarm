package serveapp

import (
	"context"
	"errors"
	"fmt"

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
}

// The native transition is the request's commit point. Target construction and
// queued-work recovery then belong to the accepted process occurrence.
func (s *processLifecycleSupervisor) continueRunRuntime(ctx context.Context, req runcontrol.TransitionRequest) (runcontrol.TransitionResult, error) {
	if s == nil {
		return runcontrol.TransitionResult{}, errors.New("run continue requires process lifecycle ownership")
	}
	s.operationMu.Lock()
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

func (s *processLifecycleSupervisor) finishContinuedRun(lease *worklifetime.Lease, runID string, reference sourceartifact.RootAdmissionReference, revision, id string) {
	var use *runtime.RuntimeContextUse
	var err error
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
				failure := failures.FromError(err, "runtime_context", "continue_load").Failure
				state.failure = &failure
			}
			s.continuedRuns[runID] = state
		}
		s.mu.Unlock()
	}()
	s.operationMu.Lock()
	manager, liveRuns := s.runtimeContexts, s.selectedRecoveryEnvironment.LiveRuns
	if s.stopping || manager == nil || liveRuns == nil {
		err = errors.New("continued run lost its live process")
	} else {
		var availability struct {
			status string
			root   sourceartifact.RootAdmissionReference
		}
		_, observed, readErr := manager.LookupRunStatus(lease.Context(), runID)
		if readErr != nil {
			err = readErr
		} else {
			availability.status, availability.root = observed.Status, observed.RootReference
			if (availability.status != string(runlifecycle.StateRunning) && availability.status != string(runlifecycle.StateLive)) || availability.root != reference {
				err = errors.New("continued run was superseded before target startup")
			}
		}
		if err == nil {
			scope, scopeErr := liveRuns.LoadRunLiveScope(lease.Context(), runID)
			if scopeErr != nil {
				err = scopeErr
			} else if !scope.Empty() {
				live, found, liveErr := liveRuns.LoadLiveRun(lease.Context(), scope)
				if liveErr != nil {
					err = liveErr
				} else if !found || live.RunID != runID {
					err = errors.New("continued run lost its exact live selection")
				}
			}
		}
		if err == nil {
			current, exists, readErr := s.processCapability.CurrentSourceSet(lease.Context())
			if readErr != nil || !exists {
				err = errors.Join(readErr, errors.New("continued run lost its source set"))
			} else if current.Revision != revision && !sourceSetContainsRoot(current, reference) {
				err = errors.New("continued run source set was superseded")
			}
		}
		if err == nil {
			use, err = s.acquireLiveTargetRuntimeLocked(lease.Context(), reference)
		}
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
		err = errors.New("continued run has no loaded recovery owner")
		return
	}
	recovery := use.Runtime().RunControl.RecoverContinuedRun(use.WorkContext(), runID)
	err = recovery.Err
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
		return &apiv1.RunRuntimeReadiness{Ready: state.phase == "ready", Reason: "runtime_" + state.phase, Failure: state.failure}, nil
	}
	return nil, nil
}
