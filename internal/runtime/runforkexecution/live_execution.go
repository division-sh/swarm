package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// Preparation must refuse before predecessor retirement. Once execution starts,
// its existing owner handles preparation disposal and transaction revalidation.
func executePreparedLiveFork(ctx context.Context, req SelectedContractExecutionRequest, prepared *PreparedSelectedFork) (out SelectedContractExecutionResult, finalErr error) {
	executed := false
	defer func() {
		if !executed {
			finalErr = errors.Join(finalErr, req.Owner.completePreparation(prepared))
		}
	}()
	execute := func(context.Context) error {
		executed = true
		out, finalErr = executePreparedSelectedContractRunFork(ctx, req, prepared)
		return finalErr
	}
	if req.LiveRuns == nil || req.LiveContexts == nil {
		return out, errors.New("live fork requires its durable flow selector and runtime contexts")
	}
	scope, err := req.LiveRuns.LoadRunLiveScope(ctx, prepared.plan.SourceRunID)
	if err != nil {
		return out, err
	}
	if err := scope.Validate(); err != nil {
		return out, err
	}
	live, found, err := req.LiveRuns.LoadLiveRun(ctx, scope)
	if err != nil {
		return out, err
	}
	if found {
		use, lookup, availability, err := req.LiveContexts.AcquireRun(ctx, live.RunID)
		if err != nil {
			if use != nil {
				err = errors.Join(err, use.Done())
			}
			return out, err
		}
		if !lookup.Loaded() || !availability.Available() || use == nil || use.Runtime() == nil || !use.Context.SourceArtifactFact.Matches(live.Source) {
			if use != nil {
				finalErr = use.Done()
			}
			return out, errors.Join(finalErr, errors.New("live predecessor requires its exact admitted runtime"))
		}
		parked, parkCtx := use.Runtime(), use.WorkContext()
		if live.Source.Matches(prepared.loadedSource.SourceArtifactFact) {
			req.LiveRuntime = parked
			finalErr = parked.WithParkedLiveRun(parkCtx, live, execute)
			if !out.Activation.Activated {
				finalErr = errors.Join(finalErr, parked.ReconcileLiveRun(context.WithoutCancel(parkCtx), live.RunID))
			}
			finalErr = errors.Join(finalErr, use.Done())
		} else {
			pauseCtx, release, err := parked.ParkLiveRun(parkCtx, live)
			if err != nil {
				return out, errors.Join(err, use.Done())
			}
			releaseUse := sync.OnceValue(func() error { release(); return use.Done() })
			defer func() { finalErr = errors.Join(finalErr, releaseUse()) }()
			acquire := req.AcquireLiveRuntime
			paused := false
			req.AcquireLiveRuntime = func(ctx context.Context, hash string) (*runtimepkg.RuntimeContextUse, error) {
				if acquire == nil || parked.RunControl == nil {
					return nil, errors.New("bundle swap requires its existing runtime and run-control owners")
				}
				// Materialization has revalidated the immutable plan, and selected
				// dispatch has settled. Only now change its observed source status.
				result, err := parked.RunControl.Pause(pauseCtx, runcontrol.TransitionRequest{RunID: live.RunID, Reason: "fork bundle swap", ControlledBy: "run.fork"})
				paused = result.Status == runcontrol.StatusPaused
				if err != nil {
					return nil, err
				}
				if err := releaseUse(); err != nil {
					return nil, err
				}
				return acquire(ctx, hash)
			}
			finalErr = execute(ctx)
			finalErr = errors.Join(finalErr, releaseUse())
			if !paused && !out.Activation.Activated {
				finalErr = errors.Join(finalErr, parked.ReconcileLiveRun(context.WithoutCancel(parkCtx), live.RunID))
			}
		}
	} else {
		finalErr = execute(ctx)
	}
	if out.Activation.Activated {
		use, lookup, _, err := req.LiveContexts.AcquireRun(context.WithoutCancel(ctx), out.Materialization.ForkRunID)
		if err == nil && (use == nil || !lookup.Loaded()) {
			err = errors.New("committed live fork requires its current ordinary runtime")
		}
		if err == nil {
			err = use.Runtime().ReconcileLiveRun(use.WorkContext(), out.Materialization.ForkRunID)
		}
		if use != nil {
			err = errors.Join(err, use.Done())
		}
		finalErr = errors.Join(finalErr, err)
	}
	return out, finalErr
}

func (c selectedContractForkLocalRuntimeContainer) activatePrepared(p *PreparedSelectedFork, req SelectedContractExecutionRequest, result SelectedContractExecutionResult, diagnostic error) (SelectedContractExecutionResult, error) {
	c.diagnostics.add(diagnostic)
	if req.LiveRuntime == nil && req.AcquireLiveRuntime == nil {
		return result, errors.New("fork activation requires its ordinarily admitted live target")
	}
	return c.activateLive(p, req, result)
}

// Dispatch remains selected until canonical publication settlement. The native
// activation then checks the exact receipts and parks/transfers in one attempt.
func (c selectedContractForkLocalRuntimeContainer) activateLive(p *PreparedSelectedFork, req SelectedContractExecutionRequest, result SelectedContractExecutionResult) (out SelectedContractExecutionResult, finalErr error) {
	out = result
	runtime := c.attachment.runtime
	if err := runtime.startExecution(c.attachment.ctx, c.attachment.pipeline, c.diagnostics, p.recoveryFromExecutionID != ""); err != nil {
		return out, err
	}
	inputs, err := c.commitSelectedInputs()
	defer func() {
		for _, input := range inputs {
			finalErr = errors.Join(finalErr, runtime.bus.AbandonPreparedPublish(context.WithoutCancel(input.ctx), input.prepared))
		}
	}()
	if err != nil {
		return out, err
	}
	for _, input := range inputs {
		if err := c.dispatchCommittedInput(input); err != nil {
			return out, err
		}
		out.ExecutedEventCount++
	}
	// Stop actors and join accepted work while their selected grant still exists.
	// Shutdown retires that grant and therefore belongs after the native handoff.
	if err := runtime.stopExecutionWork(); err != nil {
		return out, err
	}
	if req.LiveRuntime == nil && req.AcquireLiveRuntime != nil {
		use, err := req.AcquireLiveRuntime(p.operation.Context(), p.loadedSource.SourceArtifactFact.BundleHash())
		if use != nil {
			defer func() { finalErr = errors.Join(finalErr, use.Done()) }()
		}
		if err != nil {
			return out, err
		}
		if use == nil || use.Runtime() == nil || !use.Context.SourceArtifactFact.Matches(p.loadedSource.SourceArtifactFact) {
			return out, errors.New("live fork requires its exact ordinarily admitted target")
		}
		req.LiveRuntime = use.Runtime()
	}
	evidence, err := req.LiveRuntime.CurrentStartupGrantEvidence()
	if err != nil {
		return out, err
	}
	ctx := startupownership.WithGenerationGrantEvidence(p.operation.Context(), evidence)
	ctx = effects.WithAuthority(ctx, c.authority)
	activation, err := c.ports.fork.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		InheritedSchedules: p.readiness, ForkOperation: p.forkOperation, DataPins: result.Materialization.DataPins,
		ExecutionSource: p.loadedSource.Source, ForkRunID: result.Materialization.ForkRunID,
		AllowedSourceEventIDs: selectedContractExecutionFrontierEventIDs(p.frontier.FrontierEvents),
		FrontierAdmission:     p.frontier, RouteTopology: p.routeTopology, RecipientPlanning: *p.model.RecipientPlanning,
	})
	out.Activation = activation
	if !out.Activation.Activated {
		return out, errors.Join(err, fmt.Errorf("live handoff was not acknowledged; selected work remains recoverable"))
	}
	// Post-commit reconciliation can fail without undoing the immutable ACK.
	return out, err
}
