package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
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
		releaseSelection := req.LiveContexts.BeginLiveRunSelection()
		defer releaseSelection()
		req.liveSelectionHeld = true
		live, found, err = req.LiveRuns.LoadLiveRun(ctx, scope)
		if err != nil {
			return out, err
		}
	}
	if found {
		use, err := acquireLivePredecessor(ctx, req.LiveContexts, live)
		if err != nil {
			return out, err
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
				use, sourcePaused, err := pauseLiveSourceAndAcquire(ctx, pauseCtx, parked, live.RunID, hash, releaseUse, acquire)
				paused = sourcePaused
				return use, err
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
		if !req.liveSelectionHeld {
			releaseSelection := req.LiveContexts.BeginLiveRunSelection()
			defer releaseSelection()
		}
		finalErr = errors.Join(finalErr, reconcileCommittedLiveFork(context.WithoutCancel(ctx), req.LiveContexts, out.Materialization.ForkRunID))
	}
	return out, finalErr
}

func pauseLiveSourceAndAcquire(ctx, pauseCtx context.Context, parked *runtimepkg.Runtime, runID, hash string, release func() error, acquire func(context.Context, string) (*runtimepkg.RuntimeContextUse, error)) (*runtimepkg.RuntimeContextUse, bool, error) {
	if acquire == nil || parked.RunControl == nil {
		return nil, false, errors.New("bundle swap requires its existing runtime and run-control owners")
	}
	// Materialization and selected dispatch have settled before this callback.
	result, err := parked.RunControl.Pause(pauseCtx, runcontrol.TransitionRequest{RunID: runID, Reason: "fork bundle swap", ControlledBy: "run.fork"})
	paused := result.Status == runcontrol.StatusPaused
	if err != nil {
		return nil, paused, err
	}
	if err := release(); err != nil {
		return nil, paused, err
	}
	use, err := acquire(ctx, hash)
	return use, paused, err
}

func acquireLivePredecessor(ctx context.Context, contexts *runtimepkg.RuntimeContextManager, live runlifecycle.LiveRun) (*runtimepkg.RuntimeContextUse, error) {
	use, lookup, availability, err := contexts.AcquireRun(ctx, live.RunID)
	if err != nil {
		if use != nil {
			err = errors.Join(err, use.Done())
		}
		return nil, err
	}
	if !lookup.Loaded() || !availability.Available() || use == nil || use.Runtime() == nil || !use.Context.SourceArtifactFact.Matches(live.Source) {
		var cleanupErr error
		if use != nil {
			cleanupErr = use.Done()
		}
		return nil, errors.Join(cleanupErr, errors.New("live predecessor requires its exact admitted runtime"))
	}
	return use, nil
}

func reconcileCommittedLiveFork(ctx context.Context, contexts *runtimepkg.RuntimeContextManager, runID string) error {
	use, lookup, _, err := contexts.AcquireRun(ctx, runID)
	if err == nil && (use == nil || !lookup.Loaded()) {
		err = errors.New("committed live fork requires its current ordinary runtime")
	}
	if err == nil {
		err = use.Runtime().ReconcileAcknowledgedLiveFork(use.WorkContext(), runID)
	}
	if use != nil {
		err = errors.Join(err, use.Done())
	}
	return err
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
	continuations, err := deliverycontinuation.NewSelected(c.ports.busDurable.DeliveryLifecycle, c.attachment.deliveryAuthority, p.operation.selected, runtime.bus, func(_ context.Context, cause error) { c.attachment.cancel(cause) })
	if err != nil {
		return out, err
	}
	if err := runtime.bus.SetDeliveryContinuationOwner(continuations); err != nil {
		return out, err
	}
	if err := continuations.Start(c.attachment.ctx); err != nil {
		return out, err
	}
	retireContinuations := sync.OnceValue(func() error { return continuations.Retire(context.WithoutCancel(c.attachment.ctx)) })
	defer func() { finalErr = errors.Join(finalErr, retireContinuations()) }()
	recovered := p.recoveryFromExecutionID != ""
	if err := c.restoreLiveSelectedWork(continuations, recovered); err != nil {
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
	if !recovered {
		if err := c.startSelectedServing(c.attachment.ctx); err != nil {
			return out, err
		}
	}
	for _, input := range inputs {
		if err := c.dispatchCommittedInput(input); err != nil {
			return out, err
		}
		out.ExecutedEventCount++
		out.ForkEvents = append(out.ForkEvents, SelectedContractExecutionForkEvent{
			SourceEventID: input.sourceEventID, ForkEventID: input.prepared.Event.ID(), EventName: string(input.prepared.Event.Type()),
		})
	}
	if err := c.joinLiveSelectedWork(continuations); err != nil {
		return out, err
	}
	if err := retireContinuations(); err != nil {
		return out, err
	}
	// Stop actors and join accepted work while their selected grant still exists.
	// Shutdown retires that grant and therefore belongs after the native handoff.
	if err := runtime.stopExecutionWork(); err != nil {
		return out, err
	}
	target, use, err := p.acquireOrdinaryLiveTarget(req)
	if use != nil {
		defer func() { finalErr = errors.Join(finalErr, use.Done()) }()
	}
	if err != nil {
		return out, err
	}
	req.LiveRuntime = target
	evidence, err := req.LiveRuntime.CurrentStartupGrantEvidence()
	if err != nil {
		return out, err
	}
	ctx := startupownership.WithGenerationGrantEvidence(p.operation.Context(), evidence)
	ctx = effects.WithAuthority(ctx, c.authority)
	if !req.liveSelectionHeld {
		releaseSelection := req.LiveContexts.BeginLiveRunSelection()
		defer releaseSelection()
		scope, err := req.LiveRuns.LoadRunLiveScope(ctx, p.plan.SourceRunID)
		if err != nil {
			return out, err
		}
		live, found, err := req.LiveRuns.LoadLiveRun(ctx, scope)
		if err != nil {
			return out, err
		}
		if found {
			use, err := acquireLivePredecessor(p.operation.Context(), req.LiveContexts, live)
			if err != nil {
				return out, err
			}
			parked, parkCtx := use.Runtime(), use.WorkContext()
			_, release, err := parked.ParkLiveRun(parkCtx, live)
			if err != nil {
				return out, errors.Join(err, use.Done())
			}
			defer func() {
				release()
				if !out.Activation.Activated {
					finalErr = errors.Join(finalErr, parked.ReconcileLiveRun(context.WithoutCancel(parkCtx), live.RunID))
				}
				finalErr = errors.Join(finalErr, use.Done())
			}()
		}
	}
	activation, err := c.activate(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
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

func (p *PreparedSelectedFork) acquireOrdinaryLiveTarget(req SelectedContractExecutionRequest) (*runtimepkg.Runtime, *runtimepkg.RuntimeContextUse, error) {
	if req.LiveRuntime != nil {
		return req.LiveRuntime, nil, nil
	}
	use, err := req.AcquireLiveRuntime(p.operation.Context(), p.loadedSource.SourceArtifactFact.BundleHash())
	if err != nil {
		return nil, use, err
	}
	if use == nil || use.Runtime() == nil || !use.Context.SourceArtifactFact.Matches(p.loadedSource.SourceArtifactFact) {
		return nil, use, errors.New("live fork requires its exact ordinarily admitted target")
	}
	return use.Runtime(), use, nil
}

func (c selectedContractForkLocalRuntimeContainer) restoreLiveSelectedWork(continuations *deliverycontinuation.Coordinator, recovered bool) error {
	ctx := c.attachment.ctx
	if err := c.attachment.pipeline.RestoreWorkflowTimers(ctx); err != nil {
		return err
	}
	if err := c.attachment.runtime.genericSchedules.ReconcileRunWakeups(ctx, c.req.ForkRunID); err != nil {
		return err
	}
	if err := c.admitSelectedDeploymentFeeds(ctx); err != nil {
		return err
	}
	// Recovered handlers may await already-owed fan-out or an original deadline.
	if recovered {
		if err := c.startSelectedServing(ctx); err != nil {
			return err
		}
	}
	return c.reconcileRecoveredDeliveries(ctx, continuations)
}

func (c selectedContractForkLocalRuntimeContainer) joinLiveSelectedWork(continuations *deliverycontinuation.Coordinator) error {
	if err := continuations.Synchronize(c.attachment.ctx); err != nil {
		return err
	}
	timeout := c.req.AgentRuntime.Options.QuiescenceTimeout
	if timeout <= 0 {
		timeout = selectedContractAgentRuntimeDefaultQuiescenceTimeout
	}
	ctx, cancel := context.WithTimeout(c.attachment.ctx, timeout)
	defer cancel()
	return c.attachment.runtime.WaitForQuiescence(ctx, c.attachment.runtime.bus)
}
