package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type sourcePause struct {
	predecessor *runlifecycle.LiveRun
	paused      bool
}

// Preparation must refuse before predecessor retirement. Once execution starts,
// its existing owner handles preparation disposal and transaction revalidation.
func executePreparedLiveFork(ctx context.Context, req SelectedContractExecutionRequest, prepared **PreparedSelectedFork, finishPreparation func()) (out SelectedContractExecutionResult, finalErr error) {
	current := *prepared
	executed := false
	defer func() {
		if !executed && current != nil {
			finalErr = errors.Join(finalErr, req.Owner.completePreparation(current))
		}
	}()
	execute := func(context.Context) error {
		executed = true
		out, finalErr = executePreparedSelectedContractRunFork(ctx, req, current)
		return finalErr
	}
	if req.LiveRuns == nil || req.LiveContexts == nil {
		return out, errors.New("live fork requires its durable flow selector and runtime contexts")
	}
	var paused sourcePause
	if req.Recovery == nil {
		var err error
		paused, err = pauseSelectedSourceBeforeLiveFork(ctx, req, current)
		if err != nil {
			return out, err
		}
		req.requirePausedSource = paused.paused
		if paused.paused {
			transitionOwner, ok := req.Owner.ports.pipelineObligations.(pipelineobligation.ParentTransitionOwner)
			if !ok {
				return out, errors.New("live fork requires selected-store run transition admission")
			}
			transition, err := transitionOwner.BeginSourceForkTransition(ctx, current.plan.SourceRunID)
			if err != nil {
				return out, err
			}
			defer transition.Done()
			releaseSourceWork, err := joinSelectedSourceWork(ctx, req, current.plan.SourceRunID, paused.predecessor != nil)
			if err != nil {
				return out, err
			}
			defer func() { finalErr = errors.Join(finalErr, releaseSourceWork()) }()
			// Already-admitted work may have published after the first claim drain.
			// Once its occurrence has joined, this second readback closes that cut.
			cutTransition, err := transitionOwner.BeginSourceForkTransition(ctx, current.plan.SourceRunID)
			if err != nil {
				return out, err
			}
			defer cutTransition.Done()
			point := current.plan.ForkPoint
			req.fixedPoint = &point
			req.ForkOperation = current.forkOperation
			finishPreparation()
			if err := req.Owner.completePreparation(current); err != nil {
				return out, err
			}
			current = nil
			current, finalErr = req.Owner.prepareWithOperation(ctx, req, nil)
			if finalErr != nil {
				return out, finalErr
			}
			*prepared = current
			finishRefreshed, err := req.Owner.beginSelectedOrchestration(current)
			if err != nil {
				return out, err
			}
			defer finishRefreshed()
		}
	}
	scope, err := req.LiveRuns.LoadRunLiveScope(ctx, current.plan.SourceRunID)
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
	if found && paused.predecessor == nil && !req.liveSelectionHeld {
		releaseSelection := req.LiveContexts.BeginLiveRunSelection()
		defer releaseSelection()
		req.liveSelectionHeld = true
		live, found, err = req.LiveRuns.LoadLiveRun(ctx, scope)
		if err != nil {
			return out, err
		}
	}
	if paused.predecessor != nil {
		if found {
			return out, fmt.Errorf("live fork source changed after pause: source=%s live=%s", paused.predecessor.RunID, live.RunID)
		}
		live, found = *paused.predecessor, true
	} else if req.Recovery == nil && found && live.RunID == current.plan.SourceRunID {
		return out, fmt.Errorf("paused fork source %s became live before materialization", live.RunID)
	}
	if found {
		use, err := acquireLivePredecessor(ctx, req.LiveContexts, live)
		if err != nil {
			return out, err
		}
		parked, parkCtx := use.Runtime(), use.WorkContext()
		if live.Source.Matches(current.loadedSource.SourceArtifactFact) && use.Context.RootAdmission.Reference() != current.loadedSource.RootedSource.Admission().Reference() {
			return out, errors.Join(errors.New("live predecessor has a different admitted root"), use.Done())
		}
		if live.Source.Matches(current.loadedSource.SourceArtifactFact) {
			releaseUse := sync.OnceValue(use.Done)
			defer func() { finalErr = errors.Join(finalErr, releaseUse()) }()
			req.LiveRuntime = parked
			finalErr = parked.WithParkedLiveRun(parkCtx, live, execute)
			if !out.Activation.Activated {
				finalErr = errors.Join(finalErr, parked.ReconcileLiveRun(context.WithoutCancel(parkCtx), live.RunID))
			}
			finalErr = errors.Join(finalErr, releaseUse())
		} else {
			pauseCtx, release, err := parked.ParkLiveRun(parkCtx, live)
			if err != nil {
				return out, errors.Join(err, use.Done())
			}
			releaseUse := sync.OnceValue(func() error { release(); return use.Done() })
			defer func() { finalErr = errors.Join(finalErr, releaseUse()) }()
			acquire := req.AcquireLiveRuntime
			paused := false
			alreadyPaused := req.requirePausedSource && live.RunID == current.plan.SourceRunID
			req.AcquireLiveRuntime = func(ctx context.Context, root sourceartifact.RootAdmissionReference) (*runtimepkg.RuntimeContextUse, error) {
				use, sourcePaused, err := pauseLiveSourceAndAcquire(ctx, pauseCtx, parked, live.RunID, alreadyPaused, root, releaseUse, acquire)
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
	if out.Activation.Activated && !out.liveReconciliationAttempted {
		if !req.liveSelectionHeld {
			releaseSelection := req.LiveContexts.BeginLiveRunSelection()
			defer releaseSelection()
		}
		finalErr = errors.Join(finalErr, reconcileCommittedLiveFork(context.WithoutCancel(ctx), req.LiveContexts, out.Materialization.ForkRunID))
	}
	return out, finalErr
}

func joinSelectedSourceWork(ctx context.Context, req SelectedContractExecutionRequest, runID string, wasLive bool) (func() error, error) {
	use, lookup, availability, err := req.LiveContexts.AcquireRun(ctx, runID)
	if err != nil {
		if use != nil {
			err = errors.Join(err, use.Done())
		}
		return nil, err
	}
	if !lookup.Loaded() {
		if wasLive {
			return nil, errors.New("live fork source runtime was unloaded before its work joined")
		}
		return func() error { return nil }, nil
	}
	if !availability.Available() || use == nil || use.Runtime() == nil {
		if use != nil {
			return nil, errors.Join(errors.New("live fork source has no admitted runtime occurrence"), use.Done())
		}
		return nil, errors.New("live fork source has no admitted runtime occurrence")
	}
	occurrence := use.Runtime().WorkOccurrence()
	if occurrence == nil {
		return nil, errors.Join(errors.New("live fork source runtime has no work occurrence"), use.Done())
	}
	if err := use.Done(); err != nil {
		return nil, err
	}
	done, err := occurrence.BeginRunTransition(ctx, runID)
	if err != nil {
		return nil, err
	}
	return func() error { done(); return nil }, nil
}

func pauseSelectedSourceBeforeLiveFork(ctx context.Context, req SelectedContractExecutionRequest, prepared *PreparedSelectedFork) (result sourcePause, finalErr error) {
	control, ok := req.LiveRuns.(runcontrol.Store)
	if !ok {
		return sourcePause{}, errors.New("live fork requires its selected run-control owner")
	}
	sourceID := prepared.plan.SourceRunID
	source := prepared.originalSource.SourceArtifactFact
	if err := source.Validate(); err != nil {
		return sourcePause{}, fmt.Errorf("live fork original source: %w", err)
	}
	scope, err := req.LiveRuns.LoadRunLiveScope(ctx, sourceID)
	if err != nil {
		return sourcePause{}, err
	}
	if err := scope.Validate(); err != nil {
		return sourcePause{}, err
	}
	live, found, err := req.LiveRuns.LoadLiveRun(ctx, scope)
	if err != nil {
		return sourcePause{}, err
	}
	availability, err := req.Owner.ports.fork.LoadRunBundleAvailability(ctx, sourceID)
	if err != nil {
		return sourcePause{}, err
	}
	if !availability.Available() || availability.RunID != sourceID || availability.BundleHash != source.BundleHash() {
		return sourcePause{}, fmt.Errorf("live fork source differs from its exact admitted bundle: %s", availability.DetailString())
	}
	selectedSource := found && live.RunID == sourceID
	pausedLanding := false
	if availability.Status == "paused" {
		landingReader, ok := req.LiveRuns.(runlifecycle.LandingRunReader)
		if !ok {
			return sourcePause{}, errors.New("paused fork source requires its landing-run reader")
		}
		landing, selected, err := landingReader.LoadLandingRun(ctx, scope)
		if err != nil {
			return sourcePause{}, err
		}
		pausedLanding = selected && landing.RunID == sourceID && landing.State == runlifecycle.StatePaused
	}
	if !selectedSource && !pausedLanding {
		// A retained historical source is not the flow's live run to pause.
		return sourcePause{}, nil
	}
	ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, source), sourceID)
	if scope, present := authoractivity.ScopeFromContext(ctx); present && scope.Kind == authoractivity.ScopeBundle {
		ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(scope.RuntimeInstanceID, source.BundleHash()))
	}
	parked, err := control.RunDispatchParked(ctx, sourceID)
	if err != nil {
		return sourcePause{}, fmt.Errorf("read live fork source control: %w", err)
	}
	if parked {
		if availability.Status != "paused" {
			return sourcePause{}, fmt.Errorf("live fork source changed during paused readback: run=%s status=%s", sourceID, availability.Status)
		}
		result := sourcePause{paused: true}
		if selectedSource {
			result.predecessor = &live
		}
		return result, nil
	}
	if !selectedSource {
		return sourcePause{}, fmt.Errorf("retained paused fork source %s lacks its admitted paused control", sourceID)
	}
	if availability.Status != "running" && availability.Status != "live" {
		return sourcePause{}, fmt.Errorf("live fork source is not pausable: run=%s status=%s", sourceID, availability.Status)
	}
	use, lookup, admitted, err := req.LiveContexts.AcquireRun(ctx, sourceID)
	if err != nil {
		return sourcePause{}, errors.Join(err, use.Done())
	}
	if !lookup.Loaded() || !admitted.Available() || use == nil || use.Runtime() == nil ||
		!use.Context.SourceArtifactFact.Matches(source) || admitted.RootReference != availability.RootReference {
		return sourcePause{}, errors.Join(errors.New("live fork source requires its original admitted runtime"), use.Done())
	}
	defer func() { finalErr = errors.Join(finalErr, use.Done()) }()
	pauseCtx := correlation.WithRunID(correlation.WithSourceArtifactFact(use.WorkContext(), source), sourceID)
	_, err = runcontrol.NewController(control, nil, runcontrol.Options{}).Pause(pauseCtx, runcontrol.TransitionRequest{
		RunID: sourceID, Reason: "fork_live_switch", ControlledBy: "run.fork",
	})
	if err != nil {
		return sourcePause{}, err
	}
	parked, err = control.RunDispatchParked(pauseCtx, sourceID)
	if err != nil {
		return sourcePause{}, fmt.Errorf("read paused fork source control: %w", err)
	}
	availability, err = req.Owner.ports.fork.LoadRunBundleAvailability(pauseCtx, sourceID)
	if err != nil {
		return sourcePause{}, fmt.Errorf("read paused fork source availability: %w", err)
	}
	if !parked || availability.Status != "paused" || availability.BundleHash != source.BundleHash() {
		return sourcePause{}, fmt.Errorf("live fork source changed during pause readback: %s", availability.DetailString())
	}
	return sourcePause{predecessor: &live, paused: true}, nil
}

func pauseLiveSourceAndAcquire(ctx, pauseCtx context.Context, parked *runtimepkg.Runtime, runID string, alreadyPaused bool, root sourceartifact.RootAdmissionReference, release func() error, acquire func(context.Context, sourceartifact.RootAdmissionReference) (*runtimepkg.RuntimeContextUse, error)) (*runtimepkg.RuntimeContextUse, bool, error) {
	if acquire == nil || parked.RunControl == nil {
		return nil, false, errors.New("bundle swap requires its existing runtime and run-control owners")
	}
	paused := false
	if alreadyPaused {
		var err error
		paused, err = parked.RunControl.QueueableRunDispatchParked(pauseCtx, runID)
		if err != nil || !paused {
			return nil, paused, errors.Join(err, errors.New("fork source continued before bundle swap"))
		}
	} else {
		// A different live predecessor still takes its original pause at handoff.
		result, err := parked.RunControl.Pause(pauseCtx, runcontrol.TransitionRequest{RunID: runID, Reason: "fork bundle swap", ControlledBy: "run.fork"})
		paused = result.Status == runcontrol.StatusPaused
		if err != nil {
			return nil, paused, err
		}
	}
	if err := release(); err != nil {
		return nil, paused, err
	}
	use, err := acquire(ctx, root)
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
	if !lookup.Loaded() || !availability.Available() || use == nil || use.Runtime() == nil || !use.Context.SourceArtifactFact.Matches(live.Source) || use.Context.RootAdmission.Reference() != availability.RootReference {
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
	reconcile, err := c.selectedBarrierReconciler()
	if err != nil {
		return out, err
	}
	continuations, err := deliverycontinuation.NewSelected(c.ports.busDurable.DeliveryLifecycle, c.attachment.deliveryAuthority, p.operation.selected, runtime.bus, func(_ context.Context, cause error) { c.attachment.cancel(cause) }, reconcile)
	if err != nil {
		return out, err
	}
	if err := runtime.bus.SetDeliveryContinuationOwner(continuations); err != nil {
		return out, err
	}
	if err := c.admitSelectedDeploymentFeeds(c.attachment.ctx); err != nil {
		return out, err
	}
	// Bind fixed-cut ordinals before the first barrier reconciliation.
	inputs, err := c.commitSelectedInputs()
	defer func() {
		for _, input := range inputs {
			finalErr = errors.Join(finalErr, runtime.bus.AbandonPreparedPublish(context.WithoutCancel(input.ctx), input.prepared))
		}
	}()
	if err != nil {
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
	// A stopped producer cannot add another selected publication. Drain any
	// committed event that reached the pipeline after the earlier quiescence cut.
	if err := runtime.bus.RecoverSelectedRunPipelineToExhaustion(c.attachment.ctx, c.req.ForkRunID); err != nil {
		return out, err
	}
	return c.activateLiveHandoff(p, req, out)
}

func (c selectedContractForkLocalRuntimeContainer) activateLiveHandoff(p *PreparedSelectedFork, req SelectedContractExecutionRequest, result SelectedContractExecutionResult) (out SelectedContractExecutionResult, finalErr error) {
	out = result
	target, use, err := p.acquireOrdinaryLiveTarget(req)
	if use != nil {
		defer func() { finalErr = errors.Join(finalErr, use.Done()) }()
	}
	if err != nil {
		return out, err
	}
	req.LiveRuntime = target
	ordinaryGrant, err := req.LiveRuntime.CurrentStartupGrant()
	if err != nil {
		return out, err
	}
	evidence, err := ordinaryGrant.Evidence()
	if err != nil {
		return out, err
	}
	ctx := startupownership.WithGenerationGrantEvidence(p.operation.Context(), evidence)
	ctx = effects.WithAuthority(ctx, c.authority)
	var releasePredecessorPark func()
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
			releasePredecessorPark = sync.OnceFunc(release)
			defer func() {
				releasePredecessorPark()
				if !out.Activation.Activated {
					finalErr = errors.Join(finalErr, parked.ReconcileLiveRun(context.WithoutCancel(parkCtx), live.RunID))
				}
				finalErr = errors.Join(finalErr, use.Done())
			}()
		}
	}
	process, err := p.agentRuntime.Options.ProcessCapability.Evidence()
	if err != nil {
		return out, err
	}
	activation, err := c.activate(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		OriginalRootAdmission:   p.originalSource.RootedSource.Admission(),
		Process:                 process.LeaseStamp(),
		OrdinaryGenerationGrant: ordinaryGrant,
		InheritedSchedules:      p.readiness, ForkOperation: p.forkOperation, DataPins: result.Materialization.DataPins,
		ExecutionSource: p.loadedSource.Source, ForkRunID: result.Materialization.ForkRunID,
		AllowedSourceEventIDs: selectedContractExecutionFrontierEventIDs(p.frontier.FrontierEvents),
		FrontierAdmission:     p.frontier, RouteTopology: p.routeTopology, RecipientPlanning: *p.model.RecipientPlanning,
	})
	out.Activation = activation
	if !out.Activation.Activated {
		return out, errors.Join(err, fmt.Errorf("live handoff was not acknowledged; selected work remains recoverable"))
	}
	if p.forkOperation != nil {
		record, found, _, projectionErr := p.owner.observeSelectedForkOperation(ctx, *p.forkOperation, p.operation)
		if !activatedForkOperationObserved(record, found, projectionErr) {
			return out, errors.Join(err, projectionErr, errors.New("acknowledged live handoff lacks its exact immutable operation result"))
		}
		out.ExecutedEventCount = selectedForkRecordedOutcome(record).ExecutedEventCount
	}
	// Post-commit reconciliation can fail without undoing the immutable ACK.
	// Direct targets may still be parked by the outer owner.
	if use != nil {
		if releasePredecessorPark != nil {
			releasePredecessorPark()
		}
		out.liveReconciliationAttempted = true
		err = errors.Join(err, target.ReconcileAcknowledgedLiveFork(use.WorkContext(), out.Materialization.ForkRunID))
	}
	return out, err
}

func activatedForkOperationObserved(record runfork.ForkOperationRecord, found bool, err error) bool {
	return err == nil && found && record.Status == runfork.ForkOperationActivated
}

func (p *PreparedSelectedFork) acquireOrdinaryLiveTarget(req SelectedContractExecutionRequest) (*runtimepkg.Runtime, *runtimepkg.RuntimeContextUse, error) {
	if req.LiveRuntime != nil {
		if req.LiveRuntime.Bus == nil {
			return nil, nil, errors.New("live fork target requires its compiled source admission")
		}
		admitted, err := req.LiveRuntime.Bus.AdmitSourceArtifactFact(p.operation.Context())
		if err != nil {
			return nil, nil, err
		}
		root, present, err := correlation.RootAdmissionFromContext(admitted)
		if err != nil || !present || root.Reference() != p.loadedSource.RootedSource.Admission().Reference() {
			return nil, nil, errors.Join(err, errors.New("live fork target has a different admitted root"))
		}
		return req.LiveRuntime, nil, nil
	}
	use, err := req.AcquireLiveRuntime(p.operation.Context(), p.loadedSource.RootedSource.Admission().Reference())
	if err != nil {
		return nil, use, err
	}
	if use == nil || use.Runtime() == nil || !use.Context.SourceArtifactFact.Matches(p.loadedSource.SourceArtifactFact) || use.Context.RootAdmission.Reference() != p.loadedSource.RootedSource.Admission().Reference() {
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
