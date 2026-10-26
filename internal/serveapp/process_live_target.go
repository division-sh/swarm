package serveapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

// Source-set replacement and process shutdown share one lifecycle owner. The
// target is admitted only after selected preparation has validated the request.
func (s *processLifecycleSupervisor) acquireLiveTargetRuntime(ctx context.Context, bundleHash string) (_ *runtime.RuntimeContextUse, retErr error) {
	if s == nil {
		return nil, errors.New("live target requires process lifecycle ownership")
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.acquireLiveTargetRuntimeLocked(ctx, bundleHash)
}

func (s *processLifecycleSupervisor) acquireLiveTargetRuntimeLocked(ctx context.Context, bundleHash string) (_ *runtime.RuntimeContextUse, retErr error) {
	s.mu.RLock()
	manager, resetting := s.runtimeContexts, s.resetting
	requests := append([]serveRuntimeBundleContextRequest(nil), s.resetRequests...)
	s.mu.RUnlock()
	if manager == nil || resetting || len(requests) == 0 || s.processCapability == nil || s.resetBuildExecution == nil {
		return nil, errors.New("live target requires converged runtime composition")
	}
	current, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil || !exists {
		return nil, errors.Join(err, errors.New("live target requires its committed source set"))
	}
	if use, loaded, err := s.acquireCurrentLiveSource(ctx, manager, current, bundleHash); loaded || err != nil {
		return use, err
	}
	request := requests[0]
	loaded, err := loadServeRuntimeBundleFromArtifact(ctx, request.RepoRoot, request.Stores.sourceReader, bundleHash, request.PlatformPackBases, func(artifact *sourceartifact.AdmittedSourceArtifact, _ semanticview.Source) (*sourceartifact.RuntimeProjection, error) {
		return sourceartifact.MaterializeRuntimeProjection(artifact)
	})
	if err != nil {
		return nil, err
	}
	projectionOwned := true
	defer func() {
		if projectionOwned && loaded.cleanup != nil {
			retErr = errors.Join(retErr, loaded.cleanup())
		}
	}()
	request, err = prepareLiveTargetRequest(ctx, request, loaded)
	if err != nil {
		return nil, err
	}
	candidate, err := buildServeRuntimeBundleContext(request)
	if err != nil {
		return nil, fmt.Errorf("construct live target %s: %w", bundleHash, err)
	}
	retained := false
	registered := false
	defer func() {
		if !retained {
			retErr = errors.Join(retErr, s.discardLiveTarget(ctx, manager, bundleHash, candidate, registered))
		}
	}()
	plan, err := compileServeSourceSetPlan([]serveRuntimeBundleContext{candidate})
	if err != nil {
		return nil, err
	}
	execution, err := s.resetBuildExecution(candidate)
	if err != nil {
		return nil, err
	}
	grant, err := s.replaceLiveSourceSet(ctx, manager, current, plan, bundleHash, request.RuntimeInstanceID)
	if err != nil {
		return nil, err
	}
	if err := candidate.runtime.InstallStartupGrant(grant); err != nil {
		return nil, err
	}
	// Startup MCP uses only the candidate's exact private startup token. Normal
	// traffic still acquires the manager and cannot select this occurrence yet.
	s.mu.Lock()
	s.resetContexts = []serveRuntimeBundleContext{candidate}
	s.resetRequests = []serveRuntimeBundleContextRequest{request}
	s.mu.Unlock()
	if err := candidate.runtime.Start(request.Ctx); err != nil {
		return nil, err
	}
	definition := runtime.BundleContext{
		SourceArtifactFact: candidate.sourceArtifactFact, BundleIdentity: candidate.bootIdentity,
		Source: candidate.loaded.source, Runtime: candidate.runtime, WorkOwner: candidate.runtime.WorkOccurrence(),
		ProviderTriggerGeneration: candidate.providerTriggerGeneration,
		InstalledTriggerSubjects:  candidate.installedTriggerSubjects, PackInventoryDigest: candidate.packInventoryDigest,
		ChannelPlans: candidate.runtime.Options.ChannelPlans, DeclaredChannelPublication: candidate.runtime.Options.DeclaredChannelPublication,
	}
	if err := manager.RegisterLiveReplacement(definition); err != nil {
		return nil, err
	}
	registered = true
	s.mu.Lock()
	s.currentRT, s.execution = candidate.runtime, execution
	s.currentSourceArtifactFact = candidate.sourceArtifactFact
	s.mu.Unlock()
	retained, projectionOwned = true, false
	if s.resetRefresh != nil {
		if err := s.resetRefresh(ctx); err != nil {
			return nil, err
		}
	}
	if s.resetChannelPublication != nil {
		if err := s.resetChannelPublication(); err != nil {
			return nil, err
		}
	}
	use, lookup, err := manager.AcquireBundleHash(ctx, bundleHash)
	if err == nil && !lookup.Loaded() {
		err = fmt.Errorf("admitted live target is not loaded: %s", lookup.Cause)
	}
	return use, err
}

func prepareLiveTargetRequest(ctx context.Context, request serveRuntimeBundleContextRequest, loaded serveRuntimeBundle) (serveRuntimeBundleContextRequest, error) {
	if err := validateServeMultiContextToolGatewayAdmission(request.Config, []serveRuntimeBundle{loaded}); err != nil {
		return request, err
	}
	packs, err := cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: request.Config}, loaded.bundle, request.ProviderCredentials, request.ManagedCredentials)
	if err != nil {
		return request, err
	}
	request.Loaded, request.StateStoreSummary = loaded, ""
	request.ProviderTriggerCatalog = packs.ProviderTriggers.Catalog
	request.ChannelPlans, request.ChannelBindings = packs.Channels.Plans, packs.Channels.Bindings
	request.BootStartedAt, request.BootProgress = time.Now().UTC(), nil
	return request, nil
}

func (s *processLifecycleSupervisor) discardLiveTarget(ctx context.Context, manager *runtime.RuntimeContextManager, bundleHash string, candidate serveRuntimeBundleContext, registered bool) (err error) {
	if registered {
		err = manager.DeactivateBundleHashWithOptions(bundleHash, runtime.RuntimeContextCauseUnavailable, s.shutdownOptions).ShutdownErr
	} else {
		err = candidate.runtime.ShutdownWithOptions(s.shutdownOptions)
	}
	if candidate.workspaces != nil {
		err = errors.Join(err, candidate.workspaces.ReleaseSourceProjection(context.WithoutCancel(ctx)))
	}
	return err
}

func (s *processLifecycleSupervisor) acquireCurrentLiveSource(ctx context.Context, manager *runtime.RuntimeContextManager, current agenttopology.SourceSetPlan, bundleHash string) (*runtime.RuntimeContextUse, bool, error) {
	member := false
	for _, source := range current.Sources {
		member = member || source.BundleHash == bundleHash
	}
	if !member {
		return nil, false, nil
	}
	pending, err := manager.PreparePendingSourceSetTransition(ctx, current)
	if err != nil {
		return nil, false, err
	}
	if err := pending.Commit(ctx, s.processCapability); err != nil {
		return nil, false, err
	}
	use, lookup, err := manager.AcquireBundleHash(ctx, bundleHash)
	return use, lookup.Loaded(), err
}

func (s *processLifecycleSupervisor) replaceLiveSourceSet(ctx context.Context, manager *runtime.RuntimeContextManager, current, plan agenttopology.SourceSetPlan, bundleHash, runtimeInstanceID string) (startupownership.LiveGenerationGrant, error) {
	request := agenttopology.SourceSetCommitRequest{
		OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan, RequirePausedRemoval: true,
	}
	if err := s.processCapability.CheckSourceSetReplacement(ctx, request); err != nil {
		return nil, err
	}
	// Only a bundle swap reaches this join; an ordinary pause keeps processes.
	for _, source := range current.Sources {
		if result := manager.DeactivateBundleHashWithOptions(source.BundleHash, runtime.RuntimeContextCauseUnloaded, s.shutdownOptions); result.ShutdownErr != nil {
			return nil, result.ShutdownErr
		}
	}
	if s.stopRunStalled != nil {
		s.stopRunStalled()
		s.stopRunStalled = nil
	}
	if err := s.releaseResetProjections(context.WithoutCancel(ctx)); err != nil {
		return nil, err
	}
	transition, err := manager.PrepareSourceSetTransition(ctx, plan)
	if err != nil {
		return nil, err
	}
	if _, err := s.processCapability.RestoreSourceSet(ctx, request); err != nil {
		if errors.Is(err, runcontrol.ErrNotPaused) {
			// This refusal precedes every write. Rebuild only after observing the
			// exact predecessor, never on an attempted or uncertain commit.
			if abortErr := transition.Abort(); abortErr != nil {
				return nil, errors.Join(err, abortErr)
			}
			recoveryCtx := context.WithoutCancel(ctx)
			observed, exists, readErr := s.processCapability.CurrentSourceSet(recoveryCtx)
			if readErr != nil || !exists || observed.Revision != current.Revision || len(current.Sources) != 1 {
				return nil, errors.Join(err, readErr, errors.New("refused source replacement lost its exact predecessor"))
			}
			use, restoreErr := s.acquireLiveTargetRuntimeLocked(recoveryCtx, current.Sources[0].BundleHash)
			if use != nil {
				restoreErr = errors.Join(restoreErr, use.Done())
			}
			return nil, errors.Join(err, restoreErr)
		}
		// An uncertain commit must keep stale grants withdrawn until re-observed.
		transition.RetainForRetry()
		return nil, err
	}
	if err := transition.Commit(ctx, s.processCapability); err != nil {
		return nil, err
	}
	s.resetGeneration++
	return s.processCapability.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: bundleHash, RuntimeInstanceID: runtimeInstanceID,
		RuntimeGeneration: s.resetGeneration, SourceSetRevision: plan.Revision,
	})
}

func (s *processLifecycleSupervisor) prepareLiveRunRuntime(ctx context.Context, runID string) (retErr error) {
	if s == nil {
		return errors.New("continue requires its process runtime contexts")
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	liveRuns := s.selectedRecoveryEnvironment.LiveRuns
	s.mu.RUnlock()
	if manager == nil || liveRuns == nil {
		return errors.New("continue requires its process runtime contexts")
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
		if found && live.RunID != runID {
			return &runlifecycle.LiveRunConflictError{Scope: scope, LiveRunID: live.RunID}
		}
	}
	_, availability, err := manager.LookupRunStatus(ctx, runID)
	if err != nil {
		return err
	}
	if !availability.Available() {
		return fmt.Errorf("continue requires its retained source artifact: %s", availability.DetailString())
	}
	use, err := s.acquireLiveTargetRuntime(ctx, availability.BundleHash)
	if use != nil {
		defer func() { retErr = errors.Join(retErr, use.Done()) }()
	}
	return err
}
