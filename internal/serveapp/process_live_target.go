package serveapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
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
	s.mu.RLock()
	manager, resetting := s.runtimeContexts, s.resetting
	requests := append([]serveRuntimeBundleContextRequest(nil), s.resetRequests...)
	s.mu.RUnlock()
	if manager == nil || resetting || len(requests) == 0 || s.processCapability == nil {
		return nil, errors.New("live target requires converged runtime composition")
	}
	current, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil || !exists {
		return nil, errors.Join(err, errors.New("live target requires its committed source set"))
	}
	member := false
	for _, source := range current.Sources {
		member = member || source.BundleHash == bundleHash
	}
	if member {
		pending, err := manager.PreparePendingSourceSetTransition(ctx, current)
		if err != nil {
			return nil, err
		}
		if err := pending.Commit(ctx, s.processCapability); err != nil {
			return nil, err
		}
		use, lookup, err := manager.AcquireBundleHash(ctx, bundleHash)
		if lookup.Loaded() || err != nil {
			return use, err
		}
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
	bundles := make([]serveRuntimeBundle, 0, len(requests)+1)
	for _, existing := range requests {
		bundles = append(bundles, existing.Loaded)
	}
	if err := validateServeMultiContextToolGatewayAdmission(request.Config, append(bundles, loaded)); err != nil {
		return nil, err
	}
	packs, err := cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: request.Config}, loaded.bundle, request.ProviderCredentials, request.ManagedCredentials)
	if err != nil {
		return nil, err
	}
	request.Loaded, request.StateStoreSummary = loaded, ""
	// Match serve's existing secondary-context composition: only the primary
	// context owns the process MCP listener and its turn registry.
	request.EnableToolGateway, request.ToolGatewayBinding = false, toolgateway.Binding{}
	request.ProviderTriggerCatalog = packs.ProviderTriggers.Catalog
	request.ChannelPlans, request.ChannelBindings = packs.Channels.Plans, packs.Channels.Bindings
	request.BootStartedAt, request.BootProgress = time.Now().UTC(), nil
	candidate, err := buildServeRuntimeBundleContext(request)
	if err != nil {
		return nil, fmt.Errorf("construct live target %s: %w", bundleHash, err)
	}
	retained := false
	defer func() {
		if !retained {
			retErr = errors.Join(retErr, candidate.runtime.ShutdownWithOptions(s.shutdownOptions))
			if candidate.workspaces != nil {
				retErr = errors.Join(retErr, candidate.workspaces.ReleaseSourceProjection(context.WithoutCancel(ctx)))
			}
		}
	}()
	plan := current
	if !member {
		addition, err := compileServeSourceSetPlan([]serveRuntimeBundleContext{candidate})
		if err != nil {
			return nil, err
		}
		plan, err = agenttopology.NewSourceSetPlan(append(current.Sources, addition.Sources...), append(current.Agents, addition.Agents...))
		if err != nil {
			return nil, err
		}
		transition, err := manager.PrepareSourceSetTransition(ctx, plan)
		if err != nil {
			return nil, err
		}
		if _, err := s.processCapability.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{
			OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan,
		}); err != nil {
			// An uncertain commit cannot restore visibility of stale grants.
			transition.RetainForRetry()
			return nil, err
		}
		if err := transition.Commit(ctx, s.processCapability); err != nil {
			return nil, err
		}
	}
	s.resetGeneration++
	grant, err := s.processCapability.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: bundleHash, RuntimeInstanceID: request.RuntimeInstanceID,
		RuntimeGeneration: s.resetGeneration, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		return nil, err
	}
	if err := candidate.runtime.InstallStartupGrant(grant); err != nil {
		return nil, err
	}
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
	if err := manager.Register(definition); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.resetContexts = append(s.resetContexts, candidate)
	s.resetRequests = append(s.resetRequests, request)
	s.mu.Unlock()
	retained, projectionOwned = true, false
	use, lookup, err := manager.AcquireBundleHash(ctx, bundleHash)
	if err == nil && !lookup.Loaded() {
		err = fmt.Errorf("admitted live target is not loaded: %s", lookup.Cause)
	}
	return use, err
}

func (s *processLifecycleSupervisor) prepareLiveRunRuntime(ctx context.Context, runID string) (retErr error) {
	if s == nil {
		return errors.New("continue requires its process runtime contexts")
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	s.mu.RUnlock()
	if manager == nil {
		return errors.New("continue requires its process runtime contexts")
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
