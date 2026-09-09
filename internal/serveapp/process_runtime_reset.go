package serveapp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func loadServeRecoveredResetSources(ctx context.Context, repo string, artifacts sourceArtifactReader, recovery *destructivereset.PendingRecovery, packBases *packartifact.PlatformPackBaseGenerationOwner, materialize func(*sourceartifact.AdmittedSourceArtifact, semanticview.Source) (*sourceartifact.RuntimeProjection, error)) ([]serveRuntimeBundle, error) {
	plan, err := recovery.SourceSet()
	if err != nil {
		return nil, err
	}
	if len(plan.Sources) == 0 {
		return nil, nil
	}
	spec, err := cliapp.EmbeddedPlatformSpecPath()
	if err != nil {
		return nil, err
	}
	var loaded []serveRuntimeBundle
	for _, source := range plan.Sources {
		bundle, err := loadServeRuntimeBundleFromArtifact(ctx, repo, artifacts, source.BundleHash, spec, packBases, materialize)
		if err != nil {
			for _, prior := range loaded {
				err = errors.Join(err, prior.cleanup())
			}
			return nil, err
		}
		loaded = append(loaded, bundle)
	}
	return loaded, nil
}

type serveRuntimeReset struct {
	supervisor  *processLifecycleSupervisor
	operationID string
	release     sync.Once
}

func (s *processLifecycleSupervisor) ResetSourceProjections(ctx context.Context) ([]destructivereset.SourceProjection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.currentRT == nil && !s.resetting {
		return nil, nil
	}
	var intents []destructivereset.SourceProjection
	for i, current := range s.resetContexts {
		intent, err := current.loaded.sourceProjection.CleanupIntent()
		if err != nil {
			return nil, err
		}
		if i >= len(s.resetRequests) {
			return nil, errors.New("reset source projection lacks workspace construction identity")
		}
		intents = append(intents, destructivereset.SourceProjection{Cleanup: intent, ManagedContainers: s.resetRequests[i].WorkspaceBackend.Backend == "docker"})
	}
	return intents, nil
}

func (s *processLifecycleSupervisor) BeginDestructiveReset(ctx context.Context, operationID string) (destructivereset.RuntimeReset, error) {
	if s == nil {
		return nil, errors.New("reset requires process lifecycle supervisor")
	}
	if id, err := uuid.Parse(operationID); err != nil || id == uuid.Nil || id.String() != operationID {
		return nil, errors.New("reset requires its canonical durable operation ID")
	}
	s.operationMu.Lock()
	if err := ctx.Err(); err != nil {
		s.operationMu.Unlock()
		return nil, err
	}
	if s.resetOperationID == operationID && s.resetConverged {
		// Publication may precede an uncertain final journal acknowledgment.
		// The same operation must not withdraw its own successor execution.
		return &serveRuntimeReset{supervisor: s, operationID: operationID}, nil
	}
	if s.resetBuildExecution == nil || len(s.resetRequests) == 0 {
		if s.processCapability == nil || s.runtimeContexts == nil || s.currentRT != nil {
			s.operationMu.Unlock()
			return nil, errors.New("reset requires admitted-source reconstruction inputs")
		}
		if !s.resetStartup {
			plan, exists, err := s.processCapability.CurrentSourceSet(ctx)
			if err != nil || !exists || len(plan.Sources) != 0 {
				s.operationMu.Unlock()
				return nil, errors.Join(err, errors.New("reset without constructors requires authoritative empty topology"))
			}
		}
	}
	if s.resetOperationID != operationID && s.resetOperationID != "" && !s.resetConverged {
		s.operationMu.Unlock()
		return nil, destructivereset.ErrOperationInProgress
	}
	if s.resetStartup {
		operation, err := s.processCapability.ReadResetOperation(ctx, operationID)
		if err != nil {
			s.operationMu.Unlock()
			return nil, err
		}
		s.resetRecoveredProjections = append(append([]destructivereset.SourceProjection(nil), operation.Request.SourceProjections...), operation.Allocations...)
	}
	s.resetOperationID, s.resetConverged = operationID, false
	s.mu.Lock()
	s.resetting = true
	if s.ready != nil {
		s.ready.Store(false)
	}
	s.mu.Unlock()
	if err := s.joinResetContexts(); err != nil {
		s.operationMu.Unlock()
		return nil, err
	}
	if s.stopRunStalled != nil {
		s.stopRunStalled()
		s.stopRunStalled = nil
	}
	return &serveRuntimeReset{supervisor: s, operationID: operationID}, nil
}

func (r *serveRuntimeReset) Release() {
	r.release.Do(r.supervisor.operationMu.Unlock)
}

func (r *serveRuntimeReset) SettleResources(ctx context.Context) error {
	s := r.supervisor
	if s.resetOperationID != r.operationID {
		return errors.New("reset resource settlement identity changed")
	}
	if s.resetConverged {
		return nil
	}
	intents := s.resetRecoveredProjections
	if !s.resetStartup {
		if err := s.releaseResetProjections(ctx); err != nil {
			return err
		}
		operation, err := s.processCapability.ReadResetOperation(ctx, r.operationID)
		if err != nil {
			return err
		}
		intents = append(append([]destructivereset.SourceProjection(nil), operation.Request.SourceProjections...), operation.Allocations...)
	}
	for _, intent := range intents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if intent.ManagedContainers {
			if s.resetContainerRuntime == nil {
				return errors.New("reset projection disposal requires its container owner")
			}
			if err := s.resetContainerRuntime.DisposeProjectionContainers(ctx, intent.Cleanup); err != nil {
				return err
			}
		}
		if err := sourceartifact.SettleRuntimeProjectionCleanup(intent.Cleanup); err != nil {
			return err
		}
	}
	return nil
}

func (r *serveRuntimeReset) Complete(ctx context.Context, retainSources bool) error {
	s := r.supervisor
	if s.resetOperationID != r.operationID {
		return errors.New("reset lifecycle operation identity changed")
	}
	if s.resetConverged {
		return nil
	}
	if s.resetStartup {
		// Startup reconstructs through the ordinary admitted-source path while
		// the recovery handle retains serialization. Never reconstruct it twice.
		plan, exists, err := s.processCapability.CurrentSourceSet(ctx)
		if err != nil || !exists {
			return errors.Join(err, errors.New("reset startup requires committed source topology"))
		}
		if !retainSources && len(plan.Sources) != 0 {
			return errors.New("cleared reset startup still has admitted sources")
		}
		if len(plan.Sources) != len(s.resetContexts) {
			return errors.New("reset startup contexts have not converged with committed sources")
		}
		for _, candidate := range s.resetContexts {
			use, _, err := s.runtimeContexts.AcquireBundleHash(ctx, candidate.sourceArtifactFact.BundleHash())
			if err != nil || use == nil {
				return errors.Join(err, errors.New("reset startup publication is not executable"))
			}
			matches := use.Runtime() == candidate.runtime
			grant, grantErr := candidate.runtime.CurrentStartupGrantEvidence()
			doneErr := use.Done()
			if !matches || grantErr != nil || doneErr != nil || grant.State != startupownership.GrantAdmitted {
				return errors.Join(grantErr, doneErr, errors.New("reset startup execution authority has not converged"))
			}
		}
		s.mu.Lock()
		s.resetting = false
		s.mu.Unlock()
		s.resetStartup, s.resetConverged = false, true
		return nil
	}
	s.mu.Lock()
	s.currentRT = nil
	s.mu.Unlock()
	if retainSources {
		if err := s.reconstructResetContexts(ctx); err != nil {
			return err
		}
	} else {
		s.mu.Lock()
		s.execution = nil
		s.resetting = false
		s.mu.Unlock()
	}
	s.resetConverged = true
	return nil
}

func (s *processLifecycleSupervisor) joinResetContexts() error {
	var result error
	for _, retired := range s.runtimeContexts.DeactivateAllWithOptions(runtime.RuntimeContextCauseUnloaded, s.shutdownOptions) {
		result = errors.Join(result, retired.ShutdownErr)
	}
	return result
}

// The runtime owner must have joined before releasing either projection handle.
// ReleaseSourceProjection permanently closes the workspace, even for retained bytes.
func (s *processLifecycleSupervisor) releaseResetProjections(ctx context.Context) error {
	var result error
	for _, current := range s.resetContexts {
		if current.workspaces != nil {
			if err := current.workspaces.ReleaseSourceProjection(ctx); err != nil {
				result = errors.Join(result, err)
				continue
			}
		}
		if current.loaded.cleanup != nil {
			result = errors.Join(result, current.loaded.cleanup())
		}
	}
	return result
}

func (s *processLifecycleSupervisor) reconstructResetContexts(ctx context.Context) (retErr error) {
	plan, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("reset reconstruction requires retained source topology")
	}
	if len(plan.Sources) == 0 {
		// A later retain request preserves the current empty set; admitted
		// construction inputs are not authority to resurrect a deleted source.
		s.mu.Lock()
		s.execution = nil
		s.resetting = false
		s.mu.Unlock()
		return nil
	}
	s.resetGeneration++
	candidates := make([]serveRuntimeBundleContext, 0, len(s.resetRequests))
	staged := false
	defer func() {
		if retErr == nil {
			return
		}
		var joinErr error
		if staged {
			joinErr = s.joinResetContexts()
		} else {
			for _, candidate := range candidates {
				candidate.runtime.CloseAdmission()
			}
			for _, candidate := range candidates {
				joinErr = errors.Join(joinErr, candidate.runtime.ShutdownWithOptions(s.shutdownOptions))
			}
		}
		retErr = errors.Join(retErr, joinErr)
		if joinErr == nil {
			retErr = errors.Join(retErr, s.releaseResetProjections(context.WithoutCancel(ctx)))
		}
	}()
	for _, original := range s.resetRequests {
		request := original
		if request.Loaded.bundle == nil || request.Loaded.bundle.SourceArtifact == nil {
			return errors.New("reset reconstruction requires identical admitted source artifact")
		}
		projection, err := s.materializeResetProjection(ctx, request.Loaded.bundle.SourceArtifact, request.WorkspaceBackend.Backend == "docker")
		if err != nil {
			return err
		}
		request.Loaded.sourceProjection = projection
		request.Loaded.cleanup = projection.Release
		request.BootStartedAt = time.Now().UTC()
		request.BootProgress = nil
		candidate, err := buildServeRuntimeBundleContext(request)
		if err != nil {
			return errors.Join(err, projection.Release())
		}
		candidates = append(candidates, candidate)
		s.mu.Lock()
		s.resetContextsManaged = false
		s.resetContexts = append([]serveRuntimeBundleContext(nil), candidates...)
		s.mu.Unlock()
		grant, err := s.processCapability.IssueGenerationGrant(ctx, startupownership.GrantRequest{
			BundleHash: candidate.sourceArtifactFact.BundleHash(), RuntimeInstanceID: request.RuntimeInstanceID,
			RuntimeGeneration: s.resetGeneration, SourceSetRevision: plan.Revision,
		})
		if err != nil {
			return err
		}
		if err := candidate.runtime.InstallStartupGrant(grant); err != nil {
			return err
		}
	}
	reconciliations, err := reconcileServeStandingServices(ctx, candidates[0].runtime.Pipeline, candidates)
	if err != nil {
		return err
	}
	for i := range candidates {
		targets, activations, err := reconcileServeRuntimeStandingTargets(candidates[i].runtime, reconciliations)
		if err != nil {
			return err
		}
		candidates[i].startupStandingTargets, candidates[i].startupStandingActivations = targets, activations
	}
	definitions, err := plannedServeRuntimeContexts(candidates)
	if err != nil {
		return err
	}
	if err := s.runtimeContexts.StageResetRuntimeContexts(definitions...); err != nil {
		return err
	}
	s.resetContextsManaged = true
	staged = true
	execution, err := s.resetBuildExecution(candidates[0])
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.currentRT, s.execution = candidates[0].runtime, execution
	s.resetContexts = candidates
	s.mu.Unlock()
	// Every retained execution consumer now points at staged, unselectable
	// occurrences. Startup can prepare standing targets, then publish the set.
	if err := ctx.Err(); err != nil {
		return err
	}
	release, err := prepareResetServeRuntimeContexts(s.resetRequests[0].Ctx, candidates, s.runtimeContexts)
	if err != nil {
		return err
	}
	if s.resetRefresh != nil {
		if err := s.resetRefresh(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.runtimeContexts.ReleaseResetExecution(definitions...); err != nil {
		return err
	}
	if err := release(); err != nil {
		return err
	}
	s.mu.Lock()
	s.resetting = false
	if s.ready != nil {
		s.ready.Store(true)
	}
	s.mu.Unlock()
	return nil
}

func (s *processLifecycleSupervisor) materializeResetProjection(ctx context.Context, artifact *sourceartifact.AdmittedSourceArtifact, managed bool) (*sourceartifact.RuntimeProjection, error) {
	intent, err := sourceartifact.PlanRuntimeProjection(artifact)
	if err != nil {
		return nil, err
	}
	if err := destructivereset.RecordProjectionAllocation(ctx, s.processCapability, s.resetOperationID,
		destructivereset.SourceProjection{Cleanup: intent, ManagedContainers: managed}); err != nil {
		return nil, err
	}
	return sourceartifact.MaterializePlannedRuntimeProjection(artifact, intent)
}

func (s *processLifecycleSupervisor) ManagedResetContainerInventory(ctx context.Context) ([]destructivereset.ContainerRef, error) {
	if s == nil {
		return nil, errors.New("reset inventory requires process lifecycle supervisor")
	}
	s.mu.RLock()
	contexts := append([]serveRuntimeBundleContext(nil), s.resetContexts...)
	s.mu.RUnlock()
	if s.resetStartup {
		managed := false
		for _, source := range s.resetRecoveredProjections {
			managed = managed || source.ManagedContainers
		}
		if !managed {
			return nil, nil
		}
		if s.resetContainerRuntime == nil {
			return nil, errors.New("pending reset requires its container inventory owner")
		}
		inventory, err := s.resetContainerRuntime.ManagedResetContainerInventory(ctx)
		if err != nil {
			return nil, err
		}
		var owned []destructivereset.ContainerRef
		for _, candidate := range inventory {
			for _, projection := range s.resetRecoveredProjections {
				if projection.ManagedContainers && candidate.BundleHash == projection.Cleanup.BundleHash && candidate.SourceProjection == projection.Cleanup.Identity {
					owned = append(owned, candidate)
					break
				}
			}
		}
		return owned, nil
	}
	seen := map[destructivereset.ContainerRef]bool{}
	var result []destructivereset.ContainerRef
	for _, current := range contexts {
		if current.workspaces == nil {
			continue
		}
		inventory, err := current.workspaces.ManagedResetContainerInventory(ctx)
		if err != nil {
			return nil, err
		}
		for _, container := range inventory {
			projection := current.loaded.sourceProjection
			if projection == nil || projection.Identity() == "" ||
				container.BundleHash != current.sourceArtifactFact.BundleHash() || container.SourceProjection != projection.Identity() {
				continue
			}
			if !seen[container] {
				result = append(result, container)
				seen[container] = true
			}
		}
	}
	return result, nil
}

func (s *processLifecycleSupervisor) InspectManagedContainer(ctx context.Context, name string) (destructivereset.ManagedContainerInspection, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.resetStartup && s.resetContainerRuntime != nil {
		return s.resetContainerRuntime.InspectManagedContainer(ctx, name)
	}
	inspected := false
	for _, current := range s.resetContexts {
		if current.workspaces == nil {
			continue
		}
		inspection, err := current.workspaces.InspectManagedContainer(ctx, name)
		inspected = true
		if err != nil || inspection.Exists {
			return inspection, err
		}
	}
	if !inspected {
		return destructivereset.ManagedContainerInspection{}, fmt.Errorf("reset container %s has no inspection owner", name)
	}
	return destructivereset.ManagedContainerInspection{}, nil
}

func (s *processLifecycleSupervisor) StopManagedContainer(ctx context.Context, planned destructivereset.ContainerRef) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.resetStartup && s.resetContainerRuntime != nil {
		for _, projection := range s.resetRecoveredProjections {
			if projection.ManagedContainers && projection.Cleanup.BundleHash == planned.BundleHash && projection.Cleanup.Identity == planned.SourceProjection {
				return s.resetContainerRuntime.StopManagedContainer(ctx, planned)
			}
		}
		return fmt.Errorf("reset container %s is outside the admitted predecessor projections", planned.RuntimeID)
	}
	for _, current := range s.resetContexts {
		if current.workspaces != nil && current.loaded.sourceProjection != nil &&
			current.sourceArtifactFact.BundleHash() == planned.BundleHash && current.loaded.sourceProjection.Identity() == planned.SourceProjection {
			return current.workspaces.StopManagedContainer(ctx, planned)
		}
	}
	return fmt.Errorf("reset container %s has no exact owned source projection", planned.Name)
}
