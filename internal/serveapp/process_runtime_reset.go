package serveapp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type serveRuntimeReset struct {
	supervisor *processLifecycleSupervisor
	release    sync.Once
	completed  bool
}

func (s *processLifecycleSupervisor) BeginDestructiveReset(ctx context.Context) (destructivereset.RuntimeReset, error) {
	if s == nil {
		return nil, errors.New("reset requires process lifecycle supervisor")
	}
	s.operationMu.Lock()
	if err := ctx.Err(); err != nil {
		s.operationMu.Unlock()
		return nil, err
	}
	if s.resetBuildExecution == nil || len(s.resetRequests) == 0 {
		s.operationMu.Unlock()
		return nil, errors.New("reset requires admitted-source reconstruction inputs")
	}
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
	return &serveRuntimeReset{supervisor: s}, nil
}

func (r *serveRuntimeReset) Release() {
	r.release.Do(r.supervisor.operationMu.Unlock)
}

func (r *serveRuntimeReset) Complete(ctx context.Context, retainSources bool) error {
	if r.completed {
		return nil
	}
	s := r.supervisor
	if err := s.releaseResetProjections(ctx); err != nil {
		return err
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
	r.completed = true
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
		projection, err := sourceartifact.MaterializeRuntimeProjection(request.Loaded.bundle.SourceArtifact)
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
	if err := startResetServeRuntimeContexts(s.resetRequests[0].Ctx, candidates, s.runtimeContexts); err != nil {
		return err
	}
	if s.resetRefresh != nil {
		if err := s.resetRefresh(ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.resetting = false
	if s.ready != nil {
		s.ready.Store(true)
	}
	s.mu.Unlock()
	return nil
}

func (s *processLifecycleSupervisor) ManagedResetContainerInventory(ctx context.Context) ([]destructivereset.ContainerRef, error) {
	if s == nil {
		return nil, errors.New("reset inventory requires process lifecycle supervisor")
	}
	s.mu.RLock()
	contexts := append([]serveRuntimeBundleContext(nil), s.resetContexts...)
	s.mu.RUnlock()
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
	for _, current := range s.resetContexts {
		if current.workspaces == nil {
			continue
		}
		inspection, err := current.workspaces.InspectManagedContainer(ctx, name)
		if err != nil || inspection.Exists {
			return inspection, err
		}
	}
	return destructivereset.ManagedContainerInspection{}, nil
}

func (s *processLifecycleSupervisor) StopManagedContainer(ctx context.Context, planned destructivereset.ContainerRef) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, current := range s.resetContexts {
		if current.workspaces != nil && current.loaded.sourceProjection != nil &&
			current.sourceArtifactFact.BundleHash() == planned.BundleHash && current.loaded.sourceProjection.Identity() == planned.SourceProjection {
			return current.workspaces.StopManagedContainer(ctx, planned)
		}
	}
	return fmt.Errorf("reset container %s has no exact owned source projection", planned.Name)
}
