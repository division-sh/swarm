package serveapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

// processLifecycleSupervisor owns only serve-process runtime attachment and
// shutdown. Bundle replacement is not a supported process operation.
type processLifecycleSupervisor struct {
	stores            serveRuntimePersistence
	ready             serveReadiness
	processCapability runtimestartupownership.ProcessCapability
	runtimeLifetime   context.Context
	shutdownOptions   runtime.ShutdownOptions
	cloneRuntime      func(context.Context, *runtime.Runtime) (*runtime.Runtime, *worklifetime.RuntimeOccurrence, error)
	startRuntime      func(context.Context, *runtime.Runtime) error
	shutdownRuntime   func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	operationMu       sync.Mutex

	mu                      sync.RWMutex
	currentRT               *runtime.Runtime
	currentBundleSourceFact runtimecorrelation.BundleSourceFact
	runtimeContexts         *runtime.RuntimeContextManager
}

func newProcessLifecycleSupervisor(stores serveRuntimePersistence, ready serveReadiness, initialRT *runtime.Runtime) *processLifecycleSupervisor {
	s := &processLifecycleSupervisor{
		stores: stores, ready: ready, currentRT: initialRT,
		shutdownOptions: runtime.DefaultShutdownOptions(),
		startRuntime:    func(ctx context.Context, rt *runtime.Runtime) error { return rt.Start(ctx) },
		shutdownRuntime: func(_ context.Context, rt *runtime.Runtime, opts runtime.ShutdownOptions) error {
			return rt.ShutdownWithOptions(opts)
		},
	}
	s.cloneRuntime = func(ctx context.Context, predecessor *runtime.Runtime) (*runtime.Runtime, *worklifetime.RuntimeOccurrence, error) {
		if predecessor == nil {
			return nil, nil, errors.New("predecessor runtime is required")
		}
		deps := stores.runtimeDeps()
		deps.Config = predecessor.Config
		deps.Options = predecessor.Options
		restored, err := runtime.NewRuntime(ctx, deps)
		if err != nil {
			return nil, nil, err
		}
		return restored, restored.WorkOccurrence(), nil
	}
	return s
}

func (s *processLifecycleSupervisor) SetProcessCapability(capability runtimestartupownership.ProcessCapability) {
	if s == nil {
		return
	}
	s.operationMu.Lock()
	s.processCapability = capability
	s.operationMu.Unlock()
}

func (s *processLifecycleSupervisor) SetRuntimeContextManager(manager *runtime.RuntimeContextManager, fact runtimecorrelation.BundleSourceFact) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.runtimeContexts = manager
	s.currentBundleSourceFact = fact
	s.mu.Unlock()
}

func (s *processLifecycleSupervisor) PublicIngressState() (*runtime.Runtime, *runtime.RuntimeContextManager) {
	if s == nil {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentRT, s.runtimeContexts
}

func (s *processLifecycleSupervisor) CurrentRuntime() *runtime.Runtime {
	current, _ := s.PublicIngressState()
	return current
}

func (s *processLifecycleSupervisor) acquireCurrentRuntime(ctx context.Context) (*runtime.RuntimeContextUse, error) {
	if s == nil {
		return nil, errors.New("runtime process lifecycle owner is unavailable")
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	bundleHash := s.currentBundleSourceFact.BundleHash()
	s.mu.RUnlock()
	if manager == nil || bundleHash == "" {
		return nil, errors.New("runtime context manager unavailable")
	}
	use, lookup, err := manager.AcquireBundleHash(ctx, bundleHash)
	if err != nil {
		return nil, err
	}
	if use == nil || !lookup.Loaded() {
		return nil, fmt.Errorf("current runtime context unavailable: %s", lookup.Cause)
	}
	return use, nil
}

func (s *processLifecycleSupervisor) ShutdownProcessWithOptions(ctx context.Context, opts runtime.ShutdownOptions) error {
	if s == nil {
		return nil
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	settlementErr := s.settlePendingSourceSetTransitionLocked(ctx)
	if settlementErr != nil {
		ownershipTerminal := false
		if s.processCapability != nil {
			_, ownershipTerminal = s.processCapability.TerminalResult()
		}
		if !ownershipTerminal {
			return settlementErr
		}
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	bundleHash := s.currentBundleSourceFact.BundleHash()
	current := s.currentRT
	s.mu.RUnlock()
	var shutdownErr error
	if manager != nil && bundleHash != "" {
		shutdownErr = manager.DeactivateBundleHashWithOptions(bundleHash, runtime.RuntimeContextCauseUnavailable, opts).ShutdownErr
	} else if current != nil {
		shutdownErr = s.stopRuntime(ctx, current, opts)
	}
	s.mu.Lock()
	s.currentRT = nil
	if s.ready != nil {
		s.ready.Store(false)
	}
	s.mu.Unlock()
	return errors.Join(settlementErr, shutdownErr)
}

func (s *processLifecycleSupervisor) settlePendingSourceSetTransition(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.settlePendingSourceSetTransitionLocked(ctx)
}

func (s *processLifecycleSupervisor) settlePendingSourceSetTransitionLocked(ctx context.Context) error {
	if s.processCapability == nil {
		return nil
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	s.mu.RUnlock()
	if manager == nil {
		return nil
	}
	plan, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("process shutdown cannot settle topology without an installed source set")
	}
	transition, err := manager.PreparePendingSourceSetTransition(ctx, plan)
	if err != nil {
		return err
	}
	if transition == nil {
		return nil
	}
	if err := transition.Commit(ctx, s.processCapability); err != nil {
		return err
	}
	return s.attachPrimaryRuntime(ctx, manager)
}

func (s *processLifecycleSupervisor) attachPrimaryRuntime(ctx context.Context, manager *runtime.RuntimeContextManager) error {
	primary, ok := manager.Primary()
	if !ok || primary == nil {
		return errors.New("completed topology has no loaded primary runtime context")
	}
	use, lookup, err := manager.AcquireBundleHash(ctx, primary.BundleHash())
	if err != nil {
		return err
	}
	if use == nil || !lookup.Loaded() || use.Runtime() == nil {
		return errors.New("completed topology primary runtime context is not executable")
	}
	rt := use.Runtime()
	fact := use.Context.BundleSourceFact
	if err := use.Done(); err != nil {
		return err
	}
	s.mu.Lock()
	s.currentRT = rt
	s.currentBundleSourceFact = fact
	if s.ready != nil {
		s.ready.Store(true)
	}
	s.mu.Unlock()
	return nil
}

func (s *processLifecycleSupervisor) compensateBundleDeletePredecessor(ctx context.Context, manager *runtime.RuntimeContextManager, predecessor runtime.BundleContext, generation uint64) error {
	if s == nil || manager == nil || predecessor.Runtime == nil {
		return errors.New("bundle delete predecessor restoration requires exact runtime ownership")
	}
	if s.processCapability == nil {
		return errors.New("bundle delete predecessor restoration requires the process topology capability")
	}
	clone, workOwner, err := s.cloneRuntime(ctx, predecessor.Runtime)
	if err != nil {
		return fmt.Errorf("construct bundle delete predecessor runtime: %w", err)
	}
	if workOwner == nil {
		return errors.New("bundle delete predecessor restoration requires a runtime occurrence")
	}
	owned := true
	defer func() {
		if owned {
			_ = s.stopRuntime(context.Background(), clone, s.shutdownOptions)
		}
	}()
	plan, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("bundle delete predecessor source set is unavailable")
	}
	if err := installRestoredRuntimeGrant(ctx, s.processCapability, clone, plan, generation+1); err != nil {
		return fmt.Errorf("install bundle delete predecessor generation grant: %w", err)
	}
	if err := s.startRuntime(s.runtimeStartContext(ctx), clone); err != nil {
		return fmt.Errorf("start bundle delete predecessor runtime: %w", err)
	}
	targets, _, err := clone.EnsureStandingTargets(ctx)
	if err != nil {
		return fmt.Errorf("restore bundle delete predecessor standing targets: %w", err)
	}
	restored := predecessor
	restored.Runtime = clone
	restored.WorkOwner = workOwner
	restored.StandingTargets = targets
	publication, err := manager.PrepareBundleRuntimeRestoration(restored)
	if err != nil {
		return err
	}
	if err := publication.Publish(); err != nil {
		_ = publication.Discard()
		return err
	}
	owned = false
	s.mu.Lock()
	if s.currentRT == predecessor.Runtime {
		s.currentRT = clone
		if s.ready != nil {
			s.ready.Store(true)
		}
	}
	s.mu.Unlock()
	return nil
}

func installRestoredRuntimeGrant(ctx context.Context, capability runtimestartupownership.ProcessCapability, rt *runtime.Runtime, plan runtimeagenttopology.SourceSetPlan, generation uint64) error {
	if capability == nil || rt == nil || generation == 0 {
		return errors.New("restored runtime generation authority is required")
	}
	bundleHash, bundleSource := rt.Options.BundleSourceFact.StorageValues()
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: bundleHash, BundleSource: bundleSource,
		RuntimeInstanceID: strings.TrimSpace(rt.Options.RuntimeInstanceID), RuntimeGeneration: generation,
		SourceSetRevision: plan.Revision,
	})
	if err != nil {
		return err
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		_ = grant.Retire(context.Background())
		return err
	}
	return nil
}

func (s *processLifecycleSupervisor) stopRuntime(ctx context.Context, rt *runtime.Runtime, opts runtime.ShutdownOptions) error {
	if s == nil || rt == nil {
		return nil
	}
	if s.shutdownRuntime != nil {
		return s.shutdownRuntime(ctx, rt, opts)
	}
	return rt.ShutdownWithOptions(opts)
}

func (s *processLifecycleSupervisor) runtimeStartContext(fallback context.Context) context.Context {
	if s != nil && s.runtimeLifetime != nil {
		return s.runtimeLifetime
	}
	if fallback != nil {
		return fallback
	}
	return context.Background()
}
