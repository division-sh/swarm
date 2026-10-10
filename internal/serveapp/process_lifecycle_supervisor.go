package serveapp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

// processLifecycleSupervisor owns only serve-process runtime attachment and
// shutdown. Bundle replacement is not a supported process operation.
type processLifecycleSupervisor struct {
	ready             serveReadiness
	processCapability runtimestartupownership.ProcessCapability
	shutdownOptions   runtime.ShutdownOptions
	shutdownRuntime   func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	operationMu       sync.Mutex

	mu                          sync.RWMutex
	currentRT                   *runtime.Runtime
	currentSourceArtifactFact   runtimecorrelation.SourceArtifactFact
	runtimeContexts             *runtime.RuntimeContextManager
	execution                   map[string]apiv1.MethodHandler
	resetting                   bool
	apiReady                    bool
	resetContexts               []serveRuntimeBundleContext
	resetContextsManaged        bool
	resetRequests               []serveRuntimeBundleContextRequest
	resetBuildExecution         func(serveRuntimeBundleContext) (map[string]apiv1.MethodHandler, error)
	resetRefresh                func(context.Context) error
	resetChannelPublication     func() error
	resetGeneration             uint64
	resetOperationID            string
	resetConverged              bool
	resetStartup                bool
	resetRecoveredProjections   []destructivereset.SourceProjection
	selected                    *storeselected.RunFork
	selectedResetPredecessor    *storeselected.RunFork
	selectedProcess             *worklifetime.Process
	selectedRecoveryEnvironment runtimerunforkexecution.SelectedForkRecoveryEnvironment
	resetContainerRuntime       interface {
		destructivereset.ManagedContainerRuntime
		destructivereset.ManagedContainerInventoryReader
		DisposeProjectionContainers(context.Context, sourceartifact.RuntimeProjectionCleanup) error
	}
	stopRunStalled func()
}

func newProcessLifecycleSupervisor(ready serveReadiness, initialRT *runtime.Runtime) *processLifecycleSupervisor {
	s := &processLifecycleSupervisor{
		ready: ready, currentRT: initialRT,
		shutdownOptions: runtime.DefaultShutdownOptions(),
		shutdownRuntime: func(_ context.Context, rt *runtime.Runtime, opts runtime.ShutdownOptions) error {
			return rt.ShutdownWithOptions(opts)
		},
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

func (s *processLifecycleSupervisor) SetRuntimeContextManager(manager *runtime.RuntimeContextManager, fact runtimecorrelation.SourceArtifactFact) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.runtimeContexts = manager
	s.currentSourceArtifactFact = fact
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
	defer s.mu.RUnlock()
	if s.resetting {
		return nil, errors.New("runtime reset has not converged")
	}
	manager := s.runtimeContexts
	bundleHash := s.currentSourceArtifactFact.BundleHash()
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
	selectedErr := s.retireSelectedContextsLocked(ctx)
	s.mu.RLock()
	manager := s.runtimeContexts
	current := s.currentRT
	s.mu.RUnlock()
	shutdownErr := selectedErr
	var terminalErr error
	if s.processCapability != nil {
		if result, terminal := s.processCapability.TerminalResult(); terminal && result.Cause != runtimestartupownership.TerminalReleased {
			terminalErr = &runtimestartupownership.PossessionError{Cause: result.Cause}
		}
	}
	if len(s.resetRequests) > 0 {
		s.mu.Lock()
		s.resetting = true
		s.mu.Unlock()
		if s.stopRunStalled != nil {
			s.stopRunStalled()
			s.stopRunStalled = nil
		}
		for _, result := range manager.DeactivateAllWithOptions(runtime.RuntimeContextCauseUnavailable, opts) {
			shutdownErr = errors.Join(shutdownErr, result.ShutdownErr)
		}
		// Construction can fail before a candidate is registered. The
		// supervisor still owns it and must join it before projection release.
		if !s.resetContextsManaged {
			for _, current := range s.resetContexts {
				shutdownErr = errors.Join(shutdownErr, s.stopRuntime(ctx, current.runtime, opts))
			}
		}
		if shutdownErr == nil {
			shutdownErr = s.releaseResetProjections(ctx)
		}
	} else if manager != nil {
		for _, result := range manager.DeactivateAllWithOptions(runtime.RuntimeContextCauseUnavailable, opts) {
			shutdownErr = errors.Join(shutdownErr, result.ShutdownErr)
		}
	} else if current != nil {
		shutdownErr = errors.Join(shutdownErr, s.stopRuntime(ctx, current, opts))
	}
	s.mu.Lock()
	s.currentRT = nil
	s.apiReady = false
	if s.ready != nil {
		s.ready.Store(false)
	}
	s.mu.Unlock()
	return errors.Join(terminalErr, shutdownErr)
}

// Final store release consults the supervisor, not a captured family from boot.
// A failed candidate remains selected here until its join succeeds.
func (s *processLifecycleSupervisor) RetireSelectedContexts(ctx context.Context) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.retireSelectedContextsLocked(ctx)
}

func (s *processLifecycleSupervisor) retireSelectedContextsLocked(ctx context.Context) error {
	if s.selected == nil {
		return nil
	}
	return s.selected.RetireSelectedContexts(ctx)
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
