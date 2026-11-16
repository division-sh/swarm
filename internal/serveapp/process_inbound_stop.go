package serveapp

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (s *processLifecycleSupervisor) prepareInboundStop() context.Context {
	s.mu.Lock()
	if s.inboundStopCtx == nil {
		s.inboundStopCtx, s.inboundStopCancel = context.WithTimeout(context.Background(), s.shutdownOptions.Grace)
	}
	s.stopping, s.apiReady = true, false
	manager, ctx := s.runtimeContexts, s.inboundStopCtx
	s.mu.Unlock()
	if manager != nil {
		manager.PrepareInboundStop()
	}
	return ctx
}

type serveInboundStop struct {
	supervisor *processLifecycleSupervisor
	contexts   *runtime.InboundRuntimeStop
}

func (s *processLifecycleSupervisor) beginInboundStop() *serveInboundStop {
	ctx := s.prepareInboundStop()
	s.shutdownMu.Lock()
	s.operationMu.Lock()
	s.mu.RLock()
	selected, manager, current := s.selected, s.runtimeContexts, s.currentRT
	s.mu.RUnlock()
	var preparationErr error
	if selected != nil {
		preparationErr = selected.SealSelectedContexts()
	}
	manager.CloseFreshDispatch()
	current.CloseFreshDispatch()
	s.operationMu.Unlock()
	if selected != nil {
		preparationErr = errors.Join(preparationErr, selected.DrainSelectedContexts(ctx))
		preparationErr = errors.Join(preparationErr, selected.RetireSelectedContexts(context.WithoutCancel(ctx)))
	}
	s.operationMu.Lock()
	if s.processCapability != nil {
		if result, terminal := s.processCapability.TerminalResult(); terminal && result.Cause != runtimestartupownership.TerminalReleased {
			preparationErr = errors.Join(preparationErr, &runtimestartupownership.PossessionError{Cause: result.Cause})
		}
	}
	if len(s.resetRequests) > 0 && s.stopRunStalled != nil {
		s.stopRunStalled()
		s.stopRunStalled = nil
	}
	var additional []*runtime.Runtime
	for _, candidate := range s.resetContexts {
		additional = append(additional, candidate.runtime)
	}
	held := manager.PrepareInboundRuntimeStop(ctx, s.shutdownOptions.Grace, additional...)
	_ = held.Open(preparationErr) // The retained result is returned once by Finish.
	return &serveInboundStop{supervisor: s, contexts: held}
}

func (s *serveInboundStop) Finish() error {
	defer s.supervisor.shutdownMu.Unlock()
	defer s.supervisor.operationMu.Unlock()
	result := s.contexts.Finish()
	if result == nil && len(s.supervisor.resetRequests) != 0 {
		result = s.supervisor.releaseResetProjections(context.WithoutCancel(s.supervisor.inboundStopCtx))
	}
	s.supervisor.mu.Lock()
	s.supervisor.currentRT = nil
	s.supervisor.mu.Unlock()
	s.supervisor.inboundStopCancel()
	return result
}
