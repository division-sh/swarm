package serveapp

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type serveStopClock interface {
	StartGrace(time.Duration) (context.Context, context.CancelFunc)
}

type realServeStopClock struct{}

func (realServeStopClock) StartGrace(grace time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), grace)
}

func (s *processLifecycleSupervisor) prepareInboundStop() context.Context {
	s.mu.Lock()
	if s.inboundStopCtx == nil {
		clock := s.stopClock
		if clock == nil {
			clock = realServeStopClock{}
		}
		s.inboundStopCtx, s.inboundStopCancel = clock.StartGrace(s.shutdownOptions.Grace)
	}
	s.stopping, s.apiReady = true, false
	manager, current, ctx := s.runtimeContexts, s.currentRT, s.inboundStopCtx
	s.mu.Unlock()
	if manager != nil {
		manager.PrepareInboundStop()
		manager.CloseFreshDispatch()
	}
	current.CloseFreshDispatch()
	return ctx
}

func (s *processLifecycleSupervisor) beginWebhookClaim(ctx context.Context) (context.Context, func(), bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.stopping || s.runtimeContexts == nil {
		return ctx, nil, false
	}
	return s.runtimeContexts.BeginPendingChannelInputSelection(ctx)
}

type serveInboundStop struct {
	supervisor  *processLifecycleSupervisor
	contexts    *runtime.InboundRuntimeStop
	terminalErr error
}

func (s *processLifecycleSupervisor) beginInboundStop(exposure *publicingress.Controller) *serveInboundStop {
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
	// Admitted registration writers still need ordinary runtime acquisition.
	exposure.SealUpdates(ctx)
	s.operationMu.Lock()
	var terminalErr error
	if s.processCapability != nil {
		if result, terminal := s.processCapability.TerminalResult(); terminal && result.Cause != runtimestartupownership.TerminalReleased {
			terminalErr = &runtimestartupownership.PossessionError{Cause: result.Cause}
		}
	}
	if len(s.resetRequests) > 0 && s.stopRunStalled != nil {
		s.stopRunStalled()
		s.stopRunStalled = nil
	}
	s.mu.RLock()
	additional := []*runtime.Runtime{s.currentRT}
	s.mu.RUnlock()
	for _, candidate := range s.resetContexts {
		additional = append(additional, candidate.runtime)
	}
	held := manager.PrepareInboundRuntimeStop(ctx, s.shutdownOptions.Grace, additional...)
	_ = held.Open(preparationErr, terminalErr) // Finish reports joins separately from the intake veto.
	return &serveInboundStop{supervisor: s, contexts: held, terminalErr: terminalErr}
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
	return errors.Join(s.terminalErr, result)
}
