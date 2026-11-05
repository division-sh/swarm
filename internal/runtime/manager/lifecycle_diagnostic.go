package manager

import (
	"context"
	"errors"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type LifecycleDiagnosticOwner = runfork.LifecycleDiagnosticOwner
type LifecycleDiagnosticCausality = runfork.LifecycleDiagnosticCausality
type LifecycleDiagnosticOrigin = runfork.LifecycleDiagnosticOrigin
type LifecycleDiagnosticProvenance = runfork.LifecycleDiagnosticProvenance

const (
	LifecycleDiagnosticNormal        = runfork.LifecycleDiagnosticNormal
	LifecycleDiagnosticSelectedFork  = runfork.LifecycleDiagnosticSelectedFork
	LifecycleDiagnosticObservation   = runfork.LifecycleDiagnosticObservation
	LifecycleDiagnosticAcceptedEvent = runfork.LifecycleDiagnosticAcceptedEvent
)

func (c *agentLifecycleCoordinator) lifecycleTransitionWithDiagnostic(ctx context.Context, req AgentLifecycleTransition) (AgentLifecycleTransition, error) {
	origin := c.diagnosticOrigin
	if event, ok := runtimebus.InboundEventFromContext(ctx); ok {
		origin.Causality, origin.ParentEventID = LifecycleDiagnosticAcceptedEvent, event.ID()
	}
	if err := origin.Validate(); err != nil {
		return AgentLifecycleTransition{}, err
	}
	req.DiagnosticOrigin = origin
	if req.OperationKind == "process_takeover" {
		req.StartupRecovery = runtimeeffects.NewRecoveryRequest(req.Now, c.executionPosture)
	}
	return req, nil
}

func (c *agentLifecycleCoordinator) commitLifecycleTransition(ctx context.Context, store AgentLifecyclePersistence, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	if store == nil {
		return AgentLifecycleTransitionResult{}, errors.New("agent lifecycle requires its durable row owner")
	}
	req, err := c.lifecycleTransitionWithDiagnostic(ctx, req)
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	return store.CommitAgentLifecycleTransition(ctx, req)
}
