package tools

import (
	"context"
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func unmanagedToolTestContext() context.Context {
	return runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerBuildTestInfrastructure)
}

func toolEventTestContext(actor models.AgentConfig) context.Context {
	mode := executionmode.Mode(actor.ExecutionMode)
	if !mode.Valid() {
		mode = executionmode.Live
	}
	envelope := events.EventEnvelope{
		EntityID:     actor.EffectiveEntityID(),
		FlowInstance: actor.CanonicalFlowPath(),
	}
	ctx := correlation.WithRunID(unmanagedToolTestContext(), actor.Identity.RunID)
	return runtimebus.WithInboundEvent(ctx, toolTestInboundEvent("tool.execution.requested", nil, envelope, mode))
}

func toolTestInboundEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, mode executionmode.Mode) events.Event {
	return toolTestInboundEventWithSource(eventType, payload, envelope, mode, eventtest.RootRoutingSource(toolTestRunID))
}

func toolTestInboundEventWithSource(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, mode executionmode.Mode, source events.RoutingSource) events.Event {
	return eventtest.RunCreatingRootIngressWithRoutingSourceAndMode(
		"11111111-1111-4111-8111-111111111111",
		eventType,
		"test-gateway",
		"",
		payload,
		0,
		toolTestRunID,
		"",
		envelope,
		source,
		time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
		mode,
	)
}
