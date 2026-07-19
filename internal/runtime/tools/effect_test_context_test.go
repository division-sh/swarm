package tools

import (
	"context"
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
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
	return runtimebus.WithInboundEvent(unmanagedToolTestContext(), toolTestInboundEvent("tool.execution.requested", nil, envelope, mode))
}

func toolTestInboundEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, mode executionmode.Mode) events.Event {
	return eventtest.RunCreatingRootIngressWithMode(
		"11111111-1111-4111-8111-111111111111",
		eventType,
		"test-gateway",
		"",
		payload,
		0,
		"22222222-2222-4222-8222-222222222222",
		"",
		envelope,
		time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
		mode,
	)
}
