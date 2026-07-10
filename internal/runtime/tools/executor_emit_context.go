package tools

import (
	"github.com/division-sh/swarm/internal/events"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeventpayload "github.com/division-sh/swarm/internal/runtime/eventpayload"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func (e *Executor) enrichEmitPayloadContext(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		out[k] = v
	}
	return out
}

func rejectEmitEnvelopeFields(payload map[string]any) error {
	for _, field := range runtimeeventpayload.RuntimeOwnedCanonicalContextFields(payload) {
		return failures.NewDetail(
			"invalid_tool_input",
			"tool-executor",
			"handle_emit_tool.envelope_field",
			map[string]any{"field": field},
		)
	}
	return nil
}
