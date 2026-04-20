package tools

import (
	"swarm/internal/events"
	runtimeeventpayload "swarm/internal/runtime/eventpayload"
	models "swarm/internal/runtime/core/actors"
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
		return NewRuntimeError(
			"invalid_tool_input",
			"tool-executor",
			"handle_emit_tool.envelope_field",
			false,
			"%s is platform-owned event envelope context and must not be authored in emit payload",
			field,
		)
	}
	return nil
}
